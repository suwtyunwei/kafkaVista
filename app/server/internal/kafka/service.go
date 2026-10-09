package kafka

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"

	"kafkavista/server/internal/config"
	"kafkavista/server/internal/model"
)

type Service struct {
	cfg config.Config
}

type ProduceMessage struct {
	Partition *int32
	Key       string
	Value     string
}

type cacheEntry struct {
	createdAt time.Time
	data      map[string]interface{}
}

type logDirCacheEntry struct {
	createdAt time.Time
	data      logDirSizeData
}

type topicMetaCacheEntry struct {
	createdAt time.Time
	items     []map[string]interface{}
	total     int
}

type groupSummaryCacheEntry struct {
	createdAt time.Time
	data      map[string]interface{}
}

type logDirSizeData struct {
	leader  map[string]map[int32]int64
	replica map[string]map[int32]int64
}

var (
	cacheMu           sync.Mutex
	statsTTL          = 10 * time.Second
	logDirTTL         = 30 * time.Second
	topicMetaTTL      = 30 * time.Second
	groupSummaryTTL   = 15 * time.Second
	topicMetaCache    = map[string]topicMetaCacheEntry{}
	groupSummaryCache = map[string]groupSummaryCacheEntry{}
	statsData         = map[string]cacheEntry{}
	logDirData        = map[string]logDirCacheEntry{}
	clientPoolMu      sync.Mutex
	clientPool        = map[string]sarama.Client{}
	refreshMu         sync.Mutex
	topicRefreshing   = map[string]bool{}
	groupRefreshing   = map[string]bool{}
)

// tryMarkRefreshing marks a cache entry as being refreshed in the background.
// Returns false if another goroutine is already refreshing the same entry.
func tryMarkRefreshing(m map[string]bool, key string) bool {
	refreshMu.Lock()
	defer refreshMu.Unlock()
	if m[key] {
		return false
	}
	m[key] = true
	return true
}

func unmarkRefreshing(m map[string]bool, key string) {
	refreshMu.Lock()
	defer refreshMu.Unlock()
	delete(m, key)
}

func New(cfg config.Config) *Service {
	return &Service{cfg: cfg}
}

func (s *Service) saramaConfig(cluster model.KafkaCluster) *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.ClientID = "kafkavista"
	cfg.Version = sarama.V2_1_0_0
	cfg.Admin.Timeout = time.Duration(s.cfg.KafkaRequestTimeoutMS) * time.Millisecond
	cfg.Net.DialTimeout = time.Duration(s.cfg.KafkaRequestTimeoutMS) * time.Millisecond
	cfg.Net.ReadTimeout = time.Duration(s.cfg.KafkaRequestTimeoutMS) * time.Millisecond
	cfg.Net.WriteTimeout = time.Duration(s.cfg.KafkaRequestTimeoutMS) * time.Millisecond
	cfg.Producer.Return.Successes = true
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	if strings.Contains(cluster.SecurityProtocol, "SSL") {
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if cluster.SASLMechanism != "" || strings.Contains(cluster.SecurityProtocol, "SASL") {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.User = cluster.SASLUsername
		cfg.Net.SASL.Password = cluster.SASLPassword
		switch strings.ToUpper(cluster.SASLMechanism) {
		case "SCRAM-SHA-256":
			cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		case "SCRAM-SHA-512":
			cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		default:
			cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		}
	}
	return cfg
}

func brokers(cluster model.KafkaCluster) []string {
	items := strings.Split(cluster.BootstrapServers, ",")
	out := make([]string, 0, len(items))
	for _, item := range items {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func (s *Service) admin(cluster model.KafkaCluster) (sarama.ClusterAdmin, error) {
	return sarama.NewClusterAdmin(brokers(cluster), s.saramaConfig(cluster))
}

func (s *Service) consumer(cluster model.KafkaCluster) (sarama.Consumer, error) {
	return sarama.NewConsumer(brokers(cluster), s.saramaConfig(cluster))
}

// client returns a long-lived shared Kafka client for the cluster, avoiding
// repeated TCP/TLS handshakes on every request. Callers must NOT close it.
// The pool is keyed by cluster ID + UpdatedAt, so config changes get a fresh
// client. Do not use the returned client after the cluster config changes.
func (s *Service) client(cluster model.KafkaCluster) (sarama.Client, error) {
	key := fmt.Sprintf("%s:%s", cluster.ID, cluster.UpdatedAt.Format(time.RFC3339Nano))
	clientPoolMu.Lock()
	if c, ok := clientPool[key]; ok {
		clientPoolMu.Unlock()
		return c, nil
	}
	clientPoolMu.Unlock()

	c, err := sarama.NewClient(brokers(cluster), s.saramaConfig(cluster))
	if err != nil {
		return nil, err
	}
	clientPoolMu.Lock()
	if len(clientPool) >= 8 {
		for _, v := range clientPool {
			v.Close()
		}
		clientPool = map[string]sarama.Client{}
	}
	clientPool[key] = c
	clientPoolMu.Unlock()
	return c, nil
}

func logDirSizes(admin sarama.ClusterAdmin, client sarama.Client) logDirSizeData {
	bs, _, err := admin.DescribeCluster()
	if err != nil {
		return logDirSizeData{leader: map[string]map[int32]int64{}, replica: map[string]map[int32]int64{}}
	}
	brokerIDs := make([]int32, 0, len(bs))
	for _, broker := range bs {
		brokerIDs = append(brokerIDs, broker.ID())
	}
	dirs, err := admin.DescribeLogDirs(brokerIDs)
	if err != nil {
		return logDirSizeData{leader: map[string]map[int32]int64{}, replica: map[string]map[int32]int64{}}
	}
	sizes := logDirSizeData{leader: map[string]map[int32]int64{}, replica: map[string]map[int32]int64{}}
	for brokerID, brokerDirs := range dirs {
		for _, dir := range brokerDirs {
			if dir.ErrorCode != sarama.ErrNoError {
				continue
			}
			for _, topic := range dir.Topics {
				if _, ok := sizes.replica[topic.Topic]; !ok {
					sizes.replica[topic.Topic] = map[int32]int64{}
				}
				if _, ok := sizes.leader[topic.Topic]; !ok {
					sizes.leader[topic.Topic] = map[int32]int64{}
				}
				for _, partition := range topic.Partitions {
					if partition.IsTemporary {
						continue
					}
					sizes.replica[topic.Topic][partition.PartitionID] += partition.Size
					leader, err := client.Leader(topic.Topic, partition.PartitionID)
					if err == nil && leader != nil && leader.ID() == brokerID {
						sizes.leader[topic.Topic][partition.PartitionID] = partition.Size
					}
				}
			}
		}
	}
	return sizes
}

func cachedLogDirSizes(cluster model.KafkaCluster, admin sarama.ClusterAdmin, client sarama.Client) logDirSizeData {
	key := fmt.Sprintf("%s:%s", cluster.ID, cluster.UpdatedAt.Format(time.RFC3339Nano))
	cacheMu.Lock()
	cached, ok := logDirData[key]
	if ok && time.Since(cached.createdAt) < logDirTTL {
		cacheMu.Unlock()
		return cached.data
	}
	cacheMu.Unlock()

	data := logDirSizes(admin, client)
	cacheMu.Lock()
	if len(logDirData) > 16 {
		logDirData = map[string]logDirCacheEntry{}
	}
	logDirData[key] = logDirCacheEntry{createdAt: time.Now(), data: data}
	cacheMu.Unlock()
	return data
}

func invalidateClusterCache(clusterID string) {
	cacheMu.Lock()
	defer cacheMu.Unlock()
	prefix := clusterID + ":"
	for key := range topicMetaCache {
		if strings.HasPrefix(key, prefix) {
			delete(topicMetaCache, key)
		}
	}
	for key := range groupSummaryCache {
		if strings.HasPrefix(key, prefix) {
			delete(groupSummaryCache, key)
		}
	}
	for key := range logDirData {
		if strings.HasPrefix(key, prefix) {
			delete(logDirData, key)
		}
	}
	for key := range statsData {
		if strings.HasPrefix(key, prefix) {
			delete(statsData, key)
		}
	}
}

func (s *Service) Overview(cluster model.KafkaCluster) (map[string]interface{}, error) {
	admin, err := s.admin(cluster)
	if err != nil {
		return nil, err
	}
	defer admin.Close()
	topicsMap, err := admin.ListTopics()
	if err != nil {
		return nil, err
	}
	topics := make([]string, 0, len(topicsMap))
	for topic := range topicsMap {
		if !strings.HasPrefix(topic, "__") {
			topics = append(topics, topic)
		}
	}
	sort.Strings(topics)
	groupsMap, _ := admin.ListConsumerGroups()
	groups := make([]string, 0, len(groupsMap))
	for group := range groupsMap {
		groups = append(groups, group)
	}
	sort.Strings(groups)
	return map[string]interface{}{"topics": topics, "topic_summaries": []interface{}{}, "groups": groups, "topic_count": len(topics), "group_count": len(groups)}, nil
}

func (s *Service) Stats(cluster model.KafkaCluster) (map[string]interface{}, error) {
	cacheKey := fmt.Sprintf("%s:%s", cluster.ID, cluster.UpdatedAt.Format(time.RFC3339Nano))
	cacheMu.Lock()
	if cached, ok := statsData[cacheKey]; ok && time.Since(cached.createdAt) < statsTTL {
		cacheMu.Unlock()
		return cached.data, nil
	}
	cacheMu.Unlock()

	admin, err := s.admin(cluster)
	if err != nil {
		return nil, err
	}
	defer admin.Close()
	topics, err := admin.ListTopics()
	if err != nil {
		return nil, err
	}
	groups, _ := admin.ListConsumerGroups()
	brokers, controllerID, _ := admin.DescribeCluster()
	visibleTopics := 0
	for topic := range topics {
		if !strings.HasPrefix(topic, "__") {
			visibleTopics++
		}
	}
	data := map[string]interface{}{"topic_count": visibleTopics, "group_count": len(groups), "broker_count": len(brokers), "controller_id": controllerID}
	cacheMu.Lock()
	if len(statsData) > 64 {
		statsData = map[string]cacheEntry{}
	}
	statsData[cacheKey] = cacheEntry{createdAt: time.Now(), data: data}
	cacheMu.Unlock()
	return data, nil
}

func (s *Service) Detail(cluster model.KafkaCluster, includeTopics bool) (map[string]interface{}, error) {
	admin, err := s.admin(cluster)
	if err != nil {
		return nil, err
	}
	defer admin.Close()
	bs, controllerID, err := admin.DescribeCluster()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]interface{}, 0, len(bs))
	for _, broker := range bs {
		items = append(items, map[string]interface{}{"node_id": broker.ID(), "host": broker.Addr(), "port": 0, "rack": "", "alive": true, "status": "ALIVE", "is_controller": broker.ID() == controllerID, "connection_failed": false})
	}
	var topics []string
	if includeTopics {
		listed, _ := admin.ListTopics()
		for topic := range listed {
			if !strings.HasPrefix(topic, "__") {
				topics = append(topics, topic)
			}
		}
		sort.Strings(topics)
	}
	return map[string]interface{}{"brokers": items, "broker_count": len(items), "alive_broker_count": len(items), "controller_id": controllerID, "topics": topics, "topic_count": len(topics)}, nil
}

func (s *Service) TopicPage(cluster model.KafkaCluster, page, pageSize int, name, sortBy, sortOrder string) (map[string]interface{}, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 50
	}

	// Metadata cache: stores all topic metadata (including offsets and log sizes)
	// Keyed by cluster ID + cluster.UpdatedAt, shared across all sort/page combos
	metaKey := fmt.Sprintf("%s:%s", cluster.ID, cluster.UpdatedAt.Format(time.RFC3339Nano))
	keyword := strings.ToLower(strings.TrimSpace(name))

	cacheMu.Lock()
	cached, cacheHit := topicMetaCache[metaKey]
	cacheMu.Unlock()

	if !cacheHit {
		// First load: fetch synchronously
		fresh, err := s.fetchTopicMeta(cluster, metaKey)
		if err != nil {
			return nil, err
		}
		cached = fresh
	} else if time.Since(cached.createdAt) >= topicMetaTTL && tryMarkRefreshing(topicRefreshing, metaKey) {
		// Stale-while-revalidate: serve stale data immediately, refresh in background
		go func() {
			defer unmarkRefreshing(topicRefreshing, metaKey)
			_, _ = s.fetchTopicMeta(cluster, metaKey)
		}()
	}
	return filterSortPageTopics(cached, keyword, sortBy, sortOrder, page, pageSize)
}

// fetchTopicMeta loads all topic metadata (offsets + log sizes) from Kafka,
// stores it in the cache and returns the fresh entry.
func (s *Service) fetchTopicMeta(cluster model.KafkaCluster, metaKey string) (topicMetaCacheEntry, error) {
	admin, err := s.admin(cluster)
	if err != nil {
		return topicMetaCacheEntry{}, err
	}
	defer admin.Close()
	client, err := s.client(cluster)
	if err != nil {
		return topicMetaCacheEntry{}, err
	}
	listed, err := admin.ListTopics()
	if err != nil {
		return topicMetaCacheEntry{}, err
	}
	topics := make([]string, 0, len(listed))
	for topic := range listed {
		if strings.HasPrefix(topic, "__") {
			continue
		}
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	logSizes := cachedLogDirSizes(cluster, admin, client)
	total := len(topics)

	type topicResult struct {
		topic          string
		partitionCount int
		messageCount   int64
		logSize        int64
		replicaLogSize int64
	}
	results := make([]topicResult, len(topics))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 20)
	for i, topic := range topics {
		wg.Add(1)
		go func(idx int, t string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			numPart := int(listed[t].NumPartitions)
			var msgCount, ls, rls int64
			for p := int32(0); p < int32(numPart); p++ {
				oldest, _ := client.GetOffset(t, p, sarama.OffsetOldest)
				newest, _ := client.GetOffset(t, p, sarama.OffsetNewest)
				if newest > oldest {
					msgCount += newest - oldest
				}
				ls += logSizes.leader[t][p]
				rls += logSizes.replica[t][p]
			}
			results[idx] = topicResult{topic: t, partitionCount: numPart, messageCount: msgCount, logSize: ls, replicaLogSize: rls}
		}(i, topic)
	}
	wg.Wait()

	items := make([]map[string]interface{}, 0, total)
	for _, r := range results {
		logSizeSource := "log_dir"
		if len(logSizes.leader) == 0 && len(logSizes.replica) == 0 {
			logSizeSource = "unavailable"
		} else if r.logSize == 0 && r.replicaLogSize > 0 {
			r.logSize = r.replicaLogSize
			logSizeSource = "replica_log_dir"
		}
		items = append(items, map[string]interface{}{
			"topic":            r.topic,
			"partition_count":  r.partitionCount,
			"message_count":    r.messageCount,
			"log_size":         r.logSize,
			"replica_log_size": r.replicaLogSize,
			"log_size_source":  logSizeSource,
		})
	}

	entry := topicMetaCacheEntry{createdAt: time.Now(), items: items, total: total}
	cacheMu.Lock()
	if len(topicMetaCache) > 32 {
		topicMetaCache = map[string]topicMetaCacheEntry{}
	}
	topicMetaCache[metaKey] = entry
	cacheMu.Unlock()
	return entry, nil
}

func filterSortPageTopics(cached topicMetaCacheEntry, keyword, sortBy, sortOrder string, page, pageSize int) (map[string]interface{}, error) {
	// Filter by keyword
	filtered := make([]map[string]interface{}, 0, len(cached.items))
	for _, item := range cached.items {
		topicName, _ := item["topic"].(string)
		if keyword == "" || strings.Contains(strings.ToLower(topicName), keyword) {
			filtered = append(filtered, item)
		}
	}
	total := len(filtered)

	// Sort
	if sortBy == "log_size" {
		reverse := sortOrder != "asc"
		sort.SliceStable(filtered, func(i, j int) bool {
			left := filtered[i]["log_size"].(int64)
			right := filtered[j]["log_size"].(int64)
			if left == right {
				return filtered[i]["topic"].(string) < filtered[j]["topic"].(string)
			}
			if reverse {
				return left > right
			}
			return left < right
		})
	}

	// Paginate
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	pageItems := filtered[start:end]

	return map[string]interface{}{"items": pageItems, "total": total, "page": page, "page_size": pageSize}, nil
}

func (s *Service) TopicNames(cluster model.KafkaCluster) ([]string, error) {
	admin, err := s.admin(cluster)
	if err != nil {
		return nil, err
	}
	defer admin.Close()
	listed, err := admin.ListTopics()
	if err != nil {
		return nil, err
	}
	topics := make([]string, 0, len(listed))
	for topic := range listed {
		if strings.HasPrefix(topic, "__") {
			continue
		}
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics, nil
}

func (s *Service) DescribeTopic(cluster model.KafkaCluster, topic string) (map[string]interface{}, error) {
	admin, err := s.admin(cluster)
	if err != nil {
		return nil, err
	}
	defer admin.Close()
	client, err := s.client(cluster)
	if err != nil {
		return nil, err
	}
	logSizes := logDirSizes(admin, client)
	partitions, err := client.Partitions(topic)
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]interface{}, 0, len(partitions))
	for _, p := range partitions {
		oldest, _ := client.GetOffset(topic, p, sarama.OffsetOldest)
		newest, _ := client.GetOffset(topic, p, sarama.OffsetNewest)
		var leaderID interface{}
		if leader, err := client.Leader(topic, p); err == nil && leader != nil {
			leaderID = leader.ID()
		}
		replicas, _ := client.Replicas(topic, p)
		isr, _ := client.InSyncReplicas(topic, p)
		rows = append(rows, map[string]interface{}{"partition": p, "beginning_offset": oldest, "end_offset": newest, "message_count": max64(newest-oldest, 0), "log_dir_size": logSizes.leader[topic][p], "replica_log_dir_size": logSizes.replica[topic][p], "lag_window": max64(newest-oldest, 0), "leader": leaderID, "replicas": replicas, "isr": isr})
	}
	return map[string]interface{}{"name": topic, "partitions": rows}, nil
}
func (s *Service) ReadTopicData(cluster model.KafkaCluster, topic string, partition int32, autoOffsetReset string, offset, endOffset *int64, startMS, endMS *int64, keySearch, valueSearch string, count int, timeoutMS int) (map[string]interface{}, error) {
	if count < 1 || count > s.cfg.KafkaMaxPollRecords {
		count = min(s.cfg.KafkaMaxPollRecords, 200)
	}
	client, err := s.client(cluster)
	if err != nil {
		return nil, err
	}
	oldest, _ := client.GetOffset(topic, partition, sarama.OffsetOldest)
	newest, _ := client.GetOffset(topic, partition, sarama.OffsetNewest)
	consumer, err := s.consumer(cluster)
	if err != nil {
		return nil, err
	}
	defer consumer.Close()
	startOffset := newest
	if offset != nil {
		startOffset = *offset
	} else if autoOffsetReset == "earliest" || startMS != nil {
		startOffset = oldest
	} else if endOffset != nil {
		startOffset = max64(oldest, *endOffset-int64(count)+1)
	} else if newest > oldest {
		startOffset = max64(oldest, newest-int64(count))
	}
	if endOffset != nil && startOffset > *endOffset {
		return readResult([]map[string]interface{}{}, oldest, newest, startOffset, endOffset, startMS, endMS, strings.ToLower(strings.TrimSpace(keySearch)), strings.ToLower(strings.TrimSpace(valueSearch)), count, 0, 0, 0, partition), nil
	}
	if startOffset >= newest {
		return readResult([]map[string]interface{}{}, oldest, newest, startOffset, endOffset, startMS, endMS, strings.ToLower(strings.TrimSpace(keySearch)), strings.ToLower(strings.TrimSpace(valueSearch)), count, 0, 0, 0, partition), nil
	}
	effectiveEndOffset := endOffset
	var cappedEndOffset int64
	if newest > oldest && (effectiveEndOffset == nil || *effectiveEndOffset >= newest) {
		cappedEndOffset = newest - 1
		effectiveEndOffset = &cappedEndOffset
	}
	pc, err := consumer.ConsumePartition(topic, partition, startOffset)
	if err != nil {
		return nil, err
	}
	defer pc.Close()
	deadline := time.After(time.Duration(timeoutMS) * time.Millisecond)
	messages := make([]map[string]interface{}, 0, count)
	keyFilter := strings.ToLower(strings.TrimSpace(keySearch))
	valueFilter := strings.ToLower(strings.TrimSpace(valueSearch))
	hasSearch := keyFilter != "" || valueFilter != ""
	var scanned, totalBytes, truncated int
	if offset == nil && hasSearch && startOffset > oldest {
		pc.Close()
		startOffset = oldest
		pc, err = consumer.ConsumePartition(topic, partition, startOffset)
		if err != nil {
			return nil, err
		}
		defer pc.Close()
	}
	for len(messages) < count {
		select {
		case msg := <-pc.Messages():
			if msg == nil {
				continue
			}
			if effectiveEndOffset != nil && msg.Offset > *effectiveEndOffset {
				return readResult(messages, oldest, newest, startOffset, endOffset, startMS, endMS, keyFilter, valueFilter, count, scanned, totalBytes, truncated, partition), nil
			}
			reachedEndOffset := effectiveEndOffset != nil && msg.Offset >= *effectiveEndOffset
			scanned++
			if endMS != nil && msg.Timestamp.UnixNano()/int64(time.Millisecond) > *endMS {
				if reachedEndOffset {
					return readResult(messages, oldest, newest, startOffset, endOffset, startMS, endMS, keyFilter, valueFilter, count, scanned, totalBytes, truncated, partition), nil
				}
				continue
			}
			if startMS != nil && msg.Timestamp.UnixNano()/int64(time.Millisecond) < *startMS {
				if reachedEndOffset {
					return readResult(messages, oldest, newest, startOffset, endOffset, startMS, endMS, keyFilter, valueFilter, count, scanned, totalBytes, truncated, partition), nil
				}
				continue
			}
			key := string(msg.Key)
			value := string(msg.Value)
			if keyFilter != "" && !strings.Contains(strings.ToLower(key), keyFilter) {
				if reachedEndOffset {
					return readResult(messages, oldest, newest, startOffset, endOffset, startMS, endMS, keyFilter, valueFilter, count, scanned, totalBytes, truncated, partition), nil
				}
				continue
			}
			if valueFilter != "" && !strings.Contains(strings.ToLower(value), valueFilter) {
				if reachedEndOffset {
					return readResult(messages, oldest, newest, startOffset, endOffset, startMS, endMS, keyFilter, valueFilter, count, scanned, totalBytes, truncated, partition), nil
				}
				continue
			}
			value = truncate(value, s.cfg.KafkaMaxValueLength)
			size := len([]byte(value))
			if s.cfg.KafkaMaxResponseBytes > 0 && totalBytes+size > s.cfg.KafkaMaxResponseBytes {
				truncated++
				if reachedEndOffset {
					return readResult(messages, oldest, newest, startOffset, endOffset, startMS, endMS, keyFilter, valueFilter, count, scanned, totalBytes, truncated, partition), nil
				}
				continue
			}
			totalBytes += size
			messages = append(messages, map[string]interface{}{"topic": topic, "partition": partition, "offset": msg.Offset, "timestamp": msg.Timestamp.UnixNano() / int64(time.Millisecond), "timestamp_type": 0, "headers": []interface{}{}, "key": key, "value": value})
			if reachedEndOffset {
				return readResult(messages, oldest, newest, startOffset, endOffset, startMS, endMS, keyFilter, valueFilter, count, scanned, totalBytes, truncated, partition), nil
			}
		case <-deadline:
			return readResult(messages, oldest, newest, startOffset, endOffset, startMS, endMS, keyFilter, valueFilter, count, scanned, totalBytes, truncated, partition), nil
		}
	}
	return readResult(messages, oldest, newest, startOffset, endOffset, startMS, endMS, keyFilter, valueFilter, count, scanned, totalBytes, truncated, partition), nil
}

func (s *Service) ReadTopicDataAll(cluster model.KafkaCluster, topic string, autoOffsetReset string, offset, endOffset *int64, startMS, endMS *int64, keySearch, valueSearch string, count int, timeoutMS int) (map[string]interface{}, error) {
	if count < 1 || count > s.cfg.KafkaMaxPollRecords {
		count = min(s.cfg.KafkaMaxPollRecords, 200)
	}
	client, err := s.client(cluster)
	if err != nil {
		return nil, err
	}
	partitions, err := client.Partitions(topic)
	if err != nil {
		return nil, err
	}
	messages := make([]map[string]interface{}, 0, count)
	searched := make([]int32, 0, len(partitions))
	var scanned, totalBytes, truncated int
	perPartitionCount := count
	if len(partitions) > 0 {
		perPartitionCount = max(1, count/len(partitions)+1)
	}
	perPartitionTimeout := max(500, timeoutMS/max(1, len(partitions)))
	for _, partition := range partitions {
		data, err := s.ReadTopicData(cluster, topic, partition, autoOffsetReset, offset, endOffset, startMS, endMS, keySearch, valueSearch, perPartitionCount, perPartitionTimeout)
		if err != nil {
			return nil, err
		}
		searched = append(searched, partition)
		if items, ok := data["messages"].([]map[string]interface{}); ok {
			messages = append(messages, items...)
		}
		scanned += intFromMap(data, "scanned")
		totalBytes += intFromMap(data, "total_bytes")
		truncated += intFromMap(data, "truncated_count")
	}
	sort.SliceStable(messages, func(i, j int) bool {
		left, _ := messages[i]["timestamp"].(int64)
		right, _ := messages[j]["timestamp"].(int64)
		if left == right {
			return messages[i]["offset"].(int64) > messages[j]["offset"].(int64)
		}
		return left > right
	})
	if len(messages) > count {
		messages = messages[:count]
	}
	return map[string]interface{}{"messages": messages, "beginning_offset": nil, "end_offset": nil, "next_offset": nil, "seek_offset": offset, "end_offset_filter": endOffset, "time_offset": nil, "start_time_ms": startMS, "end_time_ms": endMS, "key_search": strings.ToLower(strings.TrimSpace(keySearch)), "value_search": strings.ToLower(strings.TrimSpace(valueSearch)), "count": count, "scanned": scanned, "total_bytes": totalBytes, "truncated_count": truncated, "searched_partitions": searched, "order_by": "timestamp_desc"}, nil
}

func intFromMap(data map[string]interface{}, key string) int {
	value, ok := data[key]
	if !ok {
		return 0
	}
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

func readResult(messages []map[string]interface{}, oldest, newest, seek int64, endOffset, startMS, endMS *int64, key, value string, count, scanned, totalBytes, truncated int, partition int32) map[string]interface{} {
	sort.SliceStable(messages, func(i, j int) bool { return messages[i]["offset"].(int64) > messages[j]["offset"].(int64) })
	nextOffset := seek
	if len(messages) > 0 {
		nextOffset = messages[0]["offset"].(int64) + 1
	}
	return map[string]interface{}{"messages": messages, "beginning_offset": oldest, "end_offset": newest, "next_offset": nextOffset, "seek_offset": seek, "end_offset_filter": endOffset, "time_offset": nil, "start_time_ms": startMS, "end_time_ms": endMS, "key_search": key, "value_search": value, "count": count, "scanned": scanned, "total_bytes": totalBytes, "truncated_count": truncated, "searched_partitions": []int32{partition}, "order_by": "offset"}
}

func (s *Service) SendMessage(cluster model.KafkaCluster, topic string, partition *int32, key, value string) (map[string]interface{}, error) {
	results, err := s.SendMessages(cluster, topic, []ProduceMessage{{Partition: partition, Key: key, Value: value}})
	if err != nil {
		return nil, err
	}
	items, _ := results["items"].([]map[string]interface{})
	if len(items) == 0 {
		return map[string]interface{}{"topic": topic}, nil
	}
	return items[0], nil
}

func (s *Service) SendMessages(cluster model.KafkaCluster, topic string, messages []ProduceMessage) (map[string]interface{}, error) {
	var defaultProducer sarama.SyncProducer
	var manualProducer sarama.SyncProducer
	defer func() {
		if defaultProducer != nil {
			defaultProducer.Close()
		}
		if manualProducer != nil {
			manualProducer.Close()
		}
	}()
	producerFor := func(manual bool) (sarama.SyncProducer, error) {
		if manual {
			if manualProducer == nil {
				cfg := s.saramaConfig(cluster)
				cfg.Producer.Partitioner = sarama.NewManualPartitioner
				producer, err := sarama.NewSyncProducer(brokers(cluster), cfg)
				if err != nil {
					return nil, err
				}
				manualProducer = producer
			}
			return manualProducer, nil
		}
		if defaultProducer == nil {
			producer, err := sarama.NewSyncProducer(brokers(cluster), s.saramaConfig(cluster))
			if err != nil {
				return nil, err
			}
			defaultProducer = producer
		}
		return defaultProducer, nil
	}
	items := make([]map[string]interface{}, 0, len(messages))
	for i, item := range messages {
		msg := &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(item.Key), Value: sarama.StringEncoder(item.Value)}
		if item.Partition != nil {
			msg.Partition = *item.Partition
		}
		producer, err := producerFor(item.Partition != nil)
		if err != nil {
			return nil, err
		}
		p, offset, err := producer.SendMessage(msg)
		if err != nil {
			return nil, fmt.Errorf("message %d: %w", i+1, err)
		}
		items = append(items, map[string]interface{}{"topic": topic, "partition": p, "offset": offset, "key": item.Key, "size": len(item.Value)})
	}
	return map[string]interface{}{"topic": topic, "count": len(items), "items": items}, nil
}

func (s *Service) CreateTopic(cluster model.KafkaCluster, topic string, partitions, replicationFactor int32) error {
	admin, err := s.admin(cluster)
	if err != nil {
		return err
	}
	defer admin.Close()
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: partitions, ReplicationFactor: int16(replicationFactor)}, false); err != nil {
		if err == sarama.ErrTopicAlreadyExists {
			return fmt.Errorf("Topic 已存在")
		}
		return err
	}
	invalidateClusterCache(cluster.ID)
	return nil
}

func (s *Service) AlterTopicPartitions(cluster model.KafkaCluster, topic string, partitions int32) error {
	if partitions < 1 {
		return fmt.Errorf("分区数必须大于 0")
	}
	admin, err := s.admin(cluster)
	if err != nil {
		return err
	}
	defer admin.Close()
	client, err := s.client(cluster)
	if err != nil {
		return err
	}
	current, err := client.Partitions(topic)
	if err != nil {
		return err
	}
	// Kafka 只支持增加分区数，提前拦截不可回退的缩容或无效修改。
	if partitions <= int32(len(current)) {
		return fmt.Errorf("新分区数必须大于当前分区数 %d", len(current))
	}
	if err := admin.CreatePartitions(topic, partitions, nil, false); err != nil {
		if err == sarama.ErrUnknownTopicOrPartition {
			return fmt.Errorf("Topic 不存在")
		}
		return err
	}
	invalidateClusterCache(cluster.ID)
	return nil
}

func (s *Service) TopicExists(cluster model.KafkaCluster, topic string) (bool, error) {
	admin, err := s.admin(cluster)
	if err != nil {
		return false, err
	}
	defer admin.Close()
	topics, err := admin.ListTopics()
	if err != nil {
		return false, err
	}
	_, ok := topics[topic]
	return ok, nil
}

func (s *Service) DeleteTopic(cluster model.KafkaCluster, topic string) error {
	admin, err := s.admin(cluster)
	if err != nil {
		return err
	}
	defer admin.Close()
	if err := admin.DeleteTopic(topic); err != nil {
		if err == sarama.ErrUnknownTopicOrPartition {
			return fmt.Errorf("Topic 不存在")
		}
		return err
	}
	invalidateClusterCache(cluster.ID)
	return nil
}

func (s *Service) GroupSummaries(cluster model.KafkaCluster, ids []string) (map[string]interface{}, error) {
	// Cache keyed by cluster + sorted group IDs
	sortedIDs := make([]string, len(ids))
	copy(sortedIDs, ids)
	sort.Strings(sortedIDs)
	cacheKey := fmt.Sprintf("%s:%s:%s", cluster.ID, cluster.UpdatedAt.Format(time.RFC3339Nano), strings.Join(sortedIDs, ","))
	cacheMu.Lock()
	cached, cacheHit := groupSummaryCache[cacheKey]
	cacheMu.Unlock()

	if !cacheHit {
		// First load: fetch synchronously
		fresh, err := s.fetchGroupSummaries(cluster, ids, cacheKey)
		if err != nil {
			return nil, err
		}
		return fresh.data, nil
	}
	if time.Since(cached.createdAt) >= groupSummaryTTL && tryMarkRefreshing(groupRefreshing, cacheKey) {
		// Stale-while-revalidate: serve stale data immediately, refresh in background
		go func() {
			defer unmarkRefreshing(groupRefreshing, cacheKey)
			_, _ = s.fetchGroupSummaries(cluster, ids, cacheKey)
		}()
	}
	return cached.data, nil
}

// fetchGroupSummaries loads consumer group offset summaries from Kafka in
// parallel, stores them in the cache and returns the fresh entry.
func (s *Service) fetchGroupSummaries(cluster model.KafkaCluster, ids []string, cacheKey string) (groupSummaryCacheEntry, error) {
	admin, err := s.admin(cluster)
	if err != nil {
		return groupSummaryCacheEntry{}, err
	}
	defer admin.Close()

	type groupResult struct {
		group          string
		topics         []string
		partitionCount int
	}
	results := make([]groupResult, len(ids))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 20)
	for i, group := range ids {
		wg.Add(1)
		go func(idx int, g string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			offsets, _ := admin.ListConsumerGroupOffsets(g, nil)
			topics := map[string]bool{}
			partitionCount := 0
			if offsets != nil {
				for topic, partitions := range offsets.Blocks {
					topics[topic] = true
					partitionCount += len(partitions)
				}
			}
			results[idx] = groupResult{group: g, topics: mapKeys(topics), partitionCount: partitionCount}
		}(i, group)
	}
	wg.Wait()

	items := make([]map[string]interface{}, 0, len(ids))
	for _, r := range results {
		items = append(items, map[string]interface{}{
			"group":           r.group,
			"topics":          r.topics,
			"active_topics":   r.topics,
			"members":         []interface{}{},
			"member_count":    0,
			"partition_count": r.partitionCount,
			"total_lag":       0,
		})
	}
	entry := groupSummaryCacheEntry{createdAt: time.Now(), data: map[string]interface{}{"items": items}}
	cacheMu.Lock()
	if len(groupSummaryCache) > 32 {
		groupSummaryCache = map[string]groupSummaryCacheEntry{}
	}
	groupSummaryCache[cacheKey] = entry
	cacheMu.Unlock()
	return entry, nil
}
func (s *Service) GroupDetail(cluster model.KafkaCluster, group string, topicFilter string) (map[string]interface{}, error) {
	admin, err := s.admin(cluster)
	if err != nil {
		return nil, err
	}
	defer admin.Close()
	client, err := s.client(cluster)
	if err != nil {
		return nil, err
	}
	members, hostByAssignment, hostByTopic := describeGroupMembers(admin, group)
	offsets, err := admin.ListConsumerGroupOffsets(group, nil)
	if err != nil {
		return nil, err
	}
	rows := []map[string]interface{}{}
	topics := map[string]bool{}
	var totalLag, totalCommitted, totalEnd int64
	filter := strings.ToLower(strings.TrimSpace(topicFilter))
	for topic, partitions := range offsets.Blocks {
		if filter != "" && !strings.Contains(strings.ToLower(topic), filter) {
			continue
		}
		topics[topic] = true
		for partition, block := range partitions {
			end, _ := client.GetOffset(topic, partition, sarama.OffsetNewest)
			begin, _ := client.GetOffset(topic, partition, sarama.OffsetOldest)
			committed := block.Offset
			lag := max64(end-committed, 0)
			totalLag += lag
			totalCommitted += committed
			totalEnd += end
			hosts := hostByAssignment[assignmentKey(topic, partition)]
			if len(hosts) == 0 {
				hosts = hostByTopic[topic]
			}
			rows = append(rows, map[string]interface{}{"topic": topic, "partition": partition, "hosts": hosts, "host": strings.Join(hosts, ", "), "committed_offset": committed, "beginning_offset": begin, "end_offset": end, "lag": lag})
		}
	}
	topicList := mapKeys(topics)
	return map[string]interface{}{"group": group, "total_lag": totalLag, "total_committed_offset": totalCommitted, "total_end_offset": totalEnd, "topics": topicList, "active_topics": topicList, "member_count": len(members), "members": members, "partitions": rows}, nil
}

func describeGroupMembers(admin sarama.ClusterAdmin, group string) ([]map[string]interface{}, map[string][]string, map[string][]string) {
	members := []map[string]interface{}{}
	hostByAssignment := map[string][]string{}
	hostByTopic := map[string][]string{}
	descriptions, err := admin.DescribeConsumerGroups([]string{group})
	if err != nil || len(descriptions) == 0 || descriptions[0] == nil {
		return members, hostByAssignment, hostByTopic
	}
	for _, member := range descriptions[0].Members {
		assignments := []map[string]interface{}{}
		assignment, err := member.GetMemberAssignment()
		if err == nil && assignment != nil {
			for topic, partitions := range assignment.Topics {
				addHost(hostByTopic, topic, member.ClientHost)
				for _, partition := range partitions {
					assignments = append(assignments, map[string]interface{}{"topic": topic, "partition": partition})
					addHost(hostByAssignment, assignmentKey(topic, partition), member.ClientHost)
				}
			}
		}
		members = append(members, map[string]interface{}{"member_id": member.MemberId, "client_id": member.ClientId, "client_host": member.ClientHost, "assignments": assignments})
	}
	return members, hostByAssignment, hostByTopic
}

func assignmentKey(topic string, partition int32) string {
	return fmt.Sprintf("%s:%d", topic, partition)
}

func addHost(items map[string][]string, key string, host string) {
	if host == "" {
		return
	}
	for _, item := range items[key] {
		if item == host {
			return
		}
	}
	items[key] = append(items[key], host)
}

func (s *Service) DeleteGroup(cluster model.KafkaCluster, group string) error {
	admin, err := s.admin(cluster)
	if err != nil {
		return err
	}
	defer admin.Close()
	return admin.DeleteConsumerGroup(group)
}

func (s *Service) DeleteGroupTopicOffsets(cluster model.KafkaCluster, group string, topic string) (int, error) {
	admin, err := s.admin(cluster)
	if err != nil {
		return 0, err
	}
	defer admin.Close()
	offsets, err := admin.ListConsumerGroupOffsets(group, nil)
	if err != nil {
		return 0, err
	}
	partitions := offsets.Blocks[topic]
	if len(partitions) == 0 {
		return 0, fmt.Errorf("Consumer Group 与 Topic 没有已提交 offset 关系")
	}
	deleted := 0
	for partition := range partitions {
		if err := admin.DeleteConsumerGroupOffset(group, topic, partition); err != nil {
			if errors.Is(err, sarama.ErrUnsupportedVersion) {
				return deleted, fmt.Errorf("当前 Kafka Broker 版本不支持按 Topic 解除 Consumer Group 订阅关系。该能力依赖 Kafka DeleteOffsets API；请升级 Kafka，或停止消费者后删除整个 Consumer Group")
			}
			if errors.Is(err, sarama.ErrGroupSubscribedToTopic) {
				return deleted, fmt.Errorf("该 Consumer Group 仍有消费者正在订阅此 Topic，请先停止相关消费者实例后再解除订阅关系")
			}
			return deleted, err
		}
		deleted++
	}
	invalidateClusterCache(cluster.ID)
	return deleted, nil
}

func (s *Service) TopicConfigs(cluster model.KafkaCluster, topic string) (map[string]interface{}, error) {
	admin, err := s.admin(cluster)
	if err != nil {
		return nil, err
	}
	defer admin.Close()
	entries, err := admin.DescribeConfig(sarama.ConfigResource{Type: sarama.TopicResource, Name: topic})
	if err != nil {
		return nil, err
	}
	allow := map[string]bool{"cleanup.policy": true, "retention.ms": true, "retention.bytes": true, "segment.bytes": true, "max.message.bytes": true, "min.insync.replicas": true, "compression.type": true, "delete.retention.ms": true, "flush.messages": true, "flush.ms": true}
	configs := make([]map[string]interface{}, 0, len(entries))
	for _, entry := range entries {
		configs = append(configs, map[string]interface{}{"name": entry.Name, "value": entry.Value, "source": fmt.Sprint(entry.Source), "read_only": entry.ReadOnly, "sensitive": entry.Sensitive, "allowed": allow[entry.Name]})
	}
	return map[string]interface{}{"topic": topic, "allowlist": mapKeys(allow), "configs": configs}, nil
}

func (s *Service) AlterTopicConfigs(cluster model.KafkaCluster, topic string, configs map[string]*string) error {
	admin, err := s.admin(cluster)
	if err != nil {
		return err
	}
	defer admin.Close()
	return admin.AlterConfig(sarama.TopicResource, topic, configs, false)
}

func (s *Service) Metrics(cluster model.KafkaCluster, topicLimit, groupLimit int) (map[string]interface{}, error) {
	started := time.Now()
	admin, err := s.admin(cluster)
	if err != nil {
		return nil, err
	}
	defer admin.Close()
	client, err := s.client(cluster)
	if err != nil {
		return nil, err
	}
	brokers, _, _ := admin.DescribeCluster()
	topicsMap, err := admin.ListTopics()
	if err != nil {
		return nil, err
	}

	groupsMap, err := admin.ListConsumerGroups()
	if err != nil {
		return nil, err
	}
	groups := make([]string, 0, len(groupsMap))
	for group := range groupsMap {
		groups = append(groups, group)
	}
	sort.Strings(groups)
	limitedGroups := false
	if groupLimit > 0 && len(groups) > groupLimit {
		groups = groups[:groupLimit]
		limitedGroups = true
	}

	memberCounts := map[string]int{}
	if len(groups) > 0 {
		if descriptions, err := admin.DescribeConsumerGroups(groups); err == nil {
			for _, desc := range descriptions {
				if desc != nil {
					memberCounts[desc.GroupId] = len(desc.Members)
				}
			}
		}
	}

	visibleTopics := 0
	visibleTopicNames := make([]string, 0, len(topicsMap))
	for topic := range topicsMap {
		if !strings.HasPrefix(topic, "__") {
			visibleTopics++
			visibleTopicNames = append(visibleTopicNames, topic)
		}
	}
	logSizes := logDirSizes(admin, client)
	var maxTopicLogSize int64
	maxTopicLogSizeName := ""
	var underReplicatedPartitions int64
	var offlinePartitions int64
	for _, topic := range visibleTopicNames {
		var topicLogSize int64
		for _, size := range logSizes.leader[topic] {
			topicLogSize += size
		}
		if topicLogSize > maxTopicLogSize {
			maxTopicLogSize = topicLogSize
			maxTopicLogSizeName = topic
		}
		partitions, err := client.Partitions(topic)
		if err != nil {
			continue
		}
		for _, partition := range partitions {
			replicas, _ := client.Replicas(topic, partition)
			isr, _ := client.InSyncReplicas(topic, partition)
			if len(replicas) > 0 && len(isr) < len(replicas) {
				underReplicatedPartitions++
			}
			leader, err := client.Leader(topic, partition)
			if err != nil || leader == nil {
				offlinePartitions++
			}
		}
	}

	var totalLag int64

	items := make([]map[string]interface{}, 0, len(groups))
	for _, group := range groups {
		offsets, err := admin.ListConsumerGroupOffsets(group, nil)
		if err != nil || offsets == nil {
			continue
		}
		topics := map[string]bool{}
		var groupLag int64
		var totalCommitted int64
		var totalEnd int64
		partitions := 0
		for topic, blocks := range offsets.Blocks {
			for partition, block := range blocks {
				end, _ := client.GetOffset(topic, partition, sarama.OffsetNewest)
				committed := block.Offset
				lag := int64(0)
				if committed >= 0 && end > committed {
					lag = end - committed
				}
				topics[topic] = true
				partitions++
				groupLag += lag
				if committed > 0 {
					totalCommitted += committed
				}
				if end > 0 {
					totalEnd += end
				}
			}
		}
		totalLag += groupLag
		items = append(items, map[string]interface{}{"group": group, "total_lag": groupLag, "total_committed_offset": totalCommitted, "total_end_offset": totalEnd, "member_count": memberCounts[group], "topic_count": len(topics), "partition_count": partitions})
	}

	unavailableBrokerCount := len(strings.Split(cluster.BootstrapServers, ",")) - len(brokers)
	if unavailableBrokerCount < 0 {
		unavailableBrokerCount = 0
	}
	return map[string]interface{}{"timestamp": time.Now().UnixNano() / int64(time.Millisecond), "elapsed_seconds": time.Since(started).Seconds(), "broker_count": len(brokers), "unavailable_broker_count": unavailableBrokerCount, "topic_count": visibleTopics, "group_count": len(groupsMap), "total_lag": totalLag, "max_topic_log_size": maxTopicLogSize, "max_topic_log_size_topic": maxTopicLogSizeName, "under_replicated_partition_count": underReplicatedPartitions, "offline_partition_count": offlinePartitions, "topics": []interface{}{}, "groups": items, "limited": map[string]interface{}{"topics": topicLimit, "groups": groupLimit, "groups_limited": limitedGroups}}, nil
}

func (s *Service) StreamTopicData(cluster model.KafkaCluster, topic string, partition int32, offset *int64, keySearch, valueSearch string, send func(map[string]interface{}) bool) error {
	consumer, err := s.consumer(cluster)
	if err != nil {
		return err
	}
	defer consumer.Close()
	start := sarama.OffsetNewest
	if offset != nil {
		start = *offset
	}
	pc, err := consumer.ConsumePartition(topic, partition, start)
	if err != nil {
		return err
	}
	defer pc.Close()
	keyFilter := strings.ToLower(strings.TrimSpace(keySearch))
	valueFilter := strings.ToLower(strings.TrimSpace(valueSearch))
	for msg := range pc.Messages() {
		key := string(msg.Key)
		value := string(msg.Value)
		if keyFilter != "" && !strings.Contains(strings.ToLower(key), keyFilter) {
			continue
		}
		if valueFilter != "" && !strings.Contains(strings.ToLower(value), valueFilter) {
			continue
		}
		payload := map[string]interface{}{"topic": topic, "partition": partition, "offset": msg.Offset, "timestamp": msg.Timestamp.UnixNano() / int64(time.Millisecond), "timestamp_type": 0, "headers": []interface{}{}, "key": key, "value": truncate(value, s.cfg.KafkaMaxValueLength)}
		if !send(payload) {
			return nil
		}
	}
	return nil
}

func JSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func truncate(value string, maxBytes int) string {
	if maxBytes <= 0 || len([]byte(value)) <= maxBytes {
		return value
	}
	runes := []rune(value)
	for len([]byte(string(runes))) > maxBytes && len(runes) > 0 {
		runes = runes[:len(runes)-1]
	}
	return string(runes) + fmt.Sprintf("\n\n... [truncated %d bytes, showing first %d bytes]", len([]byte(value)), maxBytes)
}

func mapKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
