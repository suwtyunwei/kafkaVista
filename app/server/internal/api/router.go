package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"gorm.io/gorm"

	"kafkavista/server/internal/config"
	kafkasvc "kafkavista/server/internal/kafka"
	"kafkavista/server/internal/model"
	"kafkavista/server/internal/store"
)

type API struct {
	cfg   config.Config
	db    *gorm.DB
	kafka *kafkasvc.Service
}

type user struct {
	Username string
	Role     string
}

func NewRouter(cfg config.Config, db *gorm.DB) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	corsConfig := cors.Config{
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization"},
		AllowCredentials: true,
	}
	if cfg.CORSAllowedOrigins != "" {
		corsConfig.AllowOrigins = strings.Split(cfg.CORSAllowedOrigins, ",")
	} else {
		corsConfig.AllowOriginFunc = func(origin string) bool { return true }
	}
	r.Use(gin.LoggerWithFormatter(func(param gin.LogFormatterParams) string {
		return fmt.Sprintf("%s | %3d | %13v | %15s | %-7s %#v\n", param.TimeStamp.Format("2006-01-02 15:04:05"), param.StatusCode, param.Latency, param.ClientIP, param.Method, param.Path)
	}), gin.Recovery(), cors.New(corsConfig))
	a := &API{cfg: cfg, db: db, kafka: kafkasvc.New(cfg)}

	r.GET("/api/health", a.health)
	r.GET("/api/docs", apiDocs(r, a))
	r.GET("/api/app/status", a.appStatus)
	r.POST("/api/license/tokens", a.generateLicenseToken)
	r.GET("/api/license/tokens/inspect", a.inspectLicenseToken)
	r.GET("/api/auth/sso", a.ssoStatus)
	r.GET("/api/auth/oidc/login", a.oidcLogin)
	r.GET("/api/auth/oidc/callback", a.oidcCallback)
	r.POST("/api/auth/login", a.loginRateLimit(), a.login)
	r.GET("/metrics", a.exporterMetrics)
	r.GET("/metrics/:cluster", a.exporterMetrics)

	adminAPI := r.Group("/api/admin", a.authRequired(), a.adminOnly())
	adminAPI.Use(a.auditMutations())
	adminAPI.GET("/settings", a.getSettings)
	adminAPI.PUT("/settings", a.saveSettings)
	adminAPI.POST("/settings/notification/test", a.testNotification)
	adminAPI.POST("/settings/alerting/test", a.testAlerting)
	adminAPI.POST("/settings/alerting/value", a.alertRuleValue)
	adminAPI.POST("/settings/ldap/test", a.testLDAP)
	adminAPI.POST("/settings/ldap/sync-users", a.syncLDAPUsers)
	adminAPI.GET("/users", a.listUsers)
	adminAPI.POST("/users", a.createUser)
	adminAPI.PUT("/users/:username", a.updateUser)
	adminAPI.DELETE("/users/:username", a.deleteUser)
	adminAPI.GET("/roles", a.listRoles)
	adminAPI.POST("/roles", a.createRole)
	adminAPI.DELETE("/roles/:role", a.deleteRole)

	kafkaAPI := r.Group("/kafka-api", a.authRequired())
	kafkaAPI.Use(a.auditMutations())
	a.registerKafka(kafkaAPI)
	legacy := r.Group("/api/kafka", a.authRequired())
	legacy.Use(a.auditMutations())
	a.registerKafka(legacy)
	go a.startAlertScheduler()
	go a.startAuditRetentionScheduler()
	return r
}

func (a *API) registerKafka(r *gin.RouterGroup) {
	r.GET("/actions", a.actions)
	r.GET("/migrations", a.adminOnly(), a.listMigrations)
	r.POST("/migrations/check", a.adminOnly(), a.checkMigration)
	r.POST("/migrations", a.adminOnly(), a.createMigration)
	r.GET("/migrations/:job", a.adminOnly(), a.getMigration)
	r.POST("/migrations/:job/stop", a.adminOnly(), a.stopMigration)
	r.GET("/clusters", a.listClusters)
	r.POST("/clusters", a.adminOnly(), a.createCluster)
	r.PUT("/clusters/:cluster", a.adminOnly(), a.updateCluster)
	r.DELETE("/clusters/:cluster", a.adminOnly(), a.deleteCluster)
	r.GET("/clusters/:cluster/stats", a.clusterStats)
	r.GET("/clusters/:cluster/overview", a.clusterOverview)
	r.GET("/clusters/:cluster/detail", a.clusterDetail)
	r.GET("/clusters/:cluster/metrics", a.clusterMetrics)
	r.GET("/clusters/:cluster/topics", a.topicPage)
	r.POST("/clusters/:cluster/topics", a.createTopic)
	r.GET("/clusters/:cluster/topics/:topic", a.topicDetail)
	r.PUT("/clusters/:cluster/topics/:topic/partitions", a.updateTopicPartitions)
	r.DELETE("/clusters/:cluster/topics/:topic", a.deleteTopic)
	r.GET("/clusters/:cluster/topics/:topic/data", a.topicData)
	r.GET("/clusters/:cluster/topics/:topic/stream", a.topicStream)
	r.GET("/clusters/:cluster/topics/:topic/configs", a.topicConfigs)
	r.PUT("/clusters/:cluster/topics/:topic/configs", a.updateTopicConfigs)
	r.DELETE("/clusters/:cluster/topics/:topic/configs/:config", a.deleteTopicConfig)
	r.GET("/clusters/:cluster/messages", a.messages)
	r.POST("/clusters/:cluster/messages", a.sendMessage)
	r.DELETE("/clusters/:cluster/messages", a.deleteRecords)
	r.GET("/clusters/:cluster/groups/summary", a.groupSummary)
	r.POST("/clusters/:cluster/groups/:group", a.createGroupPlaceholder)
	r.GET("/clusters/:cluster/groups/:group", a.groupDetail)
	r.GET("/clusters/:cluster/groups/:group/history", a.groupHistory)
	r.DELETE("/clusters/:cluster/groups/:group/topics/:topic", a.deleteGroupTopic)
	r.DELETE("/clusters/:cluster/groups/:group", a.deleteGroup)
	r.GET("/clusters/:cluster/permissions", a.listPermissions)
	r.PUT("/clusters/:cluster/permissions", a.savePermissions)
	r.GET("/audit-logs", a.adminOnly(), a.auditLogs)
}

func (a *API) health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "version": "1.0.0", "default_bootstrap_servers": a.cfg.DefaultBootstrapServers})
}

func (a *API) exporterMetrics(c *gin.Context) {
	settings := a.authSettings()
	if !settings.Monitoring.Enabled {
		c.String(http.StatusNotFound, "KafkaVista monitoring exporter is disabled\n")
		return
	}
	var clusters []model.KafkaCluster
	if target := c.Param("cluster"); target != "" {
		var cluster model.KafkaCluster
		if err := a.db.Where("is_active = ? AND (id = ? OR name = ?)", true, target, target).First(&cluster).Error; err != nil {
			errorJSON(c, http.StatusNotFound, "Kafka 集群不存在")
			return
		}
		clusters = []model.KafkaCluster{cluster}
	} else {
		a.db.Where("is_active = ?", true).Order("name asc").Find(&clusters)
	}
	text, err := a.kafka.PrometheusMetrics(clusters, kafkasvc.ExporterOptions{TopicLimit: settings.Monitoring.TopicLimit, GroupLimit: settings.Monitoring.GroupLimit})
	if err != nil {
		errorJSON(c, http.StatusInternalServerError, err.Error())
		return
	}
	c.Header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	c.String(http.StatusOK, text)
}

var loginLimiter = struct {
	sync.Mutex
	attempts map[string][]time.Time
}{attempts: map[string][]time.Time{}}

func (a *API) loginRateLimit() gin.HandlerFunc {
	return func(c *gin.Context) {
		ip := c.ClientIP()
		loginLimiter.Lock()
		defer loginLimiter.Unlock()
		now := time.Now()
		cutoff := now.Add(-5 * time.Minute)
		var recent []time.Time
		for _, t := range loginLimiter.attempts[ip] {
			if t.After(cutoff) {
				recent = append(recent, t)
			}
		}
		if len(recent) >= 10 {
			loginLimiter.attempts[ip] = recent
			c.JSON(http.StatusTooManyRequests, gin.H{"code": 429, "message": "登录尝试过于频繁，请 5 分钟后再试"})
			c.Abort()
			return
		}
		recent = append(recent, now)
		loginLimiter.attempts[ip] = recent
		c.Next()
	}
}

func (a *API) login(c *gin.Context) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
		UseLDAP  bool   `json:"use_ldap"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		errorJSON(c, http.StatusBadRequest, "请求格式错误")
		return
	}
	role := "user"
	displayName := req.Username
	source := "local"
	settings := a.authSettings()
	if req.UseLDAP {
		if settings.OIDC.Enabled {
			errorJSON(c, http.StatusForbidden, "OIDC 启用时不支持 LDAP 登录")
			return
		}
		ldapUser, err := a.authenticateLDAP(req.Username, req.Password)
		if err != nil {
			errorJSON(c, http.StatusUnauthorized, err.Error())
			return
		}
		if !ldapUser.IsActive {
			errorJSON(c, http.StatusForbidden, "用户已禁用")
			return
		}
		role = ldapUser.Role
		displayName = ldapUser.DisplayName
		source = "ldap"
	} else {
		var dbUser model.AppUser
		if err := a.db.Where("username = ?", req.Username).First(&dbUser).Error; err != nil {
			errorJSON(c, http.StatusUnauthorized, "用户名或密码错误")
			return
		}
		if !dbUser.IsActive {
			errorJSON(c, http.StatusForbidden, "用户已禁用")
			return
		}
		if dbUser.Source != "local" || dbUser.PasswordHash == "" || !store.VerifyPassword(dbUser.PasswordHash, req.Password) {
			errorJSON(c, http.StatusUnauthorized, "用户名或密码错误")
			return
		}
		role = dbUser.Role
		displayName = dbUser.DisplayName
	}
	text, err := a.issueToken(req.Username, role)
	if err != nil {
		errorJSON(c, http.StatusInternalServerError, "生成 Token 失败")
		return
	}
	a.upsertUser(req.Username, displayName, "", role, source)
	c.JSON(http.StatusOK, gin.H{"code": 0, "data": gin.H{"access_token": text, "token_type": "bearer", "user": gin.H{"username": req.Username, "display_name": displayName, "role": role}}})
}

func (a *API) issueToken(username string, role string) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": username, "role": role, "type": "access", "exp": time.Now().Add(time.Duration(a.cfg.JWTExpireHours) * time.Hour).Unix()})
	return token.SignedString([]byte(a.cfg.JWTSecret))
}

func (a *API) authRequired() gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		if !strings.HasPrefix(header, "Bearer ") {
			errorJSON(c, http.StatusUnauthorized, "Invalid or expired token")
			c.Abort()
			return
		}
		tokenText := strings.TrimPrefix(header, "Bearer ")
		claims := jwt.MapClaims{}
		token, err := jwt.ParseWithClaims(tokenText, claims, func(token *jwt.Token) (interface{}, error) { return []byte(a.cfg.JWTSecret), nil })
		if err != nil || !token.Valid || claims["type"] != "access" || claims["sub"] == nil {
			errorJSON(c, http.StatusUnauthorized, "Invalid or expired token")
			c.Abort()
			return
		}
		role, _ := claims["role"].(string)
		c.Set("user", user{Username: claims["sub"].(string), Role: role})
		c.Next()
	}
}

func (a *API) adminOnly() gin.HandlerFunc {
	return func(c *gin.Context) {
		if current(c).Role != "admin" {
			errorJSON(c, http.StatusForbidden, "Admin access required")
			c.Abort()
			return
		}
		c.Next()
	}
}

func current(c *gin.Context) user {
	value, _ := c.Get("user")
	if u, ok := value.(user); ok {
		return u
	}
	return user{}
}

func (a *API) actions(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"code": 0, "data": gin.H{"actions": model.AllActions}})
}

func (a *API) listClusters(c *gin.Context) {
	u := current(c)
	var clusters []model.KafkaCluster
	a.db.Where("is_active = ?", true).Order("created_at desc").Find(&clusters)
	items := []gin.H{}
	for _, cluster := range clusters {
		actions := a.allowedActions(u, cluster.ID)
		if u.Role != "admin" && !contains(actions, "cluster_view") {
			continue
		}
		item := clusterJSON(cluster)
		item["permissions"] = actions
		items = append(items, item)
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "data": gin.H{"items": items, "total": len(items)}})
}

func (a *API) createCluster(c *gin.Context) {
	var req model.KafkaCluster
	if err := c.ShouldBindJSON(&req); err != nil || req.Name == "" || req.BootstrapServers == "" {
		errorJSON(c, http.StatusBadRequest, "集群名称和地址不能为空")
		return
	}
	if !a.enterpriseEnabled() {
		var activeCount int64
		a.db.Model(&model.KafkaCluster{}).Where("is_active = ?", true).Count(&activeCount)
		if activeCount >= 10 {
			errorJSON(c, http.StatusForbidden, "社区版最多只能管理 10 个 Kafka 实例，完整版不限制实例数量")
			return
		}
	}
	req.ID = ""
	req.IsActive = true
	if req.SecurityProtocol == "" {
		req.SecurityProtocol = "PLAINTEXT"
	}
	if err := a.db.Create(&req).Error; err != nil {
		errorJSON(c, http.StatusBadRequest, "集群名称已存在")
		return
	}
	for _, action := range model.AllActions {
		a.db.Create(&model.KafkaPermission{ClusterID: req.ID, Username: current(c).Username, Action: action, CreatedBy: current(c).Username})
	}
	a.audit(current(c).Username, req.ID, "cluster_create", req.Name, req)
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success", "data": clusterJSON(req)})
}

func (a *API) updateCluster(c *gin.Context) {
	cluster, ok := a.cluster(c)
	if !ok {
		return
	}
	var req model.KafkaCluster
	if err := c.ShouldBindJSON(&req); err != nil || req.Name == "" || req.BootstrapServers == "" {
		errorJSON(c, http.StatusBadRequest, "集群名称和地址不能为空")
		return
	}
	cluster.Name = req.Name
	cluster.ClusterType = req.ClusterType
	cluster.BootstrapServers = req.BootstrapServers
	cluster.SecurityProtocol = req.SecurityProtocol
	cluster.SASLMechanism = req.SASLMechanism
	cluster.SASLUsername = req.SASLUsername
	cluster.SASLPassword = req.SASLPassword
	cluster.Description = req.Description
	a.db.Save(&cluster)
	a.audit(current(c).Username, cluster.ID, "cluster_update", cluster.Name, req)
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success", "data": clusterJSON(cluster)})
}

func (a *API) deleteCluster(c *gin.Context) {
	cluster, ok := a.cluster(c)
	if !ok {
		return
	}
	cluster.IsActive = false
	a.db.Save(&cluster)
	a.audit(current(c).Username, cluster.ID, "cluster_delete", cluster.Name, gin.H{})
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success"})
}

func (a *API) clusterStats(c *gin.Context) {
	a.withClusterPerm(c, "cluster_view", func(cluster model.KafkaCluster) { data, err := a.kafka.Stats(cluster); result(c, data, err) })
}
func (a *API) clusterOverview(c *gin.Context) {
	a.withClusterPerm(c, "cluster_view", func(cluster model.KafkaCluster) {
		data, err := a.kafka.Overview(cluster)
		if err == nil {
			a.mergeGroupPlaceholders(cluster.ID, data)
		}
		result(c, data, err)
	})
}

func (a *API) mergeGroupPlaceholders(clusterID string, data map[string]interface{}) {
	var placeholders []model.KafkaGroupPlaceholder
	a.db.Where("cluster_id = ?", clusterID).Find(&placeholders)
	if len(placeholders) == 0 {
		return
	}
	seen := map[string]bool{}
	groups := []string{}
	if raw, ok := data["groups"].([]string); ok {
		groups = append(groups, raw...)
	} else if raw, ok := data["groups"].([]interface{}); ok {
		for _, item := range raw {
			if group, ok := item.(string); ok {
				groups = append(groups, group)
			}
		}
	}
	for _, group := range groups {
		seen[group] = true
	}
	for _, placeholder := range placeholders {
		if placeholder.GroupID != "" && !seen[placeholder.GroupID] {
			groups = append(groups, placeholder.GroupID)
			seen[placeholder.GroupID] = true
		}
	}
	sort.Strings(groups)
	data["groups"] = groups
	data["group_count"] = len(groups)
}
func (a *API) clusterDetail(c *gin.Context) {
	a.withClusterPerm(c, "cluster_view", func(cluster model.KafkaCluster) {
		data, err := a.kafka.Detail(cluster, c.DefaultQuery("include_topics", "true") != "false")
		result(c, data, err)
	})
}
func (a *API) clusterMetrics(c *gin.Context) {
	a.withClusterPerm(c, "cluster_view", func(cluster model.KafkaCluster) {
		if c.Query("from") != "" || c.Query("to") != "" || c.Query("range") != "" {
			a.clusterMetricHistory(c, cluster)
			return
		}
		data, err := a.kafka.Metrics(cluster, intQuery(c, "topic_limit", 30), intQuery(c, "group_limit", 30))
		if err == nil {
			a.saveMetricSnapshot(cluster, data)
		}
		result(c, data, err)
	})
}

func (a *API) saveMetricSnapshot(cluster model.KafkaCluster, data map[string]interface{}) {
	a.saveClusterMetricSnapshot(cluster, data)
	rawGroups := metricGroups(data["groups"])
	if len(rawGroups) == 0 {
		return
	}
	now := time.Now()
	rows := make([]model.KafkaGroupMetric, 0, len(rawGroups))
	for _, group := range rawGroups {
		name, _ := group["group"].(string)
		if name == "" {
			continue
		}
		rows = append(rows, model.KafkaGroupMetric{ClusterID: cluster.ID, GroupID: name, TotalLag: toInt64(group["total_lag"]), TotalCommittedOffset: toInt64(group["total_committed_offset"]), TotalEndOffset: toInt64(group["total_end_offset"]), MemberCount: toInt(group["member_count"]), TopicCount: toInt(group["topic_count"]), PartitionCount: toInt(group["partition_count"]), CreatedAt: now})
	}
	if len(rows) > 0 {
		a.db.Create(&rows)
	}
}

func (a *API) saveClusterMetricSnapshot(cluster model.KafkaCluster, data map[string]interface{}) {
	now := time.Now()
	row := model.KafkaClusterMetric{ClusterID: cluster.ID, BrokerCount: toInt(data["broker_count"]), TopicCount: toInt(data["topic_count"]), GroupCount: toInt(data["group_count"]), TotalLag: toInt64(data["total_lag"]), TotalProducedOffset: toInt64(data["total_produced_offset"]), TotalConsumedOffset: toInt64(data["total_consumed_offset"]), TopTopics: fmt.Sprint(data["top_topics"]), CreatedAt: now}
	var prev model.KafkaClusterMetric
	if err := a.db.Where("cluster_id = ?", cluster.ID).Order("created_at desc").First(&prev).Error; err == nil {
		minutes := math.Max(now.Sub(prev.CreatedAt).Minutes(), 1.0/60.0)
		producedDelta := row.TotalProducedOffset - prev.TotalProducedOffset
		consumedDelta := row.TotalConsumedOffset - prev.TotalConsumedOffset
		if producedDelta > 0 {
			row.TopicMessagesPerMinute = float64(producedDelta) / minutes
			row.TopicMessagesPerSecond = row.TopicMessagesPerMinute / 60
		}
		if consumedDelta > 0 {
			row.ConsumeMessagesPerMinute = float64(consumedDelta) / minutes
		}
	}
	a.db.Create(&row)
}

func (a *API) collectExporterMetricSnapshot(cluster model.KafkaCluster) error {
	settings := a.authSettings()
	text, err := a.kafka.PrometheusMetrics([]model.KafkaCluster{cluster}, kafkasvc.ExporterOptions{TopicLimit: settings.Monitoring.TopicLimit, GroupLimit: settings.Monitoring.GroupLimit})
	if err != nil {
		return err
	}
	data, groups := parseExporterSnapshot(cluster.ID, text)
	a.saveClusterMetricSnapshot(cluster, data)
	if len(groups) > 0 {
		now := time.Now()
		rows := make([]model.KafkaGroupMetric, 0, len(groups))
		for _, group := range groups {
			rows = append(rows, model.KafkaGroupMetric{ClusterID: cluster.ID, GroupID: group.Group, TotalLag: group.TotalLag, TotalCommittedOffset: group.TotalCommittedOffset, TotalEndOffset: group.TotalEndOffset, MemberCount: group.MemberCount, TopicCount: len(group.Topics), PartitionCount: group.PartitionCount, CreatedAt: now})
		}
		a.db.Create(&rows)
	}
	return nil
}

type exporterGroupSnapshot struct {
	Group                string
	TotalLag             int64
	TotalCommittedOffset int64
	TotalEndOffset       int64
	MemberCount          int
	Topics               map[string]bool
	PartitionCount       int
}

func parseExporterSnapshot(clusterID, text string) (map[string]interface{}, map[string]*exporterGroupSnapshot) {
	topicNames := map[string]bool{}
	topicOffsets := map[string]int64{}
	groupNames := map[string]bool{}
	groups := map[string]*exporterGroupSnapshot{}
	data := map[string]interface{}{"broker_count": 0, "topic_count": 0, "group_count": 0, "total_lag": int64(0), "total_produced_offset": int64(0), "total_consumed_offset": int64(0), "top_topics": ""}
	for _, line := range strings.Split(text, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, labels, value, ok := parsePrometheusLine(line)
		if !ok || labels["cluster_id"] != clusterID {
			continue
		}
		switch name {
		case "kafka_brokers":
			data["broker_count"] = int(value)
		case "kafka_topic_partitions":
			if topic := labels["topic"]; topic != "" {
				topicNames[topic] = true
			}
		case "kafka_topic_partition_current_offset":
			if topic := labels["topic"]; topic != "" {
				topicOffsets[topic] += int64(value)
				topicNames[topic] = true
			}
		case "kafka_consumergroup_members":
			group := labels["consumergroup"]
			if group == "" {
				continue
			}
			item := exporterGroup(groups, group)
			item.MemberCount = int(value)
			groupNames[group] = true
		case "kafka_consumergroup_current_offset":
			group := labels["consumergroup"]
			if group == "" {
				continue
			}
			item := exporterGroup(groups, group)
			item.TotalCommittedOffset += int64(value)
		case "kafka_consumergroup_lag":
			group := labels["consumergroup"]
			if group == "" {
				continue
			}
			item := exporterGroup(groups, group)
			lag := int64(value)
			item.TotalLag += lag
			item.TotalEndOffset += lag
			item.PartitionCount++
			if topic := labels["topic"]; topic != "" {
				item.Topics[topic] = true
			}
			groupNames[group] = true
		}
	}
	var totalLag int64
	var totalConsumed int64
	for _, group := range groups {
		group.TotalEndOffset += group.TotalCommittedOffset
		totalLag += group.TotalLag
		totalConsumed += group.TotalCommittedOffset
	}
	var totalProduced int64
	topics := make([]string, 0, len(topicOffsets))
	for topic, offset := range topicOffsets {
		totalProduced += offset
		topics = append(topics, topic)
	}
	sort.Slice(topics, func(i, j int) bool { return topicOffsets[topics[i]] > topicOffsets[topics[j]] })
	if len(topics) > 6 {
		topics = topics[:6]
	}
	data["topic_count"] = len(topicNames)
	data["group_count"] = len(groupNames)
	data["total_lag"] = totalLag
	data["total_produced_offset"] = totalProduced
	data["total_consumed_offset"] = totalConsumed
	data["top_topics"] = strings.Join(topics, ", ")
	return data, groups
}

func exporterGroup(groups map[string]*exporterGroupSnapshot, group string) *exporterGroupSnapshot {
	item := groups[group]
	if item == nil {
		item = &exporterGroupSnapshot{Group: group, Topics: map[string]bool{}}
		groups[group] = item
	}
	return item
}

func parsePrometheusLine(line string) (string, map[string]string, float64, bool) {
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return "", nil, 0, false
	}
	value, err := strconv.ParseFloat(parts[len(parts)-1], 64)
	if err != nil {
		return "", nil, 0, false
	}
	head := parts[0]
	labels := map[string]string{}
	if idx := strings.IndexByte(head, '{'); idx >= 0 {
		name := head[:idx]
		labelText := strings.TrimSuffix(head[idx+1:], "}")
		for _, pair := range strings.Split(labelText, ",") {
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) == 2 {
				labels[kv[0]] = strings.Trim(kv[1], `"`)
			}
		}
		return name, labels, value, true
	}
	return head, labels, value, true
}

func metricGroups(value interface{}) []map[string]interface{} {
	switch groups := value.(type) {
	case []map[string]interface{}:
		return groups
	case []interface{}:
		items := make([]map[string]interface{}, 0, len(groups))
		for _, raw := range groups {
			if group, ok := raw.(map[string]interface{}); ok {
				items = append(items, group)
			}
		}
		return items
	default:
		return nil
	}
}

func (a *API) clusterMetricHistory(c *gin.Context, cluster model.KafkaCluster) {
	_ = a.collectExporterMetricSnapshot(cluster)
	now := time.Now()
	from := now.Add(-time.Hour)
	to := now
	if r := intQuery(c, "range", 0); r > 0 {
		from = now.Add(-time.Duration(r) * time.Minute)
	}
	if v := int64Ptr(c.Query("from")); v != nil {
		from = time.UnixMilli(*v)
	}
	if v := int64Ptr(c.Query("to")); v != nil {
		to = time.UnixMilli(*v)
	}
	var clusterRows []model.KafkaClusterMetric
	a.db.Where("cluster_id = ? AND created_at BETWEEN ? AND ?", cluster.ID, from, to).Order("created_at asc").Limit(2000).Find(&clusterRows)
	points := make([]gin.H, 0, len(clusterRows))
	for _, row := range clusterRows {
		points = append(points, gin.H{"created_at": row.CreatedAt, "broker_count": row.BrokerCount, "topic_count": row.TopicCount, "group_count": row.GroupCount, "total_lag": row.TotalLag, "topic_messages_per_second": row.TopicMessagesPerSecond, "topic_messages_per_minute": row.TopicMessagesPerMinute, "consume_messages_per_minute": row.ConsumeMessagesPerMinute, "top_topics": row.TopTopics})
	}
	var rows []model.KafkaGroupMetric
	a.db.Where("cluster_id = ? AND created_at BETWEEN ? AND ?", cluster.ID, from, to).Order("created_at asc").Limit(2000).Find(&rows)
	items := make([]gin.H, 0, len(rows))
	for _, row := range rows {
		items = append(items, gin.H{"group": row.GroupID, "total_lag": row.TotalLag, "member_count": row.MemberCount, "topic_count": row.TopicCount, "partition_count": row.PartitionCount, "created_at": row.CreatedAt})
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "data": gin.H{"cluster_id": cluster.ID, "from": from, "to": to, "points": points, "items": items}})
}

func (a *API) topicPage(c *gin.Context) {
	a.withClusterPerm(c, "cluster_view", func(cluster model.KafkaCluster) {
		data, err := a.kafka.TopicPage(cluster, intQuery(c, "page", 1), intQuery(c, "page_size", 50), c.Query("name"), c.Query("sort_by"), c.DefaultQuery("sort_order", "desc"))
		result(c, data, err)
	})
}

func (a *API) topicDetail(c *gin.Context) {
	a.withClusterPerm(c, "cluster_view", func(cluster model.KafkaCluster) {
		data, err := a.kafka.DescribeTopic(cluster, c.Param("topic"))
		result(c, data, err)
	})
}

func (a *API) createTopic(c *gin.Context) {
	a.withClusterPerm(c, "topic_create", func(cluster model.KafkaCluster) {
		var req struct {
			Topic             string `json:"topic"`
			Partitions        int32  `json:"partitions"`
			ReplicationFactor int32  `json:"replication_factor"`
		}
		if err := c.ShouldBindJSON(&req); err != nil || req.Topic == "" {
			errorJSON(c, http.StatusBadRequest, "Topic 名称不能为空")
			return
		}
		if req.Partitions < 1 {
			req.Partitions = 1
		}
		if req.ReplicationFactor < 1 {
			req.ReplicationFactor = 1
		}
		err := a.kafka.CreateTopic(cluster, req.Topic, req.Partitions, req.ReplicationFactor)
		if err != nil {
			kafkaError(c, err)
			return
		}
		a.audit(current(c).Username, cluster.ID, "topic_create", req.Topic, req)
		c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success"})
	})
}

func (a *API) deleteTopic(c *gin.Context) {
	a.withClusterPerm(c, "topic_delete", func(cluster model.KafkaCluster) {
		if err := a.kafka.DeleteTopic(cluster, c.Param("topic")); err != nil {
			kafkaError(c, err)
			return
		}
		a.audit(current(c).Username, cluster.ID, "topic_delete", c.Param("topic"), gin.H{})
		c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success"})
	})
}

func (a *API) updateTopicPartitions(c *gin.Context) {
	a.withClusterPerm(c, "topic_manage", func(cluster model.KafkaCluster) {
		var req struct {
			Partitions int32 `json:"partitions"`
		}
		if err := c.ShouldBindJSON(&req); err != nil || req.Partitions < 1 {
			errorJSON(c, http.StatusBadRequest, "分区数必须大于 0")
			return
		}
		if err := a.kafka.AlterTopicPartitions(cluster, c.Param("topic"), req.Partitions); err != nil {
			kafkaError(c, err)
			return
		}
		a.audit(current(c).Username, cluster.ID, "topic_partitions_update", c.Param("topic"), req)
		c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success"})
	})
}

func (a *API) topicData(c *gin.Context) {
	a.withClusterPerm(c, "message_read", func(cluster model.KafkaCluster) {
		var data map[string]interface{}
		var err error
		endOffset := int64QueryPtr(c, "end_offset", "endOffset")
		if strings.EqualFold(c.Query("partition"), "all") {
			data, err = a.kafka.ReadTopicDataAll(cluster, c.Param("topic"), c.DefaultQuery("autoOffsetReset", "newest"), int64Ptr(c.Query("offset")), endOffset, int64Ptr(c.Query("start_time_ms")), int64Ptr(c.Query("end_time_ms")), c.Query("key_search"), c.Query("value_search"), intQuery(c, "count", 20), intQuery(c, "timeout_ms", 1500))
		} else {
			data, err = a.kafka.ReadTopicData(cluster, c.Param("topic"), int32(intQuery(c, "partition", 0)), c.DefaultQuery("autoOffsetReset", "newest"), int64Ptr(c.Query("offset")), endOffset, int64Ptr(c.Query("start_time_ms")), int64Ptr(c.Query("end_time_ms")), c.Query("key_search"), c.Query("value_search"), intQuery(c, "count", 20), intQuery(c, "timeout_ms", 1500))
		}
		result(c, data, err)
	})
}

func (a *API) messages(c *gin.Context) {
	a.withClusterPerm(c, "message_read", func(cluster model.KafkaCluster) {
		var data map[string]interface{}
		var err error
		endOffset := int64QueryPtr(c, "end_offset", "endOffset")
		if strings.EqualFold(c.Query("partition"), "all") {
			data, err = a.kafka.ReadTopicDataAll(cluster, c.Query("topic"), "newest", int64Ptr(c.Query("offset")), endOffset, nil, nil, "", "", intQuery(c, "max_messages", 50), intQuery(c, "timeout_ms", 1500))
		} else {
			data, err = a.kafka.ReadTopicData(cluster, c.Query("topic"), int32(intQuery(c, "partition", 0)), "newest", int64Ptr(c.Query("offset")), endOffset, nil, nil, "", "", intQuery(c, "max_messages", 50), intQuery(c, "timeout_ms", 1500))
		}
		result(c, data, err)
	})
}

func (a *API) sendMessage(c *gin.Context) {
	a.withClusterPerm(c, "message_send", func(cluster model.KafkaCluster) {
		type produceItem struct {
			Partition *int32 `json:"partition"`
			Key       string `json:"key"`
			Value     string `json:"value"`
		}
		var req struct {
			Topic     string        `json:"topic"`
			Partition *int32        `json:"partition"`
			Key       string        `json:"key"`
			Value     string        `json:"value"`
			Messages  []produceItem `json:"messages"`
		}
		if err := c.ShouldBindJSON(&req); err != nil || req.Topic == "" {
			errorJSON(c, http.StatusBadRequest, "Topic 和消息内容不能为空")
			return
		}
		messages := make([]kafkasvc.ProduceMessage, 0, len(req.Messages))
		for _, item := range req.Messages {
			messages = append(messages, kafkasvc.ProduceMessage{Partition: item.Partition, Key: item.Key, Value: item.Value})
		}
		if len(messages) == 0 {
			messages = append(messages, kafkasvc.ProduceMessage{Partition: req.Partition, Key: req.Key, Value: req.Value})
		}
		if len(messages) > 1000 {
			errorJSON(c, http.StatusBadRequest, "单次最多写入 1000 条消息")
			return
		}
		if len(messages) > 1 && !a.enterpriseEnabled() {
			errorJSON(c, http.StatusForbidden, "完整版密钥已过期，批量导入消息不可用")
			return
		}
		data, err := a.kafka.SendMessages(cluster, req.Topic, messages)
		if err != nil {
			kafkaError(c, err)
			return
		}
		a.audit(current(c).Username, cluster.ID, "message_send", req.Topic, gin.H{"count": len(messages)})
		c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success", "data": data})
	})
}

func (a *API) deleteRecords(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "delete records is not enabled"})
}

func (a *API) topicStream(c *gin.Context) {
	a.withClusterPerm(c, "message_read", func(cluster model.KafkaCluster) {
		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		flusher, _ := c.Writer.(http.Flusher)
		_, _ = c.Writer.WriteString("event: ready\ndata: {\"status\":\"ready\"}\n\n")
		if flusher != nil {
			flusher.Flush()
		}
		_ = a.kafka.StreamTopicData(cluster, c.Param("topic"), int32(intQuery(c, "partition", 0)), int64Ptr(c.Query("offset")), c.Query("key_search"), c.Query("value_search"), func(payload map[string]interface{}) bool {
			_, err := c.Writer.WriteString("data: " + kafkasvc.JSON(payload) + "\n\n")
			if flusher != nil {
				flusher.Flush()
			}
			return err == nil
		})
	})
}

func (a *API) topicConfigs(c *gin.Context) {
	a.withClusterPerm(c, "cluster_view", func(cluster model.KafkaCluster) {
		data, err := a.kafka.TopicConfigs(cluster, c.Param("topic"))
		result(c, data, err)
	})
}

func (a *API) updateTopicConfigs(c *gin.Context) {
	a.withClusterPerm(c, "topic_config_manage", func(cluster model.KafkaCluster) {
		var req struct {
			Configs map[string]*string `json:"configs"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			errorJSON(c, http.StatusBadRequest, "请求格式错误")
			return
		}
		if err := a.kafka.AlterTopicConfigs(cluster, c.Param("topic"), req.Configs); err != nil {
			kafkaError(c, err)
			return
		}
		a.audit(current(c).Username, cluster.ID, "topic_config_update", c.Param("topic"), req.Configs)
		c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success"})
	})
}

func (a *API) deleteTopicConfig(c *gin.Context) {
	value := map[string]*string{c.Param("config"): nil}
	a.withClusterPerm(c, "topic_config_manage", func(cluster model.KafkaCluster) {
		if err := a.kafka.AlterTopicConfigs(cluster, c.Param("topic"), value); err != nil {
			kafkaError(c, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success"})
	})
}

func (a *API) groupSummary(c *gin.Context) {
	a.withClusterPerm(c, "cluster_view", func(cluster model.KafkaCluster) {
		ids := strings.Split(c.Query("group_ids"), ",")
		clean := []string{}
		for _, id := range ids {
			if strings.TrimSpace(id) != "" {
				clean = append(clean, strings.TrimSpace(id))
			}
		}
		data, err := a.kafka.GroupSummaries(cluster, clean)
		result(c, data, err)
	})
}

func (a *API) groupDetail(c *gin.Context) {
	a.withClusterPerm(c, "cluster_view", func(cluster model.KafkaCluster) {
		data, err := a.kafka.GroupDetail(cluster, c.Param("group"), c.Query("topic"))
		result(c, data, err)
	})
}
func (a *API) createGroupPlaceholder(c *gin.Context) {
	a.withClusterPerm(c, "group_create", func(cluster model.KafkaCluster) {
		group := strings.TrimSpace(c.Param("group"))
		if group == "" {
			errorJSON(c, http.StatusBadRequest, "Group ID 不能为空")
			return
		}
		row := model.KafkaGroupPlaceholder{ClusterID: cluster.ID, GroupID: group, CreatedBy: current(c).Username}
		if err := a.db.Where(model.KafkaGroupPlaceholder{ClusterID: cluster.ID, GroupID: group}).FirstOrCreate(&row).Error; err != nil {
			errorJSON(c, http.StatusInternalServerError, "保存 Consumer Group 失败")
			return
		}
		a.audit(current(c).Username, cluster.ID, "group_create", group, gin.H{"note": "Kafka Consumer Group 已在 KafkaVista 登记；消费者提交 offset 后会写入 Kafka 元数据"})
		c.JSON(http.StatusOK, gin.H{"code": 0, "message": "Consumer Group 已创建"})
	})
}
func (a *API) deleteGroup(c *gin.Context) {
	a.withClusterPerm(c, "group_delete", func(cluster model.KafkaCluster) {
		group := c.Param("group")
		var placeholderCount int64
		a.db.Model(&model.KafkaGroupPlaceholder{}).Where("cluster_id = ? AND group_id = ?", cluster.ID, group).Count(&placeholderCount)
		if err := a.kafka.DeleteGroup(cluster, group); err != nil {
			if placeholderCount == 0 || !errors.Is(err, sarama.ErrGroupIDNotFound) {
				kafkaError(c, err)
				return
			}
		}
		a.db.Where("cluster_id = ? AND group_id = ?", cluster.ID, group).Delete(&model.KafkaGroupPlaceholder{})
		a.audit(current(c).Username, cluster.ID, "group_delete", group, gin.H{})
		c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success"})
	})
}
func (a *API) deleteGroupTopic(c *gin.Context) {
	a.withClusterPerm(c, "group_delete", func(cluster model.KafkaCluster) {
		group := c.Param("group")
		topic := c.Param("topic")
		deleted, err := a.kafka.DeleteGroupTopicOffsets(cluster, group, topic)
		if err != nil {
			kafkaError(c, err)
			return
		}
		a.audit(current(c).Username, cluster.ID, "group_topic_unsubscribe", group, gin.H{"topic": topic, "partitions": deleted})
		c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success", "data": gin.H{"deleted_partitions": deleted}})
	})
}
func (a *API) groupHistory(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"code": 0, "data": gin.H{"items": []interface{}{}}})
}

func (a *API) listPermissions(c *gin.Context) {
	a.withClusterPerm(c, "permission_manage", func(cluster model.KafkaCluster) {
		var rows []model.KafkaPermission
		a.db.Where("cluster_id = ?", cluster.ID).Order("username asc").Find(&rows)
		var roleRows []model.KafkaRolePermission
		a.db.Where("cluster_id = ?", cluster.ID).Order("role asc").Find(&roleRows)
		users := map[string][]string{}
		for _, row := range rows {
			users[row.Username] = append(users[row.Username], row.Action)
		}
		items := []gin.H{}
		for username, actions := range users {
			items = append(items, gin.H{"subject_type": "user", "username": username, "subject": username, "actions": actions})
		}
		roles := map[string][]string{}
		for _, row := range roleRows {
			roles[row.Role] = append(roles[row.Role], row.Action)
		}
		for role, actions := range roles {
			items = append(items, gin.H{"subject_type": "role", "role": role, "subject": role, "actions": actions})
		}
		c.JSON(http.StatusOK, gin.H{"code": 0, "data": gin.H{"items": items}})
	})
}

func (a *API) savePermissions(c *gin.Context) {
	a.withClusterPerm(c, "permission_manage", func(cluster model.KafkaCluster) {
		var req struct {
			SubjectType string   `json:"subject_type"`
			Username    string   `json:"username"`
			Role        string   `json:"role"`
			Actions     []string `json:"actions"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			errorJSON(c, http.StatusBadRequest, "请求格式错误")
			return
		}
		if req.SubjectType == "" {
			req.SubjectType = "user"
		}
		target := req.Username
		if req.SubjectType == "role" {
			target = req.Role
		}
		if target == "" {
			errorJSON(c, http.StatusBadRequest, "授权对象不能为空")
			return
		}
		if req.SubjectType == "role" {
			a.db.Where("cluster_id = ? AND role = ?", cluster.ID, target).Delete(&model.KafkaRolePermission{})
		} else {
			a.db.Where("cluster_id = ? AND username = ?", cluster.ID, target).Delete(&model.KafkaPermission{})
		}
		for _, action := range req.Actions {
			if contains(model.AllActions, action) {
				if req.SubjectType == "role" {
					a.db.Create(&model.KafkaRolePermission{ClusterID: cluster.ID, Role: target, Action: action, CreatedBy: current(c).Username})
				} else {
					a.db.Create(&model.KafkaPermission{ClusterID: cluster.ID, Username: target, Action: action, CreatedBy: current(c).Username})
				}
			}
		}
		a.audit(current(c).Username, cluster.ID, "permission_update", target, req)
		c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success"})
	})
}

func (a *API) auditLogs(c *gin.Context) {
	var rows []model.KafkaAuditLog
	query := a.db.Model(&model.KafkaAuditLog{})
	limited := false
	if !a.enterpriseEnabled() {
		limited = true
		query = query.Where("created_at >= ?", time.Now().Add(-24*time.Hour))
	}
	if startTime := parseTimeQuery(c.Query("start_time")); !startTime.IsZero() {
		query = query.Where("created_at >= ?", startTime)
	}
	if endTime := parseTimeQuery(c.Query("end_time")); !endTime.IsZero() {
		query = query.Where("created_at <= ?", endTime)
	}
	if clusterID := c.Query("cluster_id"); clusterID != "" {
		query = query.Where("cluster_id = ?", clusterID)
	}
	if action := strings.TrimSpace(c.Query("action")); action != "" {
		query = query.Where("action = ?", action)
	}
	if username := strings.TrimSpace(c.Query("username")); username != "" {
		query = query.Where("username = ?", username)
	}
	var total int64
	query.Count(&total)
	page := intQuery(c, "page", 1)
	if page < 1 {
		page = 1
	}
	pageSize := intQuery(c, "page_size", intQuery(c, "limit", 100))
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 500 {
		pageSize = 500
	}
	query.Order("created_at desc").Limit(pageSize).Offset((page - 1) * pageSize).Find(&rows)
	c.JSON(http.StatusOK, gin.H{"code": 0, "data": gin.H{"items": rows, "limited": limited, "page": page, "page_size": pageSize, "total": total}})
}

func parseTimeQuery(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04", "2006-01-02 15:04:05", "2006-01-02"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed
		}
	}
	return time.Time{}
}

func (a *API) startAuditRetentionScheduler() {
	a.cleanupAuditLogs()
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for range ticker.C {
		a.cleanupAuditLogs()
	}
}

func (a *API) cleanupAuditLogs() {
	if a.enterpriseEnabled() {
		return
	}
	a.db.Where("created_at < ?", time.Now().Add(-24*time.Hour)).Delete(&model.KafkaAuditLog{})
}

func (a *API) auditMutations() gin.HandlerFunc {
	return func(c *gin.Context) {
		method := c.Request.Method
		if method != http.MethodPost && method != http.MethodPut && method != http.MethodPatch && method != http.MethodDelete {
			c.Next()
			return
		}
		started := time.Now()
		c.Next()
		if c.Writer.Status() >= 400 {
			return
		}
		action, target := mutationAuditAction(c)
		if action == "" {
			return
		}
		u := current(c)
		clusterID := c.Param("cluster")
		detail := gin.H{"method": method, "path": c.FullPath(), "status": c.Writer.Status(), "latency_ms": time.Since(started).Milliseconds()}
		a.audit(u.Username, clusterID, action, target, detail)
	}
}

func mutationAuditAction(c *gin.Context) (string, string) {
	path := c.FullPath()
	method := c.Request.Method
	switch {
	case strings.HasSuffix(path, "/api/admin/settings") && method == http.MethodPut:
		return "settings_update", "系统设置"
	case strings.HasSuffix(path, "/api/admin/settings/notification/test"):
		return "notification_test", "通知渠道"
	case strings.HasSuffix(path, "/api/admin/settings/alerting/test"):
		return "alerting_test", "监控告警"
	case strings.HasSuffix(path, "/api/admin/settings/ldap/test"):
		return "ldap_test", "LDAP"
	case strings.HasSuffix(path, "/api/admin/settings/ldap/sync-users"):
		return "ldap_sync_users", "LDAP 用户"
	case strings.HasSuffix(path, "/api/admin/users") && method == http.MethodPost:
		return "user_create", "用户"
	case strings.HasSuffix(path, "/api/admin/users/:username") && method == http.MethodPut:
		return "user_update", c.Param("username")
	case strings.HasSuffix(path, "/api/admin/users/:username") && method == http.MethodDelete:
		return "user_delete", c.Param("username")
	case strings.HasSuffix(path, "/api/admin/roles") && method == http.MethodPost:
		return "role_create", "角色"
	case strings.HasSuffix(path, "/api/admin/roles/:role") && method == http.MethodDelete:
		return "role_delete", c.Param("role")
	case strings.Contains(path, "/clusters/:cluster/topics/:topic/configs/:config") && method == http.MethodDelete:
		return "topic_config_delete", c.Param("topic") + "/" + c.Param("config")
	case strings.Contains(path, "/clusters/:cluster/messages") && method == http.MethodDelete:
		return "message_delete", c.Param("cluster")
	}
	return "", ""
}

func (a *API) withClusterPerm(c *gin.Context, action string, fn func(model.KafkaCluster)) {
	cluster, ok := a.cluster(c)
	if !ok {
		return
	}
	if !a.hasPermission(current(c), cluster.ID, action) {
		errorJSON(c, http.StatusForbidden, "缺少 Kafka 权限: "+action)
		return
	}
	fn(cluster)
}

func (a *API) cluster(c *gin.Context) (model.KafkaCluster, bool) {
	var cluster model.KafkaCluster
	if err := a.db.Where("id = ? AND is_active = ?", c.Param("cluster"), true).First(&cluster).Error; err != nil {
		errorJSON(c, http.StatusNotFound, "Kafka 集群不存在")
		return cluster, false
	}
	return cluster, true
}

func (a *API) allowedActions(u user, clusterID string) []string {
	if u.Role == "admin" {
		return model.AllActions
	}
	var rows []model.KafkaPermission
	a.db.Where("cluster_id = ? AND username = ?", clusterID, u.Username).Find(&rows)
	var roleRows []model.KafkaRolePermission
	a.db.Where("cluster_id = ? AND role = ?", clusterID, u.Role).Find(&roleRows)
	seen := map[string]bool{}
	items := make([]string, 0, len(rows)+len(roleRows))
	for _, row := range rows {
		if !seen[row.Action] {
			seen[row.Action] = true
			items = append(items, row.Action)
		}
	}
	for _, row := range roleRows {
		if !seen[row.Action] {
			seen[row.Action] = true
			items = append(items, row.Action)
		}
	}
	return items
}

func (a *API) hasPermission(u user, clusterID, action string) bool {
	if u.Role == "admin" {
		return true
	}
	if contains(a.allowedActions(u, clusterID), action) {
		return true
	}
	return (action == "topic_create" || action == "topic_delete" || action == "topic_config_manage") && contains(a.allowedActions(u, clusterID), "topic_manage")
}

func (a *API) audit(username, clusterID, action, target string, detail interface{}) {
	b, _ := json.Marshal(detail)
	a.db.Create(&model.KafkaAuditLog{Username: username, ClusterID: clusterID, Action: action, Target: target, Detail: string(b)})
}

func result(c *gin.Context, data interface{}, err error) {
	if err != nil {
		kafkaError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "data": data})
}

func kafkaError(c *gin.Context, err error)                { errorJSON(c, http.StatusBadGateway, err.Error()) }
func errorJSON(c *gin.Context, status int, detail string) { c.JSON(status, gin.H{"detail": detail}) }

func clusterJSON(cluster model.KafkaCluster) gin.H {
	return gin.H{"id": cluster.ID, "name": cluster.Name, "cluster_type": cluster.ClusterType, "bootstrap_servers": cluster.BootstrapServers, "security_protocol": cluster.SecurityProtocol, "sasl_mechanism": cluster.SASLMechanism, "sasl_username": cluster.SASLUsername, "description": cluster.Description, "is_active": cluster.IsActive, "created_at": cluster.CreatedAt.Format(time.RFC3339), "updated_at": cluster.UpdatedAt.Format(time.RFC3339)}
}

func intQuery(c *gin.Context, key string, fallback int) int {
	value, err := strconv.Atoi(c.Query(key))
	if err != nil {
		return fallback
	}
	return value
}

func int64Ptr(raw string) *int64 {
	if raw == "" {
		return nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil
	}
	return &value
}

func int64QueryPtr(c *gin.Context, keys ...string) *int64 {
	for _, key := range keys {
		if value := int64Ptr(c.Query(key)); value != nil {
			return value
		}
	}
	return nil
}

func toInt(v interface{}) int {
	switch value := v.(type) {
	case int:
		return value
	case int8:
		return int(value)
	case int16:
		return int(value)
	case int32:
		return int(value)
	case int64:
		return int(value)
	case float32:
		return int(value)
	case float64:
		return int(value)
	default:
		return 0
	}
}

func toInt64(v interface{}) int64 {
	switch value := v.(type) {
	case int:
		return int64(value)
	case int8:
		return int64(value)
	case int16:
		return int64(value)
	case int32:
		return int64(value)
	case int64:
		return value
	case float32:
		return int64(value)
	case float64:
		return int64(value)
	default:
		return 0
	}
}

func contains(items []string, item string) bool {
	for _, value := range items {
		if value == item {
			return true
		}
	}
	return false
}
