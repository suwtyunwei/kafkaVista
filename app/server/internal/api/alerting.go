package api

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/smtp"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"kafkavista/server/internal/model"
)

var alertState = struct {
	sync.Mutex
	lastSent map[string]time.Time
}{lastSent: map[string]time.Time{}}

type alertEvent struct {
	Cluster   model.KafkaCluster
	Rule      MonitoringAlertRule
	Message   string
	Value     int64
	Threshold int64
}

func (a *API) startAlertScheduler() {
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	for {
		<-timer.C
		a.runAlertScan(false)
		alertState.Lock()
		cutoff := time.Now().Add(-2 * time.Hour)
		for k, t := range alertState.lastSent {
			if t.Before(cutoff) {
				delete(alertState.lastSent, k)
			}
		}
		alertState.Unlock()
		timer.Reset(60 * time.Second)
	}
}

func (a *API) testAlerting(c *gin.Context) {
	events, err := a.runAlertScan(true)
	if err != nil {
		errorJSON(c, http.StatusInternalServerError, err.Error())
		return
	}
	items := make([]gin.H, 0, len(events))
	for _, event := range events {
		items = append(items, gin.H{"cluster": event.Cluster.Name, "rule": event.Rule.Key, "value": event.Value, "threshold": event.Threshold, "message": event.Message})
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success", "data": gin.H{"triggered": len(events), "items": items}})
}

func (a *API) alertRuleValue(c *gin.Context) {
	var req struct {
		Rule MonitoringAlertRule `json:"rule"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Rule.Key) == "" {
		errorJSON(c, http.StatusBadRequest, "请求格式错误")
		return
	}
	if !supportedAlertRuleKey(req.Rule.Key) {
		errorJSON(c, http.StatusBadRequest, "该告警规则没有可用监控指标，已从规则列表移除")
		return
	}
	items, err := a.currentAlertRuleValues(req.Rule)
	if err != nil {
		errorJSON(c, http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success", "data": gin.H{"items": items}})
}

func (a *API) currentAlertRuleValues(rule MonitoringAlertRule) ([]gin.H, error) {
	if isGlobalAlertRule(rule.Key) {
		return []gin.H{a.migrationIncrementalAlertValue(rule)}, nil
	}
	settings := a.authSettings()
	var clusters []model.KafkaCluster
	if len(rule.ClusterIDs) == 0 {
		if err := a.db.Where("is_active = ?", true).Order("name asc").Find(&clusters).Error; err != nil {
			return nil, err
		}
	} else {
		ids := make([]string, 0, len(rule.ClusterIDs))
		for _, id := range rule.ClusterIDs {
			if trimmed := strings.TrimSpace(id); trimmed != "" {
				ids = append(ids, trimmed)
			}
		}
		if len(ids) == 0 {
			return []gin.H{}, nil
		}
		if err := a.db.Where("is_active = ? AND id IN ?", true, ids).Order("name asc").Find(&clusters).Error; err != nil {
			return nil, err
		}
	}
	items := make([]gin.H, 0, len(clusters))
	for _, cluster := range clusters {
		data, err := a.kafka.Metrics(cluster, settings.Monitoring.TopicLimit, settings.Monitoring.GroupLimit)
		if err != nil {
			value := int64(0)
			if rule.Key == "broker_offline" {
				items = append(items, gin.H{"cluster_id": cluster.ID, "cluster": cluster.Name, "key": rule.Key, "value": value, "unit": rule.Unit, "threshold": rule.Threshold, "direction": rule.Direction, "triggered": true, "error": err.Error()})
			} else {
				items = append(items, gin.H{"cluster_id": cluster.ID, "cluster": cluster.Name, "key": rule.Key, "value": nil, "unit": rule.Unit, "threshold": rule.Threshold, "direction": rule.Direction, "triggered": false, "error": err.Error()})
			}
			continue
		}
		items = append(items, alertRuleValueForCluster(cluster, data, rule))
	}
	return items, nil
}

func (a *API) runAlertScan(force bool) ([]alertEvent, error) {
	settings := a.authSettings()
	if !settings.Monitoring.Alerting.Enabled {
		return nil, nil
	}
	if len(settings.Monitoring.Alerting.Rules) == 0 {
		return nil, nil
	}
	var clusters []model.KafkaCluster
	if err := a.db.Where("is_active = ?", true).Order("name asc").Find(&clusters).Error; err != nil {
		return nil, err
	}
	var events []alertEvent
	if rule, ok := enabledGlobalAlertRule(settings.Monitoring.Alerting.Rules, "migration_incremental_sync_abnormal"); ok {
		events = append(events, a.evaluateMigrationIncrementalAlert(rule)...)
	}
	for _, cluster := range clusters {
		rules := enabledAlertRules(settings.Monitoring.Alerting.Rules, cluster.ID)
		if len(rules) == 0 {
			continue
		}
		data, err := a.kafka.Metrics(cluster, settings.Monitoring.TopicLimit, settings.Monitoring.GroupLimit)
		if err != nil {
			if rule, ok := rules["broker_offline"]; ok {
				events = append(events, alertEvent{Cluster: cluster, Rule: rule, Message: fmt.Sprintf("集群 %s 连接失败：%v", cluster.Name, err), Value: 1, Threshold: rule.Threshold})
			}
			continue
		}
		events = append(events, evaluateAlertRules(cluster, data, rules)...)
	}
	for _, event := range events {
		if !force && !alertReady(event, settings.Monitoring.Alerting.CooldownMinutes) {
			continue
		}
		if err := sendAlertEvent(settings, event); err != nil {
			return events, err
		}
		markAlertSent(event)
	}
	return events, nil
}

func enabledAlertRules(rules []MonitoringAlertRule, clusterID string) map[string]MonitoringAlertRule {
	items := map[string]MonitoringAlertRule{}
	for _, rule := range rules {
		if rule.Enabled && strings.TrimSpace(rule.Key) != "" && !isGlobalAlertRule(rule.Key) && ruleAppliesToCluster(rule, clusterID) {
			items[rule.Key] = rule
		}
	}
	return items
}

func enabledGlobalAlertRule(rules []MonitoringAlertRule, key string) (MonitoringAlertRule, bool) {
	for _, rule := range rules {
		if rule.Enabled && strings.TrimSpace(rule.Key) == key {
			return rule, true
		}
	}
	return MonitoringAlertRule{}, false
}

func isGlobalAlertRule(key string) bool {
	return key == "migration_incremental_sync_abnormal"
}

func ruleAppliesToCluster(rule MonitoringAlertRule, clusterID string) bool {
	if len(rule.ClusterIDs) == 0 {
		return true
	}
	for _, id := range rule.ClusterIDs {
		if strings.TrimSpace(id) == clusterID {
			return true
		}
	}
	return false
}

func evaluateAlertRules(cluster model.KafkaCluster, data map[string]interface{}, rules map[string]MonitoringAlertRule) []alertEvent {
	var events []alertEvent
	for _, rule := range rules {
		value, detail, available := currentAlertValue(data, rule.Key)
		if !available {
			continue
		}
		triggered := compareAlertValue(value, rule.Direction, rule.Threshold)
		if rule.Key == "broker_offline" {
			triggered = value < rule.Threshold
		}
		if triggered {
			events = append(events, alertEvent{Cluster: cluster, Rule: rule, Value: value, Threshold: rule.Threshold, Message: fmt.Sprintf("集群 %s 规则 %s 当前值 %d，阈值 %s %d。%s", cluster.Name, rule.Key, value, rule.Direction, rule.Threshold, detail)})
		}
	}
	return events
}

func alertRuleValueForCluster(cluster model.KafkaCluster, data map[string]interface{}, rule MonitoringAlertRule) gin.H {
	value, detail, available := currentAlertValue(data, rule.Key)
	triggered := false
	if available {
		if rule.Key == "broker_offline" {
			triggered = value < rule.Threshold
		} else {
			triggered = compareAlertValue(value, rule.Direction, rule.Threshold)
		}
	}
	return gin.H{"cluster_id": cluster.ID, "cluster": cluster.Name, "key": rule.Key, "value": value, "unit": rule.Unit, "threshold": rule.Threshold, "direction": rule.Direction, "triggered": triggered, "available": available, "detail": detail}
}

func currentAlertValue(data map[string]interface{}, key string) (int64, string, bool) {
	switch key {
	case "consumer_group_lag":
		return alertFieldValue(data, "total_lag", "total_lag")
	case "broker_offline":
		return alertFieldValue(data, "broker_count", "available_broker_count")
	case "broker_unavailable":
		return alertFieldValue(data, "unavailable_broker_count", "unavailable_broker_count")
	case "under_replicated_partition":
		return alertFieldValue(data, "under_replicated_partition_count", "under_replicated_partition_count")
	case "offline_partition":
		return alertFieldValue(data, "offline_partition_count", "offline_partition_count")
	case "topic_partition_count":
		return alertFieldValue(data, "topic_count", "topic_count")
	case "topic_log_size":
		if _, ok := data["max_topic_log_size"]; !ok {
			return 0, "missing_metric:max_topic_log_size", false
		}
		topicName := strings.TrimSpace(fmt.Sprint(data["max_topic_log_size_topic"]))
		if topicName == "" {
			topicName = "-"
		}
		return toInt64(data["max_topic_log_size"]), "topic=" + topicName, true
	case "consumer_member_zero":
		if _, ok := data["groups"]; !ok {
			return 0, "missing_metric:groups", false
		}
		var zeroGroups int64
		for _, group := range metricGroups(data["groups"]) {
			if toInt(group["member_count"]) == 0 {
				zeroGroups++
			}
		}
		return zeroGroups, "groups_without_members", true
	case "migration_incremental_sync_abnormal":
		return 0, "migration_jobs", false
	default:
		return 0, "no_current_value_source", false
	}
}

func alertFieldValue(data map[string]interface{}, field string, detail string) (int64, string, bool) {
	value, ok := data[field]
	if !ok {
		return 0, "missing_metric:" + field, false
	}
	return toInt64(value), detail, true
}

func (a *API) evaluateMigrationIncrementalAlert(rule MonitoringAlertRule) []alertEvent {
	value, detail := a.countAbnormalIncrementalMigrationsForRule(rule)
	if !compareAlertValue(value, rule.Direction, rule.Threshold) {
		return nil
	}
	cluster := model.KafkaCluster{ID: "global", Name: "全局"}
	message := fmt.Sprintf("平滑迁移增量同步异常任务数 %d，达到阈值 %d。%s", value, rule.Threshold, detail)
	return []alertEvent{{Cluster: cluster, Rule: rule, Value: value, Threshold: rule.Threshold, Message: message}}
}

func (a *API) migrationIncrementalAlertValue(rule MonitoringAlertRule) gin.H {
	value, detail := a.countAbnormalIncrementalMigrationsForRule(rule)
	triggered := compareAlertValue(value, rule.Direction, rule.Threshold)
	return gin.H{"cluster_id": "global", "cluster": "全局", "key": rule.Key, "value": value, "unit": rule.Unit, "threshold": rule.Threshold, "direction": rule.Direction, "triggered": triggered, "available": true, "detail": detail}
}

func (a *API) countAbnormalIncrementalMigrations() (int64, string) {
	return a.countAbnormalIncrementalMigrationsForJobs(nil)
}

func (a *API) countAbnormalIncrementalMigrationsForRule(rule MonitoringAlertRule) (int64, string) {
	ids := cleanStringIDs(rule.MigrationJobIDs)
	return a.countAbnormalIncrementalMigrationsForJobs(ids)
}

func (a *API) countAbnormalIncrementalMigrationsForJobs(jobIDs []string) (int64, string) {
	var rows []model.KafkaMigrationJob
	query := a.db.Where("status = ?", "failed")
	if len(jobIDs) > 0 {
		query = query.Where("id IN ?", jobIDs)
	}
	query.Order("updated_at desc").Find(&rows)
	var count int64
	ids := []string{}
	for _, row := range rows {
		options := map[string]interface{}{}
		_ = json.Unmarshal([]byte(row.OptionsJSON), &options)
		if enabled, _ := options["incremental_sync"].(bool); !enabled {
			continue
		}
		count++
		if len(ids) < 3 {
			ids = append(ids, row.ID)
		}
	}
	if len(ids) == 0 {
		return count, "abnormal_incremental_migrations=0"
	}
	return count, "job_ids=" + strings.Join(ids, ",")
}

func cleanStringIDs(values []string) []string {
	ids := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" && !seen[trimmed] {
			ids = append(ids, trimmed)
			seen[trimmed] = true
		}
	}
	return ids
}

func compareAlertValue(value int64, direction string, threshold int64) bool {
	switch strings.TrimSpace(direction) {
	case ">":
		return value > threshold
	case "<":
		return value < threshold
	case "<=":
		return value <= threshold
	default:
		return value >= threshold
	}
}

func alertReady(event alertEvent, cooldownMinutes int) bool {
	if cooldownMinutes <= 0 {
		cooldownMinutes = 10
	}
	key := alertKey(event)
	alertState.Lock()
	defer alertState.Unlock()
	last := alertState.lastSent[key]
	return last.IsZero() || time.Since(last) >= time.Duration(cooldownMinutes)*time.Minute
}

func markAlertSent(event alertEvent) {
	alertState.Lock()
	defer alertState.Unlock()
	alertState.lastSent[alertKey(event)] = time.Now()
}

func alertKey(event alertEvent) string {
	return event.Cluster.ID + ":" + event.Rule.Key
}

func sendAlertEvent(settings AuthSettings, event alertEvent) error {
	title := fmt.Sprintf("KafkaVista 告警：%s", event.Rule.Name)
	now := time.Now().Format("2006-01-02 15:04:05")
	unit := strings.TrimSpace(event.Rule.Unit)
	if unit == "" {
		unit = "value"
	}
	plain := fmt.Sprintf("%s\n\n级别：Warning\n集群：%s\n规则：%s\n当前值：%d %s\n阈值：%s %d %s\n时间：%s\n\n%s\n\n建议：请进入 KafkaVista 检查 Consumer Group、Broker 或 Topic 状态。", title, event.Cluster.Name, event.Rule.Key, event.Value, unit, event.Rule.Direction, event.Threshold, unit, now, event.Message)
	dingTalkMarkdown := fmt.Sprintf("### %s\n\n> **级别**：<font color=\"warning\">Warning</font>  \n> **集群**：%s  \n> **规则**：%s  \n> **当前值**：%d %s  \n> **阈值**：%s %d %s  \n> **时间**：%s  \n\n**告警详情**  \n%s  \n\n**处理建议**  \n请进入 KafkaVista 检查 Consumer Group、Broker 或 Topic 状态。", title, event.Cluster.Name, event.Rule.Key, event.Value, unit, event.Rule.Direction, event.Threshold, unit, now, event.Message)
	emailHTML := fmt.Sprintf(`<div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;background:#0f172a;color:#e5e7eb;padding:24px;border-radius:16px;max-width:680px"><div style="font-size:18px;font-weight:700;color:#fbbf24;margin-bottom:12px">%s</div><div style="background:#111827;border:1px solid #334155;border-radius:12px;padding:16px"><p><b>级别：</b><span style="color:#f59e0b">Warning</span></p><p><b>集群：</b>%s</p><p><b>规则：</b>%s</p><p><b>当前值：</b>%d %s</p><p><b>阈值：</b>%s %d %s</p><p><b>时间：</b>%s</p></div><p style="margin-top:16px;color:#cbd5e1">%s</p><p style="color:#93c5fd">建议：请进入 KafkaVista 检查 Consumer Group、Broker 或 Topic 状态。</p></div>`, title, event.Cluster.Name, event.Rule.Key, event.Value, unit, event.Rule.Direction, event.Threshold, unit, now, event.Message)
	webhooks := []struct {
		enabled bool
		url     string
		payload any
	}{
		{settings.Monitoring.Alerting.DingTalkEnabled, strings.TrimSpace(settings.Monitoring.Alerting.DingTalkWebhook), gin.H{"msgtype": "markdown", "markdown": gin.H{"title": title, "text": dingTalkMarkdown}}},
		{settings.Monitoring.Alerting.FeishuEnabled, strings.TrimSpace(settings.Monitoring.Alerting.FeishuWebhook), gin.H{"msg_type": "interactive", "card": gin.H{"config": gin.H{"wide_screen_mode": true}, "header": gin.H{"template": "orange", "title": gin.H{"tag": "plain_text", "content": title}}, "elements": []gin.H{{"tag": "div", "text": gin.H{"tag": "lark_md", "content": fmt.Sprintf("**级别：** Warning\n**集群：** %s\n**规则：** %s\n**当前值：** %d %s\n**阈值：** %s %d %s\n**时间：** %s", event.Cluster.Name, event.Rule.Key, event.Value, unit, event.Rule.Direction, event.Threshold, unit, now)}}, {"tag": "hr"}, {"tag": "div", "text": gin.H{"tag": "lark_md", "content": fmt.Sprintf("**告警详情**\n%s", event.Message)}}, {"tag": "note", "elements": []gin.H{{"tag": "plain_text", "content": "建议：请进入 KafkaVista 检查 Consumer Group、Broker 或 Topic 状态。"}}}}}}},
	}
	for _, webhook := range webhooks {
		if !webhook.enabled || webhook.url == "" {
			continue
		}
		if err := postNotificationWebhook(webhook.url, webhook.payload); err != nil {
			return err
		}
	}
	if settings.Monitoring.Alerting.EmailSMTPEnabled && emailSMTPConfigured(settings) {
		return sendEmailNotification(settings, title, plain, emailHTML)
	}
	if webhook := strings.TrimSpace(settings.Monitoring.Alerting.EmailWebhook); webhook != "" {
		return postNotificationWebhook(webhook, gin.H{"subject": title, "content": plain, "text": plain, "html": emailHTML})
	}
	return nil
}

func emailSMTPConfigured(settings AuthSettings) bool {
	alerting := settings.Monitoring.Alerting
	return strings.TrimSpace(alerting.EmailSMTPHost) != "" && strings.TrimSpace(alerting.EmailFrom) != "" && strings.TrimSpace(alerting.EmailTo) != ""
}

func sendEmailNotification(settings AuthSettings, subject, textBody, htmlBody string) error {
	alerting := settings.Monitoring.Alerting
	host := strings.TrimSpace(alerting.EmailSMTPHost)
	from := strings.TrimSpace(alerting.EmailFrom)
	to := splitEmailRecipients(alerting.EmailTo)
	if host == "" || from == "" || len(to) == 0 {
		return errors.New("请先完整配置 SMTP 服务器、发件人和收件人")
	}
	port := alerting.EmailSMTPPort
	if port <= 0 {
		port = 587
	}
	addr := net.JoinHostPort(host, fmt.Sprint(port))
	message := buildEmailMessage(from, to, subject, textBody, htmlBody)
	username := strings.TrimSpace(alerting.EmailSMTPUsername)
	var auth smtp.Auth
	if username != "" {
		auth = smtp.PlainAuth("", username, alerting.EmailSMTPPassword, host)
	}
	if alerting.EmailUseTLS {
		conn, err := tls.Dial("tcp", addr, &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12})
		if err != nil {
			return err
		}
		client, err := smtp.NewClient(conn, host)
		if err != nil {
			return err
		}
		defer client.Close()
		if auth != nil {
			if err := client.Auth(auth); err != nil {
				return err
			}
		}
		if err := client.Mail(from); err != nil {
			return err
		}
		for _, item := range to {
			if err := client.Rcpt(item); err != nil {
				return err
			}
		}
		writer, err := client.Data()
		if err != nil {
			return err
		}
		if _, err := writer.Write(message); err != nil {
			return err
		}
		if err := writer.Close(); err != nil {
			return err
		}
		return client.Quit()
	}
	return smtp.SendMail(addr, auth, from, to, message)
}

func splitEmailRecipients(value string) []string {
	parts := strings.FieldsFunc(value, func(r rune) bool { return r == ',' || r == ';' || r == '\n' || r == '\r' || r == '\t' || r == ' ' })
	items := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			items = append(items, part)
		}
	}
	return items
}

func buildEmailMessage(from string, to []string, subject, textBody, htmlBody string) []byte {
	boundary := fmt.Sprintf("kafkavista-%d", time.Now().UnixNano())
	if htmlBody == "" {
		return []byte(fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s", from, strings.Join(to, ", "), subject, textBody))
	}
	return []byte(fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: multipart/alternative; boundary=%q\r\n\r\n--%s\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s\r\n--%s\r\nContent-Type: text/html; charset=UTF-8\r\n\r\n%s\r\n--%s--", from, strings.Join(to, ", "), subject, boundary, boundary, textBody, boundary, htmlBody, boundary))
}
