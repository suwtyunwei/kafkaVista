package api

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-ldap/ldap/v3"
	"gorm.io/gorm"

	"kafkavista/server/internal/model"
	"kafkavista/server/internal/store"
)

const authSettingsKey = "auth_settings"
const installTimeKey = "install_time"
const ldapPageSize = 500

type AuthSettings struct {
	UI struct {
		Language     string `json:"language"`
		Theme        string `json:"theme"`
		PlatformName string `json:"platform_name"`
		LogoURL      string `json:"logo_url"`
	} `json:"ui"`
	License struct {
		Key       string          `json:"key,omitempty"`
		ActiveKey string          `json:"active_key"`
		Keys      []LicenseRecord `json:"keys"`
	} `json:"license"`
	RBAC struct {
		DefaultRole string   `json:"default_role"`
		AdminUsers  []string `json:"admin_users"`
	} `json:"rbac"`
	LDAP struct {
		Enabled         bool   `json:"enabled"`
		URL             string `json:"url"`
		BindDN          string `json:"bind_dn"`
		BindPassword    string `json:"bind_password"`
		BaseDN          string `json:"base_dn"`
		UserFilter      string `json:"user_filter"`
		DisplayNameAttr string `json:"display_name_attr"`
		EmailAttr       string `json:"email_attr"`
		StartTLS        bool   `json:"start_tls"`
	} `json:"ldap"`
	OIDC struct {
		Enabled       bool     `json:"enabled"`
		IssuerURL     string   `json:"issuer_url"`
		ClientID      string   `json:"client_id"`
		ClientSecret  string   `json:"client_secret"`
		RedirectURL   string   `json:"redirect_url"`
		Scopes        string   `json:"scopes"`
		UsernameClaim string   `json:"username_claim"`
		RoleClaim     string   `json:"role_claim"`
		AdminRoles    []string `json:"admin_roles"`
		ButtonText    string   `json:"button_text"`
	} `json:"oidc"`
	Monitoring struct {
		Enabled          bool `json:"enabled"`
		DashboardEnabled bool `json:"dashboard_enabled"`
		TopicLimit       int  `json:"topic_limit"`
		GroupLimit       int  `json:"group_limit"`
		Alerting         struct {
			Enabled                 bool                  `json:"enabled"`
			LagThreshold            int64                 `json:"lag_threshold"`
			BrokerResourceThreshold int                   `json:"broker_resource_threshold"`
			OfflineBrokerAlert      bool                  `json:"offline_broker_alert"`
			CooldownMinutes         int                   `json:"cooldown_minutes"`
			DingTalkEnabled         bool                  `json:"dingtalk_enabled"`
			DingTalkWebhook         string                `json:"dingtalk_webhook"`
			FeishuEnabled           bool                  `json:"feishu_enabled"`
			FeishuWebhook           string                `json:"feishu_webhook"`
			EmailWebhook            string                `json:"email_webhook"`
			EmailSMTPEnabled        bool                  `json:"email_smtp_enabled"`
			EmailSMTPHost           string                `json:"email_smtp_host"`
			EmailSMTPPort           int                   `json:"email_smtp_port"`
			EmailSMTPUsername       string                `json:"email_smtp_username"`
			EmailSMTPPassword       string                `json:"email_smtp_password"`
			EmailFrom               string                `json:"email_from"`
			EmailTo                 string                `json:"email_to"`
			EmailUseTLS             bool                  `json:"email_use_tls"`
			Rules                   []MonitoringAlertRule `json:"rules"`
		} `json:"alerting"`
	} `json:"monitoring"`
}

type MonitoringAlertRule struct {
	Key             string   `json:"key"`
	Name            string   `json:"name"`
	Enabled         bool     `json:"enabled"`
	Threshold       int64    `json:"threshold"`
	Unit            string   `json:"unit"`
	Direction       string   `json:"direction"`
	ClusterIDs      []string `json:"cluster_ids"`
	MigrationJobIDs []string `json:"migration_job_ids"`
}

type licensePayload struct {
	Edition   string `json:"edition"`
	ExpiresAt string `json:"expires_at"`
}

type LicenseRecord struct {
	Key       string `json:"key"`
	Edition   string `json:"edition"`
	ExpiresAt string `json:"expires_at"`
	CreatedAt string `json:"created_at"`
}

type oidcDiscovery struct {
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	UserInfoEndpoint      string `json:"userinfo_endpoint"`
}

func defaultAuthSettings() AuthSettings {
	var s AuthSettings
	s.RBAC.DefaultRole = "user"
	s.UI.Language = "zh-CN"
	s.UI.Theme = "dark"
	s.UI.PlatformName = "kafkaVista"
	s.UI.LogoURL = "/favicon.png?v=2026052102"
	s.RBAC.AdminUsers = []string{"admin"}
	s.LDAP.UserFilter = "(uid=%s)"
	s.LDAP.DisplayNameAttr = "cn"
	s.LDAP.EmailAttr = "mail"
	s.OIDC.Scopes = "openid profile email"
	s.OIDC.UsernameClaim = "preferred_username"
	s.OIDC.RoleClaim = "roles"
	s.OIDC.AdminRoles = []string{"admin", "kafkavista-admin"}
	s.OIDC.ButtonText = "使用 OIDC 登录"
	s.Monitoring.TopicLimit = 200
	s.Monitoring.GroupLimit = 200
	s.Monitoring.Alerting.LagThreshold = 10000
	s.Monitoring.Alerting.BrokerResourceThreshold = 85
	s.Monitoring.Alerting.OfflineBrokerAlert = true
	s.Monitoring.Alerting.CooldownMinutes = 10
	s.Monitoring.Alerting.DingTalkEnabled = true
	s.Monitoring.Alerting.FeishuEnabled = true
	s.Monitoring.Alerting.EmailSMTPEnabled = true
	s.Monitoring.Alerting.EmailSMTPPort = 587
	s.Monitoring.Alerting.Rules = defaultMonitoringAlertRules()
	return s
}

func defaultMonitoringAlertRules() []MonitoringAlertRule {
	return []MonitoringAlertRule{
		{Key: "consumer_group_lag", Name: "Consumer Group Lag", Enabled: true, Threshold: 10000, Unit: "messages", Direction: ">="},
		{Key: "broker_offline", Name: "Broker Offline", Enabled: true, Threshold: 1, Unit: "broker", Direction: ">="},
		{Key: "broker_unavailable", Name: "Broker Unavailable", Enabled: true, Threshold: 1, Unit: "broker", Direction: ">="},
		{Key: "under_replicated_partition", Name: "Under Replicated Partition", Enabled: true, Threshold: 1, Unit: "partition", Direction: ">="},
		{Key: "offline_partition", Name: "Offline Partition", Enabled: true, Threshold: 1, Unit: "partition", Direction: ">="},
		{Key: "topic_partition_count", Name: "Topic Partition Count", Enabled: false, Threshold: 2000, Unit: "partition", Direction: ">="},
		{Key: "topic_log_size", Name: "Topic Log Size", Enabled: false, Threshold: 107374182400, Unit: "bytes", Direction: ">="},
		{Key: "consumer_member_zero", Name: "Consumer Group No Members", Enabled: true, Threshold: 1, Unit: "group", Direction: ">="},
		{Key: "migration_incremental_sync_abnormal", Name: "Migration Incremental Sync Abnormal", Enabled: true, Threshold: 1, Unit: "job", Direction: ">="},
	}
}

func supportedAlertRuleKey(key string) bool {
	switch strings.TrimSpace(key) {
	case "consumer_group_lag", "broker_offline", "broker_unavailable", "under_replicated_partition", "offline_partition", "topic_partition_count", "topic_log_size", "consumer_member_zero", "migration_incremental_sync_abnormal":
		return true
	default:
		return false
	}
}

func (a *API) appStatus(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"code": 0, "data": a.licenseStatus()})
}

func (a *API) generateLicenseToken(c *gin.Context) {
	if a.cfg.LicenseAdminToken == "" {
		errorJSON(c, http.StatusForbidden, "LICENSE_ADMIN_TOKEN 未配置，授权 token 生成接口不可用")
		return
	}
	provided := c.GetHeader("X-License-Admin-Token")
	if provided == "" && strings.HasPrefix(c.GetHeader("Authorization"), "Bearer ") {
		provided = strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer ")
	}
	if provided != a.cfg.LicenseAdminToken {
		errorJSON(c, http.StatusUnauthorized, "授权管理 token 无效")
		return
	}

	var req struct {
		Days      int    `json:"days"`
		ExpiresAt string `json:"expires_at"`
	}
	_ = c.ShouldBindJSON(&req)
	expiresAt := time.Now().UTC().AddDate(1, 0, 0)
	if req.Days > 0 {
		expiresAt = time.Now().UTC().AddDate(0, 0, req.Days)
	}
	if req.ExpiresAt != "" {
		parsed, err := time.Parse(time.RFC3339, req.ExpiresAt)
		if err != nil {
			errorJSON(c, http.StatusBadRequest, "expires_at 必须是 RFC3339 时间，例如 2027-05-21T00:00:00Z")
			return
		}
		expiresAt = parsed.UTC()
	}
	if !expiresAt.After(time.Now()) {
		errorJSON(c, http.StatusBadRequest, "过期时间必须晚于当前时间")
		return
	}

	payload := licensePayload{Edition: "enterprise", ExpiresAt: expiresAt.Format(time.RFC3339)}
	raw, err := json.Marshal(payload)
	if err != nil {
		errorJSON(c, http.StatusInternalServerError, "生成授权 token 失败")
		return
	}
	token := "KV-ENTERPRISE-" + base64.RawURLEncoding.EncodeToString(raw)
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success", "data": gin.H{"token": token, "edition": "full", "expires_at": expiresAt.Format(time.RFC3339), "valid": true}})
}

func (a *API) inspectLicenseToken(c *gin.Context) {
	token := strings.TrimSpace(c.Query("token"))
	if token == "" {
		errorJSON(c, http.StatusBadRequest, "token 不能为空")
		return
	}
	payload, ok := parseLicenseKey(token)
	if !ok {
		errorJSON(c, http.StatusBadRequest, "token 格式无效")
		return
	}
	expiresAt, err := time.Parse(time.RFC3339, payload.ExpiresAt)
	if err != nil {
		errorJSON(c, http.StatusBadRequest, "token 过期时间无效")
		return
	}
	now := time.Now().UTC()
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success", "data": gin.H{"edition": "full", "raw_edition": payload.Edition, "expires_at": expiresAt.UTC().Format(time.RFC3339), "valid": strings.EqualFold(payload.Edition, "enterprise") && expiresAt.After(now), "remaining_seconds": int64(expiresAt.Sub(now).Seconds())}})
}

func (a *API) licenseStatus() gin.H {
	settings := a.authSettings()
	return gin.H{
		"edition":       "enterprise",
		"enterprise":    true,
		"license_valid": true,
		"expires_at":    "",
		"language":      settings.UI.Language,
		"platform_name": settings.UI.PlatformName,
		"logo_url":      settings.UI.LogoURL,
	}
}

func (a *API) enterpriseEnabled() bool {
	return true
}

func (a *API) installTime() time.Time {
	var row model.SystemSetting
	if err := a.db.First(&row, "key = ?", installTimeKey).Error; err == nil && row.Value != "" {
		if value, err := time.Parse(time.RFC3339, row.Value); err == nil {
			return value
		}
	}
	now := time.Now().UTC()
	_ = a.db.Save(&model.SystemSetting{Key: installTimeKey, Value: now.Format(time.RFC3339)}).Error
	return now
}

func parseLicenseKey(key string) (licensePayload, bool) {
	key = strings.TrimSpace(key)
	if key == "" {
		return licensePayload{}, false
	}
	key = strings.TrimPrefix(key, "KV-ENTERPRISE-")
	decoded, err := base64.RawURLEncoding.DecodeString(key)
	if err != nil {
		decoded, err = base64.StdEncoding.DecodeString(key)
	}
	if err != nil {
		decoded = []byte(key)
	}
	var payload licensePayload
	if err := json.Unmarshal(decoded, &payload); err != nil {
		return payload, false
	}
	return payload, true
}

func (a *API) authSettings() AuthSettings {
	settings := defaultAuthSettings()
	var row model.SystemSetting
	if err := a.db.First(&row, "key = ?", authSettingsKey).Error; err == nil && row.Value != "" {
		_ = json.Unmarshal([]byte(row.Value), &settings)
	}
	settings.Monitoring.Alerting.Rules = normalizeMonitoringAlertRules(settings.Monitoring.Alerting.Rules)
	settings.normalizeLicense()
	a.applyEnvAlertingDefaults(&settings)
	return settings
}

func normalizeMonitoringAlertRules(rules []MonitoringAlertRule) []MonitoringAlertRule {
	filtered := filterSupportedAlertRules(rules)
	seen := map[string]bool{}
	for _, rule := range filtered {
		seen[rule.Key] = true
	}
	for _, rule := range defaultMonitoringAlertRules() {
		if !seen[rule.Key] {
			filtered = append(filtered, rule)
		}
	}
	return filtered
}

func filterSupportedAlertRules(rules []MonitoringAlertRule) []MonitoringAlertRule {
	filtered := make([]MonitoringAlertRule, 0, len(rules))
	for _, rule := range rules {
		if supportedAlertRuleKey(rule.Key) {
			filtered = append(filtered, rule)
		}
	}
	return filtered
}

func (a *API) applyEnvAlertingDefaults(settings *AuthSettings) {
	alerting := &settings.Monitoring.Alerting
	if alerting.EmailSMTPHost == "" {
		alerting.EmailSMTPHost = a.cfg.EmailSMTPHost
	}
	if alerting.EmailSMTPPort <= 0 {
		alerting.EmailSMTPPort = a.cfg.EmailSMTPPort
	}
	if alerting.EmailSMTPUsername == "" {
		alerting.EmailSMTPUsername = a.cfg.EmailSMTPUsername
	}
	if alerting.EmailSMTPPassword == "" {
		alerting.EmailSMTPPassword = a.cfg.EmailSMTPPassword
	}
	if alerting.EmailFrom == "" {
		alerting.EmailFrom = a.cfg.EmailFrom
	}
	if alerting.EmailTo == "" {
		alerting.EmailTo = a.cfg.EmailTo
	}
	if !alerting.EmailUseTLS && a.cfg.EmailUseTLS {
		alerting.EmailUseTLS = true
	}
}

func (s *AuthSettings) normalizeLicense() {
	if s.License.Key != "" {
		if s.License.ActiveKey == "" {
			s.License.ActiveKey = s.License.Key
		}
		found := false
		for _, item := range s.License.Keys {
			if item.Key == s.License.Key {
				found = true
				break
			}
		}
		if !found {
			item := LicenseRecord{Key: s.License.Key, CreatedAt: time.Now().UTC().Format(time.RFC3339)}
			if payload, ok := parseLicenseKey(s.License.Key); ok {
				item.Edition = payload.Edition
				item.ExpiresAt = payload.ExpiresAt
			}
			s.License.Keys = append(s.License.Keys, item)
		}
	}
	s.License.Key = ""
}

func (a *API) saveAuthSettings(settings AuthSettings) error {
	settings.normalizeLicense()
	raw, err := json.Marshal(settings)
	if err != nil {
		return err
	}
	row := model.SystemSetting{Key: authSettingsKey, Value: string(raw)}
	return a.db.Save(&row).Error
}

func sanitizedSettings(settings AuthSettings) AuthSettings {
	settings.LDAP.BindPassword = ""
	settings.OIDC.ClientSecret = ""
	settings.Monitoring.Alerting.EmailSMTPPassword = ""
	return settings
}

func (a *API) ssoStatus(c *gin.Context) {
	settings := a.authSettings()
	c.JSON(http.StatusOK, gin.H{"code": 0, "data": gin.H{"ldap_enabled": settings.LDAP.Enabled && !settings.OIDC.Enabled, "oidc_enabled": settings.OIDC.Enabled && a.enterpriseEnabled(), "oidc_button_text": settings.OIDC.ButtonText, "platform_name": settings.UI.PlatformName, "logo_url": settings.UI.LogoURL, "enterprise": a.licenseStatus()}})
}

func (a *API) getSettings(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"code": 0, "data": gin.H{"settings": sanitizedSettings(a.authSettings()), "license": a.licenseStatus()}})
}

func (a *API) saveSettings(c *gin.Context) {
	current := a.authSettings()
	var req AuthSettings
	if err := c.ShouldBindJSON(&req); err != nil {
		errorJSON(c, http.StatusBadRequest, "请求格式错误")
		return
	}
	if req.OIDC.Enabled && req.LDAP.Enabled {
		errorJSON(c, http.StatusBadRequest, "OIDC 和 LDAP 只能启用一个")
		return
	}
	if req.LDAP.BindPassword == "" {
		req.LDAP.BindPassword = current.LDAP.BindPassword
	}
	if req.OIDC.ClientSecret == "" {
		req.OIDC.ClientSecret = current.OIDC.ClientSecret
	}
	if req.Monitoring.Alerting.EmailSMTPPassword == "" {
		req.Monitoring.Alerting.EmailSMTPPassword = current.Monitoring.Alerting.EmailSMTPPassword
	}
	if strings.TrimSpace(req.UI.PlatformName) == "" {
		req.UI.PlatformName = "kafkaVista"
	}
	if strings.TrimSpace(req.UI.LogoURL) == "" {
		req.UI.LogoURL = "/favicon.png?v=2026052102"
	}
	if !a.enterpriseEnabled() {
		req.UI.PlatformName = current.UI.PlatformName
		req.UI.LogoURL = current.UI.LogoURL
	}
	if req.OIDC.Enabled {
		req.LDAP.Enabled = false
	}
	if req.RBAC.DefaultRole == "" {
		req.RBAC.DefaultRole = "user"
	}
	if req.UI.Language == "" {
		req.UI.Language = "zh-CN"
	}
	if req.UI.Theme == "" {
		req.UI.Theme = "dark"
	}
	if req.LDAP.UserFilter == "" {
		req.LDAP.UserFilter = "(uid=%s)"
	}
	if req.OIDC.Scopes == "" {
		req.OIDC.Scopes = "openid profile email"
	}
	if req.OIDC.UsernameClaim == "" {
		req.OIDC.UsernameClaim = "preferred_username"
	}
	if strings.TrimSpace(req.OIDC.ButtonText) == "" {
		req.OIDC.ButtonText = "使用 OIDC 登录"
	}
	if req.Monitoring.TopicLimit <= 0 {
		req.Monitoring.TopicLimit = 200
	}
	if req.Monitoring.GroupLimit <= 0 {
		req.Monitoring.GroupLimit = 200
	}
	if req.Monitoring.Alerting.CooldownMinutes <= 0 {
		req.Monitoring.Alerting.CooldownMinutes = 10
	}
	if req.Monitoring.Alerting.EmailSMTPPort <= 0 {
		req.Monitoring.Alerting.EmailSMTPPort = 587
	}
	if len(req.Monitoring.Alerting.Rules) == 0 {
		req.Monitoring.Alerting.Rules = defaultMonitoringAlertRules()
	}
	if err := a.saveAuthSettings(req); err != nil {
		errorJSON(c, http.StatusInternalServerError, "保存设置失败")
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success", "data": sanitizedSettings(req)})
}

func (a *API) testNotification(c *gin.Context) {
	var req struct {
		Channel string `json:"channel"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		errorJSON(c, http.StatusBadRequest, "请求格式错误")
		return
	}
	settings := a.authSettings()
	if !settings.Monitoring.Alerting.Enabled {
		errorJSON(c, http.StatusBadRequest, "请先开启监控告警")
		return
	}
	channel := strings.ToLower(strings.TrimSpace(req.Channel))
	message := fmt.Sprintf("KafkaVista 监控告警测试消息\n时间：%s\n如果你收到这条消息，说明通知渠道配置成功。", time.Now().Format("2006-01-02 15:04:05"))
	var webhook string
	var payload any
	switch channel {
	case "dingtalk":
		if !settings.Monitoring.Alerting.DingTalkEnabled {
			errorJSON(c, http.StatusBadRequest, "钉钉通知未启用")
			return
		}
		webhook = strings.TrimSpace(settings.Monitoring.Alerting.DingTalkWebhook)
		payload = gin.H{"msgtype": "text", "text": gin.H{"content": message}}
	case "feishu":
		if !settings.Monitoring.Alerting.FeishuEnabled {
			errorJSON(c, http.StatusBadRequest, "飞书通知未启用")
			return
		}
		webhook = strings.TrimSpace(settings.Monitoring.Alerting.FeishuWebhook)
		payload = gin.H{"msg_type": "text", "content": gin.H{"text": message}}
	case "email":
		if !settings.Monitoring.Alerting.EmailSMTPEnabled {
			errorJSON(c, http.StatusBadRequest, "邮件 SMTP 通知未启用")
			return
		}
		if emailSMTPConfigured(settings) {
			if err := sendEmailNotification(settings, "KafkaVista 监控告警测试", message, ""); err != nil {
				errorJSON(c, http.StatusBadGateway, err.Error())
				return
			}
			c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success"})
			return
		}
		webhook = strings.TrimSpace(settings.Monitoring.Alerting.EmailWebhook)
		payload = gin.H{"subject": "KafkaVista 监控告警测试", "content": message, "text": message}
	default:
		errorJSON(c, http.StatusBadRequest, "不支持的通知渠道")
		return
	}
	if webhook == "" {
		errorJSON(c, http.StatusBadRequest, "请先填写该渠道的 Webhook")
		return
	}
	if err := postNotificationWebhook(webhook, payload); err != nil {
		errorJSON(c, http.StatusBadGateway, err.Error())
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success"})
}

func postNotificationWebhook(webhook string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, webhook, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Webhook 返回 %d：%s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func (a *API) ldapConnection(settings AuthSettings) (*ldap.Conn, error) {
	if settings.LDAP.URL == "" {
		return nil, errors.New("LDAP URL 不能为空")
	}
	conn, err := ldap.DialURL(settings.LDAP.URL)
	if err != nil {
		return nil, err
	}
	if settings.LDAP.StartTLS {
		if err := conn.StartTLS(&tls.Config{InsecureSkipVerify: true}); err != nil {
			conn.Close()
			return nil, err
		}
	}
	if settings.LDAP.BindDN != "" {
		if err := conn.Bind(settings.LDAP.BindDN, settings.LDAP.BindPassword); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return conn, nil
}

func ldapSearch(conn *ldap.Conn, baseDN, filter string, attrs []string, maxEntries int) ([]*ldap.Entry, error) {
	searchRequest := ldap.NewSearchRequest(baseDN, ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false, filter, compactLDAPAttrs(attrs), nil)
	pagingControl := ldap.NewControlPaging(uint32(ldapPageSize))
	entries := make([]*ldap.Entry, 0)

	for {
		searchRequest.Controls = []ldap.Control{pagingControl}
		res, err := conn.Search(searchRequest)
		if err != nil {
			return entries, err
		}
		entries = append(entries, res.Entries...)
		if maxEntries > 0 && len(entries) >= maxEntries {
			return entries[:maxEntries], nil
		}

		control := ldap.FindControl(res.Controls, ldap.ControlTypePaging)
		if control == nil {
			return entries, nil
		}
		pagingResult, ok := control.(*ldap.ControlPaging)
		if !ok || len(pagingResult.Cookie) == 0 {
			return entries, nil
		}
		pagingControl.SetCookie(pagingResult.Cookie)
	}
}

func compactLDAPAttrs(attrs []string) []string {
	seen := map[string]bool{}
	result := make([]string, 0, len(attrs))
	for _, attr := range attrs {
		attr = strings.TrimSpace(attr)
		if attr == "" || seen[attr] {
			continue
		}
		seen[attr] = true
		result = append(result, attr)
	}
	return result
}

func (a *API) testLDAP(c *gin.Context) {
	settings := a.authSettings()
	var req AuthSettings
	if err := c.ShouldBindJSON(&req); err == nil {
		if req.LDAP.BindPassword == "" {
			req.LDAP.BindPassword = settings.LDAP.BindPassword
		}
		settings.LDAP = req.LDAP
	}
	conn, err := a.ldapConnection(settings)
	if err != nil {
		errorJSON(c, http.StatusBadGateway, "LDAP 连接失败: "+err.Error())
		return
	}
	defer conn.Close()
	filter := settings.LDAP.UserFilter
	if filter == "" {
		filter = "(uid=%s)"
	}
	probeFilter := strings.Replace(filter, "%s", "*", 1)
	entries, err := ldapSearch(conn, settings.LDAP.BaseDN, probeFilter, []string{"dn", settings.LDAP.DisplayNameAttr, settings.LDAP.EmailAttr}, 5)
	if err != nil {
		errorJSON(c, http.StatusBadGateway, "LDAP 搜索失败: "+err.Error())
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success", "data": gin.H{"matched": len(entries)}})
}

func (a *API) syncLDAPUsers(c *gin.Context) {
	settings := a.authSettings()
	if !settings.LDAP.Enabled {
		errorJSON(c, http.StatusBadRequest, "LDAP 未启用")
		return
	}
	conn, err := a.ldapConnection(settings)
	if err != nil {
		errorJSON(c, http.StatusBadGateway, "LDAP 连接失败: "+err.Error())
		return
	}
	defer conn.Close()
	filter := settings.LDAP.UserFilter
	if filter == "" {
		filter = "(uid=%s)"
	}
	searchFilter := strings.Replace(filter, "%s", "*", 1)
	attrs := []string{"uid", "cn", "mail", "sAMAccountName", settings.LDAP.DisplayNameAttr, settings.LDAP.EmailAttr}
	entries, err := ldapSearch(conn, settings.LDAP.BaseDN, searchFilter, attrs, 0)
	if err != nil {
		errorJSON(c, http.StatusBadGateway, "LDAP 搜索失败: "+err.Error())
		return
	}
	createdOrUpdated := 0
	for _, entry := range entries {
		username := entry.GetAttributeValue("uid")
		if username == "" {
			username = entry.GetAttributeValue("sAMAccountName")
		}
		if username == "" {
			username = entry.GetAttributeValue("cn")
		}
		if username == "" {
			continue
		}
		displayName := entry.GetAttributeValue(settings.LDAP.DisplayNameAttr)
		if displayName == "" {
			displayName = entry.GetAttributeValue("cn")
		}
		email := entry.GetAttributeValue(settings.LDAP.EmailAttr)
		if email == "" {
			email = entry.GetAttributeValue("mail")
		}
		a.upsertUser(username, displayName, email, a.roleFor(username, nil), "ldap")
		createdOrUpdated++
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success", "data": gin.H{"synced": createdOrUpdated, "matched": len(entries)}})
}

func (a *API) listUsers(c *gin.Context) {
	var users []model.AppUser
	a.db.Order("updated_at desc").Find(&users)
	c.JSON(http.StatusOK, gin.H{"code": 0, "data": gin.H{"items": users}})
}

func (a *API) createUser(c *gin.Context) {
	var req struct {
		Username    string `json:"username"`
		DisplayName string `json:"display_name"`
		Email       string `json:"email"`
		Role        string `json:"role"`
		Password    string `json:"password"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Username) == "" {
		errorJSON(c, http.StatusBadRequest, "用户名不能为空")
		return
	}
	req.Username = strings.TrimSpace(req.Username)
	if req.Password == "" {
		errorJSON(c, http.StatusBadRequest, "密码不能为空")
		return
	}
	var exists model.AppUser
	if err := a.db.Where("username = ?", req.Username).First(&exists).Error; err == nil {
		errorJSON(c, http.StatusBadRequest, "用户已存在")
		return
	}
	if req.Role == "" {
		req.Role = a.authSettings().RBAC.DefaultRole
	}
	var roleCount int64
	a.db.Model(&model.AppRole{}).Where("name = ?", req.Role).Count(&roleCount)
	if req.Role != "" && roleCount == 0 {
		errorJSON(c, http.StatusBadRequest, "角色不存在")
		return
	}
	user := model.AppUser{Username: req.Username, DisplayName: req.DisplayName, Email: req.Email, Role: req.Role, Source: "local", IsActive: true}
	if hash, err := store.HashPassword(req.Password); err == nil {
		user.PasswordHash = hash
	} else {
		errorJSON(c, http.StatusInternalServerError, "创建用户失败")
		return
	}
	if err := a.db.Create(&user).Error; err != nil {
		errorJSON(c, http.StatusBadRequest, "用户创建失败")
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success", "data": user})
}

func (a *API) listRoles(c *gin.Context) {
	var roles []model.AppRole
	a.db.Order("name asc").Find(&roles)
	c.JSON(http.StatusOK, gin.H{"code": 0, "data": gin.H{"items": roles}})
}

func (a *API) createRole(c *gin.Context) {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Name) == "" {
		errorJSON(c, http.StatusBadRequest, "角色名称不能为空")
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "admin" || req.Name == "user" {
		errorJSON(c, http.StatusBadRequest, "内置角色已存在")
		return
	}
	role := model.AppRole{Name: req.Name, Description: req.Description}
	if err := a.db.Create(&role).Error; err != nil {
		errorJSON(c, http.StatusBadRequest, "角色已存在")
		return
	}
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success", "data": role})
}

func (a *API) deleteRole(c *gin.Context) {
	role := c.Param("role")
	if role == "admin" || role == "user" {
		errorJSON(c, http.StatusBadRequest, "内置角色不能删除")
		return
	}
	a.db.Model(&model.AppUser{}).Where("role = ?", role).Update("role", "user")
	a.db.Where("role = ?", role).Delete(&model.KafkaRolePermission{})
	a.db.Where("name = ?", role).Delete(&model.AppRole{})
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success"})
}

func (a *API) updateUser(c *gin.Context) {
	username := c.Param("username")
	var req struct {
		Role        string `json:"role"`
		IsActive    *bool  `json:"is_active"`
		Password    string `json:"password"`
		DisplayName string `json:"display_name"`
		Email       string `json:"email"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		errorJSON(c, http.StatusBadRequest, "请求格式错误")
		return
	}
	var user model.AppUser
	if err := a.db.Where("username = ?", username).First(&user).Error; err != nil {
		errorJSON(c, http.StatusNotFound, "用户不存在")
		return
	}
	if req.DisplayName != "" {
		user.DisplayName = req.DisplayName
	}
	if req.Email != "" {
		user.Email = req.Email
	}
	var roleCount int64
	a.db.Model(&model.AppRole{}).Where("name = ?", req.Role).Count(&roleCount)
	if req.Role != "" && roleCount > 0 {
		user.Role = req.Role
	} else if req.Role != "" {
		errorJSON(c, http.StatusBadRequest, "角色不存在")
		return
	}
	if req.IsActive != nil {
		user.IsActive = *req.IsActive
	}
	if req.Password != "" {
		if hash, err := store.HashPassword(req.Password); err == nil {
			user.PasswordHash = hash
		} else {
			errorJSON(c, http.StatusBadRequest, "密码更新失败")
			return
		}
	}
	a.db.Save(&user)
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success", "data": user})
}

func (a *API) deleteUser(c *gin.Context) {
	username := c.Param("username")
	if username == a.cfg.DefaultUser {
		errorJSON(c, http.StatusBadRequest, "默认管理员不能删除")
		return
	}
	if username == current(c).Username {
		errorJSON(c, http.StatusBadRequest, "不能删除当前登录用户")
		return
	}
	a.db.Where("username = ?", username).Delete(&model.KafkaPermission{})
	a.db.Where("username = ?", username).Delete(&model.AppUser{})
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success"})
}

func (a *API) upsertUser(username, displayName, email, role, source string) model.AppUser {
	now := time.Now()
	if role == "" {
		role = a.authSettings().RBAC.DefaultRole
	}
	if displayName == "" {
		displayName = username
	}
	var user model.AppUser
	err := a.db.Where("username = ?", username).First(&user).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		user = model.AppUser{Username: username, DisplayName: displayName, Email: email, Role: role, Source: source, IsActive: true, LastLoginAt: &now}
		a.db.Create(&user)
		return user
	}
	if displayName != "" {
		user.DisplayName = displayName
	}
	if email != "" {
		user.Email = email
	}
	if user.Role == "" {
		user.Role = role
	}
	user.Source = source
	user.LastLoginAt = &now
	if !user.IsActive {
		return user
	}
	a.db.Save(&user)
	return user
}

func (a *API) roleFor(username string, claims map[string]interface{}) string {
	settings := a.authSettings()
	if contains(settings.RBAC.AdminUsers, username) {
		return "admin"
	}
	roleClaim := settings.OIDC.RoleClaim
	if roleClaim != "" {
		if hasAnyRole(claims[roleClaim], settings.OIDC.AdminRoles) {
			return "admin"
		}
		if realm, ok := claims["realm_access"].(map[string]interface{}); ok && hasAnyRole(realm["roles"], settings.OIDC.AdminRoles) {
			return "admin"
		}
	}
	if settings.RBAC.DefaultRole != "" {
		return settings.RBAC.DefaultRole
	}
	return "user"
}

func hasAnyRole(value interface{}, wanted []string) bool {
	want := map[string]bool{}
	for _, item := range wanted {
		want[item] = true
	}
	switch v := value.(type) {
	case []interface{}:
		for _, item := range v {
			if want[fmt.Sprint(item)] {
				return true
			}
		}
	case []string:
		for _, item := range v {
			if want[item] {
				return true
			}
		}
	case string:
		return want[v]
	}
	return false
}

func (a *API) authenticateLDAP(username, password string) (model.AppUser, error) {
	settings := a.authSettings()
	if !settings.LDAP.Enabled {
		return model.AppUser{}, errors.New("LDAP 未启用")
	}
	if username == "" || password == "" {
		return model.AppUser{}, errors.New("用户名或密码不能为空")
	}
	conn, err := a.ldapConnection(settings)
	if err != nil {
		return model.AppUser{}, errors.New("LDAP 连接失败")
	}
	defer conn.Close()
	filter := fmt.Sprintf(settings.LDAP.UserFilter, ldap.EscapeFilter(username))
	attrs := []string{"dn", settings.LDAP.DisplayNameAttr, settings.LDAP.EmailAttr}
	res, err := conn.Search(ldap.NewSearchRequest(settings.LDAP.BaseDN, ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 1, 0, false, filter, attrs, nil))
	if err != nil || len(res.Entries) == 0 {
		return model.AppUser{}, errors.New("LDAP 用户不存在")
	}
	entry := res.Entries[0]
	if err := conn.Bind(entry.DN, password); err != nil {
		return model.AppUser{}, errors.New("LDAP 用户名或密码错误")
	}
	role := a.roleFor(username, nil)
	return a.upsertUser(username, entry.GetAttributeValue(settings.LDAP.DisplayNameAttr), entry.GetAttributeValue(settings.LDAP.EmailAttr), role, "ldap"), nil
}

func (a *API) oidcLogin(c *gin.Context) {
	settings := a.authSettings()
	if !a.enterpriseEnabled() {
		errorJSON(c, http.StatusForbidden, "完整版密钥已过期，OIDC 不可用")
		return
	}
	if !settings.OIDC.Enabled {
		errorJSON(c, http.StatusBadRequest, "OIDC 未启用")
		return
	}
	discovery, err := discoverOIDC(settings.OIDC.IssuerURL)
	if err != nil {
		errorJSON(c, http.StatusBadGateway, "OIDC discovery 失败")
		return
	}
	state := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))
	q := url.Values{}
	q.Set("client_id", settings.OIDC.ClientID)
	q.Set("redirect_uri", settings.OIDC.RedirectURL)
	q.Set("response_type", "code")
	q.Set("scope", settings.OIDC.Scopes)
	q.Set("state", state)
	c.Redirect(http.StatusFound, discovery.AuthorizationEndpoint+"?"+q.Encode())
}

func (a *API) oidcCallback(c *gin.Context) {
	code := c.Query("code")
	if code == "" {
		errorJSON(c, http.StatusBadRequest, "缺少 OIDC code")
		return
	}
	settings := a.authSettings()
	if !a.enterpriseEnabled() {
		errorJSON(c, http.StatusForbidden, "完整版密钥已过期，OIDC 不可用")
		return
	}
	discovery, err := discoverOIDC(settings.OIDC.IssuerURL)
	if err != nil {
		errorJSON(c, http.StatusBadGateway, "OIDC discovery 失败")
		return
	}
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", settings.OIDC.RedirectURL)
	form.Set("client_id", settings.OIDC.ClientID)
	form.Set("client_secret", settings.OIDC.ClientSecret)
	tokenResp, err := http.PostForm(discovery.TokenEndpoint, form)
	if err != nil || tokenResp.StatusCode >= 300 {
		errorJSON(c, http.StatusBadGateway, "OIDC token 交换失败")
		return
	}
	defer tokenResp.Body.Close()
	var tokenData map[string]interface{}
	_ = json.NewDecoder(tokenResp.Body).Decode(&tokenData)
	accessToken, _ := tokenData["access_token"].(string)
	userinfo, err := fetchUserInfo(discovery.UserInfoEndpoint, accessToken)
	if err != nil {
		errorJSON(c, http.StatusBadGateway, "OIDC userinfo 获取失败")
		return
	}
	username := oidcUsername(userinfo, settings.OIDC.UsernameClaim)
	if username == "" {
		errorJSON(c, http.StatusBadGateway, "OIDC 用户信息缺少用户名 Claim")
		return
	}
	display := claimString(userinfo, "name")
	if display == "" {
		display = username
	}
	email := claimString(userinfo, "email")
	role := a.roleFor(username, userinfo)
	appUser := a.upsertUser(username, display, email, role, "oidc")
	if !appUser.IsActive {
		errorJSON(c, http.StatusForbidden, "用户已禁用")
		return
	}
	jwtText, err := a.issueToken(username, appUser.Role)
	if err != nil {
		errorJSON(c, http.StatusInternalServerError, "生成 Token 失败")
		return
	}
	userPayload, _ := json.Marshal(gin.H{"username": appUser.Username, "display_name": appUser.DisplayName, "role": appUser.Role, "source": appUser.Source})
	redirect := "/login?sso_token=" + url.QueryEscape(jwtText) + "&sso_user=" + url.QueryEscape(base64.RawURLEncoding.EncodeToString(userPayload))
	c.Redirect(http.StatusFound, redirect)
}

func oidcUsername(claims map[string]interface{}, configuredClaim string) string {
	for _, key := range []string{configuredClaim, "preferred_username", "email", "sub"} {
		value := strings.TrimSpace(claimString(claims, key))
		if value != "" {
			return value
		}
	}
	return ""
}

func discoverOIDC(issuer string) (oidcDiscovery, error) {
	var d oidcDiscovery
	if issuer == "" {
		return d, errors.New("issuer is empty")
	}
	endpoint := strings.TrimRight(issuer, "/") + "/.well-known/openid-configuration"
	resp, err := http.Get(endpoint)
	if err != nil {
		return d, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return d, fmt.Errorf("status %d", resp.StatusCode)
	}
	return d, json.NewDecoder(resp.Body).Decode(&d)
}

func fetchUserInfo(endpoint, token string) (map[string]interface{}, error) {
	req, _ := http.NewRequest(http.MethodGet, endpoint, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("userinfo status %d: %s", resp.StatusCode, string(bytes.TrimSpace(body)))
	}
	var data map[string]interface{}
	return data, json.NewDecoder(resp.Body).Decode(&data)
}

func claimString(claims map[string]interface{}, key string) string {
	if claims == nil || key == "" {
		return ""
	}
	if value, ok := claims[key]; ok {
		return fmt.Sprint(value)
	}
	return ""
}
