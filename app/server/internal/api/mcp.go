package api

import (
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"kafkavista/server/internal/model"
)

const mcpSettingsKey = "mcp_settings"
const mcpLatestProtocolVersion = "2025-06-18"

// MCPSettings controls the public MCP endpoint. The token is a long-lived
// bearer credential intended for external Agent integrations.
type MCPSettings struct {
	Enabled   bool   `json:"enabled"`
	Token     string `json:"token"`
	Username  string `json:"username"`
	UpdatedAt string `json:"updated_at"`
}

var mcpSettingsMu sync.Mutex

func (a *API) loadMCPSettings() MCPSettings {
	mcpSettingsMu.Lock()
	defer mcpSettingsMu.Unlock()
	settings := MCPSettings{Username: "agent"}
	var row model.SystemSetting
	if err := a.db.First(&row, "key = ?", mcpSettingsKey).Error; err == nil && row.Value != "" {
		_ = json.Unmarshal([]byte(row.Value), &settings)
	}
	if strings.TrimSpace(settings.Username) == "" {
		settings.Username = "agent"
	}
	if strings.TrimSpace(settings.Token) == "" {
		settings.Token = newMCPToken()
		settings.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		_ = a.saveMCPSettings(settings)
	}
	return settings
}

func (a *API) saveMCPSettings(settings MCPSettings) error {
	raw, err := json.Marshal(settings)
	if err != nil {
		return err
	}
	return a.db.Save(&model.SystemSetting{Key: mcpSettingsKey, Value: string(raw)}).Error
}

func newMCPToken() string {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "kvmcp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return "kvmcp_" + base64.RawURLEncoding.EncodeToString(buf)
}

func (a *API) getMCPConfig(c *gin.Context) {
	settings := a.loadMCPSettings()
	c.JSON(http.StatusOK, gin.H{"code": 0, "data": gin.H{
		"enabled":    settings.Enabled,
		"token":      settings.Token,
		"username":   settings.Username,
		"updated_at": settings.UpdatedAt,
		"endpoint":   "/api/mcp",
		"tools":      a.mcpToolSummaries(),
	}})
}

func (a *API) saveMCPConfig(c *gin.Context) {
	var req struct {
		Enabled  *bool  `json:"enabled"`
		Username string `json:"username"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		errorJSON(c, http.StatusBadRequest, "请求格式错误")
		return
	}
	settings := a.loadMCPSettings()
	if req.Enabled != nil {
		settings.Enabled = *req.Enabled
	}
	if strings.TrimSpace(req.Username) != "" {
		settings.Username = strings.TrimSpace(req.Username)
	}
	settings.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	if err := a.saveMCPSettings(settings); err != nil {
		errorJSON(c, http.StatusInternalServerError, "保存 MCP 配置失败")
		return
	}
	a.audit(current(c).Username, "", "mcp_settings_update", "MCP", gin.H{"enabled": settings.Enabled, "username": settings.Username})
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success", "data": gin.H{"enabled": settings.Enabled, "token": settings.Token, "username": settings.Username, "updated_at": settings.UpdatedAt, "endpoint": "/api/mcp", "tools": a.mcpToolSummaries()}})
}

func (a *API) rotateMCPToken(c *gin.Context) {
	settings := a.loadMCPSettings()
	settings.Token = newMCPToken()
	settings.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	if err := a.saveMCPSettings(settings); err != nil {
		errorJSON(c, http.StatusInternalServerError, "更新 MCP Token 失败")
		return
	}
	a.audit(current(c).Username, "", "mcp_token_rotate", "MCP", gin.H{})
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success", "data": gin.H{"enabled": settings.Enabled, "token": settings.Token, "username": settings.Username, "updated_at": settings.UpdatedAt, "endpoint": "/api/mcp", "tools": a.mcpToolSummaries()}})
}

type mcpClusterInput struct {
	Name             string  `json:"name"`
	ClusterType      string  `json:"cluster_type"`
	BootstrapServers string  `json:"bootstrap_servers"`
	SecurityProtocol string  `json:"security_protocol"`
	SASLMechanism    string  `json:"sasl_mechanism"`
	SASLUsername     string  `json:"sasl_username"`
	SASLPassword     *string `json:"sasl_password"`
	Description      *string `json:"description"`
}

func mcpClusterAudit(cluster model.KafkaCluster) gin.H {
	return gin.H{"name": cluster.Name, "cluster_type": cluster.ClusterType, "bootstrap_servers": cluster.BootstrapServers, "security_protocol": cluster.SecurityProtocol, "sasl_mechanism": cluster.SASLMechanism, "sasl_username": cluster.SASLUsername, "description": cluster.Description}
}

func (a *API) mcpCreateCluster(c *gin.Context) {
	var req mcpClusterInput
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Name) == "" || strings.TrimSpace(req.BootstrapServers) == "" {
		errorJSON(c, http.StatusBadRequest, "集群名称和地址不能为空")
		return
	}
	cluster := model.KafkaCluster{Name: strings.TrimSpace(req.Name), ClusterType: req.ClusterType, BootstrapServers: strings.TrimSpace(req.BootstrapServers), SecurityProtocol: req.SecurityProtocol, SASLMechanism: req.SASLMechanism, SASLUsername: req.SASLUsername, Description: stringValue(req.Description), IsActive: true}
	if req.SASLPassword != nil {
		cluster.SASLPassword = *req.SASLPassword
	}
	if cluster.ClusterType == "" {
		cluster.ClusterType = "cluster"
	}
	if cluster.SecurityProtocol == "" {
		cluster.SecurityProtocol = "PLAINTEXT"
	}
	if err := a.db.Create(&cluster).Error; err != nil {
		errorJSON(c, http.StatusBadRequest, "集群名称已存在")
		return
	}
	for _, action := range model.AllActions {
		a.db.Create(&model.KafkaPermission{ClusterID: cluster.ID, Username: current(c).Username, Action: action, CreatedBy: current(c).Username})
	}
	a.audit(current(c).Username, cluster.ID, "cluster_create", cluster.Name, mcpClusterAudit(cluster))
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success", "data": clusterJSON(cluster)})
}

func (a *API) mcpUpdateCluster(c *gin.Context) {
	var cluster model.KafkaCluster
	if err := a.db.Where("id = ? AND is_active = ?", c.Param("cluster"), true).First(&cluster).Error; err != nil {
		errorJSON(c, http.StatusNotFound, "Kafka 集群不存在")
		return
	}
	var req mcpClusterInput
	if err := c.ShouldBindJSON(&req); err != nil {
		errorJSON(c, http.StatusBadRequest, "请求格式错误")
		return
	}
	if strings.TrimSpace(req.Name) != "" {
		cluster.Name = strings.TrimSpace(req.Name)
	}
	if strings.TrimSpace(req.ClusterType) != "" {
		cluster.ClusterType = req.ClusterType
	}
	if strings.TrimSpace(req.BootstrapServers) != "" {
		cluster.BootstrapServers = strings.TrimSpace(req.BootstrapServers)
	}
	if strings.TrimSpace(req.SecurityProtocol) != "" {
		cluster.SecurityProtocol = req.SecurityProtocol
	}
	if strings.TrimSpace(req.SASLMechanism) != "" {
		cluster.SASLMechanism = req.SASLMechanism
	}
	if strings.TrimSpace(req.SASLUsername) != "" {
		cluster.SASLUsername = req.SASLUsername
	}
	if req.SASLPassword != nil {
		cluster.SASLPassword = *req.SASLPassword
	}
	if req.Description != nil {
		cluster.Description = *req.Description
	}
	if cluster.Name == "" || cluster.BootstrapServers == "" {
		errorJSON(c, http.StatusBadRequest, "集群名称和地址不能为空")
		return
	}
	if err := a.db.Save(&cluster).Error; err != nil {
		errorJSON(c, http.StatusBadRequest, "集群名称已存在")
		return
	}
	a.audit(current(c).Username, cluster.ID, "cluster_update", cluster.Name, mcpClusterAudit(cluster))
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "success", "data": clusterJSON(cluster)})
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func mcpRequestToken(c *gin.Context) string {
	if token := strings.TrimSpace(c.GetHeader("X-MCP-Token")); token != "" {
		return token
	}
	if token := strings.TrimSpace(c.GetHeader("X-API-Key")); token != "" {
		return strings.TrimPrefix(token, "Bearer ")
	}
	header := strings.TrimSpace(c.GetHeader("Authorization"))
	if strings.HasPrefix(strings.ToLower(header), "bearer ") {
		return strings.TrimSpace(header[7:])
	}
	if token := strings.TrimSpace(c.Query("token")); token != "" {
		return token
	}
	return strings.TrimSpace(c.Query("access_token"))
}

func (a *API) mcpAuthorized(c *gin.Context) (MCPSettings, bool) {
	settings := a.loadMCPSettings()
	if !settings.Enabled {
		c.JSON(http.StatusNotFound, gin.H{"error": "KafkaVista MCP service is disabled"})
		return settings, false
	}
	token := mcpRequestToken(c)
	if token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(settings.Token)) != 1 {
		c.Header("WWW-Authenticate", `Bearer realm="kafkavista-mcp"`)
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid MCP token"})
		return settings, false
	}
	return settings, true
}

type mcpRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type mcpRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type mcpRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  interface{}     `json:"result,omitempty"`
	Error   *mcpRPCError    `json:"error,omitempty"`
}

// mcpEmptyObject marshals to {} but is a struct, so encoding/json does not
// drop it through omitempty (which would make the JSON-RPC result invalid).
type mcpEmptyObject struct{}

func mcpIsNotification(id json.RawMessage) bool {
	return len(id) == 0 || strings.TrimSpace(string(id)) == "null"
}

func mcpResponse(id json.RawMessage, result interface{}) mcpRPCResponse {
	return mcpRPCResponse{JSONRPC: "2.0", ID: id, Result: result}
}

func mcpErrorResponse(id json.RawMessage, code int, message string) mcpRPCResponse {
	return mcpRPCResponse{JSONRPC: "2.0", ID: id, Error: &mcpRPCError{Code: code, Message: message}}
}

func (a *API) mcpNotAllowed(c *gin.Context) {
	c.Header("Allow", "POST, OPTIONS")
	c.JSON(http.StatusMethodNotAllowed, gin.H{"error": "KafkaVista MCP uses Streamable HTTP; send JSON-RPC requests with POST"})
}

func (a *API) mcpHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		settings, ok := a.mcpAuthorized(c)
		if !ok {
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 8<<20)
		raw, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid MCP request body"})
			return
		}
		raw = bytes.TrimSpace(raw)
		if len(raw) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "empty MCP request"})
			return
		}
		c.Header("MCP-Protocol-Version", mcpLatestProtocolVersion)
		if raw[0] == '[' {
			var batch []json.RawMessage
			if err := json.Unmarshal(raw, &batch); err != nil {
				c.JSON(http.StatusOK, mcpErrorResponse(nil, -32700, "parse error"))
				return
			}
			responses := make([]interface{}, 0, len(batch))
			for _, item := range batch {
				response, notification := a.mcpHandleRPC(settings, item)
				if !notification && response != nil {
					responses = append(responses, response)
				}
			}
			if len(responses) == 0 {
				c.Status(http.StatusAccepted)
				return
			}
			c.JSON(http.StatusOK, responses)
			return
		}
		response, notification := a.mcpHandleRPC(settings, raw)
		if notification || response == nil {
			c.Status(http.StatusAccepted)
			return
		}
		c.JSON(http.StatusOK, response)
	}
}

func (a *API) mcpHandleRPC(settings MCPSettings, raw json.RawMessage) (interface{}, bool) {
	var req mcpRPCRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return mcpErrorResponse(nil, -32700, "parse error"), false
	}
	notification := mcpIsNotification(req.ID)
	if req.Method == "" {
		return mcpErrorResponse(req.ID, -32600, "invalid request"), notification
	}
	response := a.mcpDispatch(settings, req)
	if notification {
		return nil, true
	}
	return response, false
}

func (a *API) mcpDispatch(settings MCPSettings, req mcpRPCRequest) mcpRPCResponse {
	switch req.Method {
	case "initialize":
		var params struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(req.Params, &params)
		version := mcpNegotiateProtocolVersion(params.ProtocolVersion)
		return mcpResponse(req.ID, gin.H{
			"protocolVersion": version,
			"capabilities": gin.H{
				"tools":   gin.H{"listChanged": false},
				"logging": gin.H{},
			},
			"serverInfo":   gin.H{"name": "kafkaVista", "version": "1.0.0"},
			"instructions": "KafkaVista MCP exposes the same Kafka management capabilities as the KafkaVista console: clusters, topics, messages, consumer groups, permissions, audit logs and migrations. Use kafka_list_clusters first to discover cluster ids and names.",
		})
	case "notifications/initialized", "notifications/cancelled", "notifications/roots/list_changed":
		return mcpRPCResponse{}
	case "ping":
		return mcpResponse(req.ID, mcpEmptyObject{})
	case "tools/list":
		tools := make([]gin.H, 0)
		for _, def := range a.mcpTools() {
			tools = append(tools, gin.H{"name": def.Name, "description": def.Description, "inputSchema": def.InputSchema})
		}
		return mcpResponse(req.ID, gin.H{"tools": tools})
	case "tools/call":
		var params struct {
			Name      string                 `json:"name"`
			Arguments map[string]interface{} `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return mcpErrorResponse(req.ID, -32602, "invalid tools/call params")
		}
		if params.Arguments == nil {
			params.Arguments = map[string]interface{}{}
		}
		def, ok := a.mcpToolByName(params.Name)
		if !ok {
			return mcpErrorResponse(req.ID, -32602, "unknown tool: "+params.Name)
		}
		call, err := def.Build(params.Arguments)
		if err != nil {
			return mcpResponse(req.ID, mcpToolErrorResult(err))
		}
		data, err := a.mcpInvoke(user{Username: settings.Username, Role: "admin"}, call, def.Handler)
		if err != nil {
			return mcpResponse(req.ID, mcpToolErrorResult(err))
		}
		payload, _ := json.MarshalIndent(data, "", "  ")
		return mcpResponse(req.ID, gin.H{
			"content": []gin.H{{"type": "text", "text": string(payload)}},
			"isError": false,
		})
	case "resources/list":
		return mcpResponse(req.ID, gin.H{"resources": []interface{}{}})
	case "resources/templates/list":
		return mcpResponse(req.ID, gin.H{"resourceTemplates": []interface{}{}})
	case "prompts/list":
		return mcpResponse(req.ID, gin.H{"prompts": []interface{}{}})
	case "logging/setLevel":
		return mcpResponse(req.ID, mcpEmptyObject{})
	default:
		return mcpErrorResponse(req.ID, -32601, "method not found: "+req.Method)
	}
}

func mcpNegotiateProtocolVersion(requested string) string {
	requested = strings.TrimSpace(requested)
	for _, supported := range []string{"2024-11-05", "2025-03-26", "2025-06-18"} {
		if requested == supported {
			return supported
		}
	}
	return mcpLatestProtocolVersion
}

func mcpToolErrorResult(err error) gin.H {
	return gin.H{
		"content": []gin.H{{"type": "text", "text": err.Error()}},
		"isError": true,
	}
}

type mcpCall struct {
	Method string
	Path   string
	Params map[string]string
	Query  url.Values
	Body   interface{}
}

type mcpToolDef struct {
	Name        string
	Description string
	Group       string
	InputSchema map[string]interface{}
	Handler     gin.HandlerFunc
	Build       func(map[string]interface{}) (mcpCall, error)
}

func (a *API) mcpToolSummaries() []gin.H {
	tools := a.mcpTools()
	items := make([]gin.H, 0, len(tools))
	for _, def := range tools {
		items = append(items, gin.H{"name": def.Name, "description": def.Description, "group": def.Group, "input_schema": def.InputSchema})
	}
	return items
}

func (a *API) mcpToolByName(name string) (mcpToolDef, bool) {
	for _, def := range a.mcpTools() {
		if def.Name == name {
			return def, true
		}
	}
	return mcpToolDef{}, false
}

func (a *API) mcpInvoke(u user, call mcpCall, handler gin.HandlerFunc) (interface{}, error) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	var reader io.Reader
	if call.Body != nil {
		payload, err := json.Marshal(call.Body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(payload)
	}
	req := httptest.NewRequest(call.Method, "http://kafkavista.local/", reader)
	if call.Body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if call.Query != nil {
		req.URL.RawQuery = call.Query.Encode()
	}
	c.Request = req
	c.Set("user", u)
	for key, value := range call.Params {
		c.Params = append(c.Params, gin.Param{Key: key, Value: value})
	}
	handler(c)

	body := recorder.Body.Bytes()
	var decoded map[string]interface{}
	if len(bytes.TrimSpace(body)) > 0 {
		_ = json.Unmarshal(body, &decoded)
	}
	if recorder.Code >= http.StatusBadRequest {
		message := strings.TrimSpace(string(body))
		if decoded != nil {
			if detail, ok := decoded["detail"].(string); ok && detail != "" {
				message = detail
			} else if msg, ok := decoded["message"].(string); ok && msg != "" {
				message = msg
			}
		}
		return nil, fmt.Errorf("%s", message)
	}
	if decoded != nil {
		if data, ok := decoded["data"]; ok {
			return data, nil
		}
		return decoded, nil
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return gin.H{"success": true}, nil
	}
	var generic interface{}
	if err := json.Unmarshal(body, &generic); err == nil {
		return generic, nil
	}
	return string(body), nil
}

func (a *API) mcpClusterArg(args map[string]interface{}) (string, error) {
	raw := mcpStringArg(args, "cluster")
	if raw == "" {
		raw = mcpStringArg(args, "cluster_id")
	}
	if raw == "" {
		raw = mcpStringArg(args, "cluster_name")
	}
	if raw == "" {
		return "", errors.New("cluster is required (cluster id or name)")
	}
	var cluster model.KafkaCluster
	if err := a.db.Where("is_active = ? AND (id = ? OR name = ?)", true, raw, raw).First(&cluster).Error; err != nil {
		return "", fmt.Errorf("Kafka cluster not found: %s", raw)
	}
	return cluster.ID, nil
}

func (a *API) mcpClusterCall(method, path string, args map[string]interface{}, params map[string]string, query url.Values, body interface{}) (mcpCall, error) {
	clusterID, err := a.mcpClusterArg(args)
	if err != nil {
		return mcpCall{}, err
	}
	if params == nil {
		params = map[string]string{}
	}
	params["cluster"] = clusterID
	if query == nil {
		query = url.Values{}
	}
	return mcpCall{Method: method, Path: path, Params: params, Query: query, Body: body}, nil
}

func (a *API) mcpTopicCall(method, suffix string, args map[string]interface{}, query url.Values, body interface{}) (mcpCall, error) {
	topic := mcpStringArg(args, "topic")
	if topic == "" {
		return mcpCall{}, errors.New("topic is required")
	}
	return a.mcpClusterCall(method, "/clusters/:cluster/topics/"+topic+suffix, args, map[string]string{"topic": topic}, query, body)
}

func mcpStringArg(args map[string]interface{}, key string) string {
	value, ok := args[key]
	if !ok || value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func mcpIntArg(args map[string]interface{}, key string) (int, bool) {
	value, ok := args[key]
	if !ok || value == nil {
		return 0, false
	}
	switch v := value.(type) {
	case float64:
		return int(v), true
	case int:
		return v, true
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func mcpBoolArg(args map[string]interface{}, key string) (bool, bool) {
	value, ok := args[key]
	if !ok || value == nil {
		return false, false
	}
	switch v := value.(type) {
	case bool:
		return v, true
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(v))
		if err != nil {
			return false, false
		}
		return parsed, true
	default:
		return false, false
	}
}

func mcpStringSliceArg(args map[string]interface{}, key string) []string {
	value, ok := args[key]
	if !ok || value == nil {
		return nil
	}
	items := []string{}
	switch v := value.(type) {
	case []interface{}:
		for _, item := range v {
			if text := strings.TrimSpace(fmt.Sprint(item)); text != "" {
				items = append(items, text)
			}
		}
	case []string:
		for _, item := range v {
			if text := strings.TrimSpace(item); text != "" {
				items = append(items, text)
			}
		}
	case string:
		for _, item := range strings.Split(v, ",") {
			if text := strings.TrimSpace(item); text != "" {
				items = append(items, text)
			}
		}
	}
	return items
}

func mcpAddQuery(query url.Values, args map[string]interface{}, keys ...string) {
	for _, key := range keys {
		value, ok := args[key]
		if !ok || value == nil {
			continue
		}
		switch v := value.(type) {
		case string:
			if strings.TrimSpace(v) != "" {
				query.Set(key, v)
			}
		case bool:
			query.Set(key, strconv.FormatBool(v))
		case float64:
			query.Set(key, strconv.FormatFloat(v, 'f', -1, 64))
		case []interface{}:
			parts := make([]string, 0, len(v))
			for _, item := range v {
				parts = append(parts, fmt.Sprint(item))
			}
			if len(parts) > 0 {
				query.Set(key, strings.Join(parts, ","))
			}
		default:
			query.Set(key, fmt.Sprint(v))
		}
	}
}

func mcpCopy(args map[string]interface{}, keys ...string) map[string]interface{} {
	body := map[string]interface{}{}
	for _, key := range keys {
		if value, ok := args[key]; ok && value != nil {
			body[key] = value
		}
	}
	return body
}

func mcpObjectSchema(props map[string]interface{}, required ...string) map[string]interface{} {
	schema := map[string]interface{}{"type": "object", "properties": props}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func mcpStringProp(description string) map[string]interface{} {
	return map[string]interface{}{"type": "string", "description": description}
}

func mcpEnumProp(description string, values ...string) map[string]interface{} {
	return map[string]interface{}{"type": "string", "description": description, "enum": values}
}

func mcpIntProp(description string) map[string]interface{} {
	return map[string]interface{}{"type": "integer", "description": description}
}

func mcpBoolProp(description string) map[string]interface{} {
	return map[string]interface{}{"type": "boolean", "description": description}
}

func mcpArrayProp(description string, items map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{"type": "array", "description": description, "items": items}
}

func mcpClusterProp() map[string]interface{} {
	return mcpStringProp("Kafka 集群 ID 或集群名称。可先调用 kafka_list_clusters 获取。")
}

func (a *API) mcpTools() []mcpToolDef {
	clusterProp := mcpClusterProp()
	topicProp := mcpStringProp("Topic 名称")
	groupProp := mcpStringProp("Consumer Group ID")
	return []mcpToolDef{
		{
			Name: "kafka_list_clusters", Group: "Cluster",
			Description: "列出当前平台纳管的 Kafka 集群，以及当前账号对这些集群拥有的权限。",
			InputSchema: mcpObjectSchema(map[string]interface{}{}),
			Handler:     a.listClusters,
			Build: func(map[string]interface{}) (mcpCall, error) {
				return mcpCall{Method: http.MethodGet, Path: "/kafka-api/clusters"}, nil
			},
		},
		{
			Name: "kafka_get_cluster_stats", Group: "Cluster",
			Description: "获取 Kafka 集群统计信息：Topic 数、Consumer Group 数、Broker 数和 Controller。",
			InputSchema: mcpObjectSchema(map[string]interface{}{"cluster": clusterProp}, "cluster"),
			Handler:     a.clusterStats,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				return a.mcpClusterCall(http.MethodGet, "/kafka-api/clusters/:cluster/stats", args, nil, nil, nil)
			},
		},
		{
			Name: "kafka_get_cluster_overview", Group: "Cluster",
			Description: "获取集群总览：Topic 名称列表、Consumer Group 列表及数量。",
			InputSchema: mcpObjectSchema(map[string]interface{}{"cluster": clusterProp}, "cluster"),
			Handler:     a.clusterOverview,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				return a.mcpClusterCall(http.MethodGet, "/kafka-api/clusters/:cluster/overview", args, nil, nil, nil)
			},
		},
		{
			Name: "kafka_get_cluster_detail", Group: "Cluster",
			Description: "获取 Broker 节点详情（地址、Controller 状态等），可选是否同时返回 Topic 列表。",
			InputSchema: mcpObjectSchema(map[string]interface{}{
				"cluster":        clusterProp,
				"include_topics": mcpBoolProp("是否包含 Topic 列表，默认 true"),
			}, "cluster"),
			Handler: a.clusterDetail,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				query := url.Values{}
				mcpAddQuery(query, args, "include_topics")
				return a.mcpClusterCall(http.MethodGet, "/kafka-api/clusters/:cluster/detail", args, nil, query, nil)
			},
		},
		{
			Name: "kafka_get_cluster_metrics", Group: "Cluster",
			Description: "获取集群实时指标或历史指标。实时指标包含 Broker/Topic/Group 数量、总 Lag、URP/Offline 分区等；传入 range 或 from/to 时返回历史采样点。",
			InputSchema: mcpObjectSchema(map[string]interface{}{
				"cluster":     clusterProp,
				"topic_limit": mcpIntProp("Topic 指标数量上限"),
				"group_limit": mcpIntProp("Consumer Group 指标数量上限"),
				"range":       mcpIntProp("历史时间范围（分钟）"),
				"from":        mcpStringProp("历史起点，毫秒时间戳"),
				"to":          mcpStringProp("历史终点，毫秒时间戳"),
			}, "cluster"),
			Handler: a.clusterMetrics,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				query := url.Values{}
				mcpAddQuery(query, args, "topic_limit", "group_limit", "range", "from", "to")
				return a.mcpClusterCall(http.MethodGet, "/kafka-api/clusters/:cluster/metrics", args, nil, query, nil)
			},
		},
		{
			Name: "kafka_create_cluster", Group: "Cluster",
			Description: "新增 Kafka 集群。需要名称和 bootstrap_servers；支持 PLAINTEXT / SSL / SASL 认证参数。",
			InputSchema: mcpObjectSchema(map[string]interface{}{
				"name":              mcpStringProp("集群名称，必须唯一"),
				"bootstrap_servers": mcpStringProp("Kafka 地址，多个用逗号分隔，如 broker1:9092,broker2:9092"),
				"cluster_type":      mcpStringProp("类型：cluster 或 standalone"),
				"security_protocol": mcpStringProp("安全协议，如 PLAINTEXT / SSL / SASL_PLAINTEXT / SASL_SSL"),
				"sasl_mechanism":    mcpStringProp("SASL 机制，如 PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512"),
				"sasl_username":     mcpStringProp("SASL 用户名"),
				"sasl_password":     mcpStringProp("SASL 密码"),
				"description":       mcpStringProp("集群描述"),
			}, "name", "bootstrap_servers"),
			Handler: a.mcpCreateCluster,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				body := mcpCopy(args, "name", "bootstrap_servers", "cluster_type", "security_protocol", "sasl_mechanism", "sasl_username", "sasl_password", "description")
				return mcpCall{Method: http.MethodPost, Path: "/kafka-api/clusters", Body: body}, nil
			},
		},
		{
			Name: "kafka_update_cluster", Group: "Cluster",
			Description: "更新 Kafka 集群配置。只需传入要修改的字段，其余字段自动沿用原配置。",
			InputSchema: mcpObjectSchema(map[string]interface{}{
				"cluster":           clusterProp,
				"name":              mcpStringProp("新的集群名称"),
				"bootstrap_servers": mcpStringProp("新的 Kafka 地址"),
				"cluster_type":      mcpStringProp("类型：cluster 或 standalone"),
				"security_protocol": mcpStringProp("安全协议"),
				"sasl_mechanism":    mcpStringProp("SASL 机制"),
				"sasl_username":     mcpStringProp("SASL 用户名"),
				"sasl_password":     mcpStringProp("SASL 密码"),
				"description":       mcpStringProp("集群描述"),
			}, "cluster"),
			Handler: a.mcpUpdateCluster,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				clusterID, err := a.mcpClusterArg(args)
				if err != nil {
					return mcpCall{}, err
				}
				body := mcpCopy(args, "name", "cluster_type", "bootstrap_servers", "security_protocol", "sasl_mechanism", "sasl_username", "sasl_password", "description")
				return mcpCall{Method: http.MethodPut, Path: "/kafka-api/clusters/" + clusterID, Params: map[string]string{"cluster": clusterID}, Body: body}, nil
			},
		},
		{
			Name: "kafka_delete_cluster", Group: "Cluster",
			Description: "从平台下线（软删除）一个 Kafka 集群。",
			InputSchema: mcpObjectSchema(map[string]interface{}{"cluster": clusterProp}, "cluster"),
			Handler:     a.deleteCluster,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				return a.mcpClusterCall(http.MethodDelete, "/kafka-api/clusters/:cluster", args, nil, nil, nil)
			},
		},
		{
			Name: "kafka_list_topics", Group: "Topic",
			Description: "分页查询 Topic 列表，返回分区数、消息量、日志大小等。",
			InputSchema: mcpObjectSchema(map[string]interface{}{
				"cluster":    clusterProp,
				"page":       mcpIntProp("页码，默认 1"),
				"page_size":  mcpIntProp("每页条数，默认 50，最大 100"),
				"name":       mcpStringProp("Topic 名称关键字"),
				"sort_by":    mcpEnumProp("排序字段", "topic", "log_size"),
				"sort_order": mcpEnumProp("排序方向", "asc", "desc"),
			}, "cluster"),
			Handler: a.topicPage,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				query := url.Values{}
				mcpAddQuery(query, args, "page", "page_size", "name", "sort_by", "sort_order")
				return a.mcpClusterCall(http.MethodGet, "/kafka-api/clusters/:cluster/topics", args, nil, query, nil)
			},
		},
		{
			Name: "kafka_get_topic", Group: "Topic",
			Description: "查询 Topic 详情：每个分区的 beginning/end offset、Leader、副本、ISR 和日志大小。",
			InputSchema: mcpObjectSchema(map[string]interface{}{"cluster": clusterProp, "topic": topicProp}, "cluster", "topic"),
			Handler:     a.topicDetail,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				return a.mcpTopicCall(http.MethodGet, "", args, nil, nil)
			},
		},
		{
			Name: "kafka_create_topic", Group: "Topic",
			Description: "创建 Topic，指定分区数和副本因子。",
			InputSchema: mcpObjectSchema(map[string]interface{}{
				"cluster":            clusterProp,
				"topic":              topicProp,
				"partitions":         mcpIntProp("分区数，默认 1"),
				"replication_factor": mcpIntProp("副本因子，默认 1"),
			}, "cluster", "topic"),
			Handler: a.createTopic,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				body := mcpCopy(args, "topic", "partitions", "replication_factor")
				return a.mcpClusterCall(http.MethodPost, "/kafka-api/clusters/:cluster/topics", args, nil, nil, body)
			},
		},
		{
			Name: "kafka_update_topic_partitions", Group: "Topic",
			Description: "增加 Topic 分区数（Kafka 不支持减少分区）。",
			InputSchema: mcpObjectSchema(map[string]interface{}{
				"cluster":    clusterProp,
				"topic":      topicProp,
				"partitions": mcpIntProp("新的分区数，必须大于当前分区数"),
			}, "cluster", "topic", "partitions"),
			Handler: a.updateTopicPartitions,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				body := mcpCopy(args, "partitions")
				return a.mcpTopicCall(http.MethodPut, "/partitions", args, nil, body)
			},
		},
		{
			Name: "kafka_delete_topic", Group: "Topic",
			Description: "删除 Topic。该操作不可恢复，请谨慎调用。",
			InputSchema: mcpObjectSchema(map[string]interface{}{"cluster": clusterProp, "topic": topicProp}, "cluster", "topic"),
			Handler:     a.deleteTopic,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				return a.mcpTopicCall(http.MethodDelete, "", args, nil, nil)
			},
		},
		{
			Name: "kafka_get_topic_configs", Group: "Topic",
			Description: "查询 Topic 配置项及可修改白名单。",
			InputSchema: mcpObjectSchema(map[string]interface{}{"cluster": clusterProp, "topic": topicProp}, "cluster", "topic"),
			Handler:     a.topicConfigs,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				return a.mcpTopicCall(http.MethodGet, "/configs", args, nil, nil)
			},
		},
		{
			Name: "kafka_update_topic_configs", Group: "Topic",
			Description: "批量修改 Topic 配置。configs 为键值对象，值为字符串；传 null 表示删除该配置并恢复默认。",
			InputSchema: mcpObjectSchema(map[string]interface{}{
				"cluster": clusterProp,
				"topic":   topicProp,
				"configs": map[string]interface{}{"type": "object", "description": "配置键值对象，如 {\"retention.ms\":\"604800000\"}", "additionalProperties": true},
			}, "cluster", "topic", "configs"),
			Handler: a.updateTopicConfigs,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				configs, _ := args["configs"]
				body := map[string]interface{}{"configs": configs}
				return a.mcpTopicCall(http.MethodPut, "/configs", args, nil, body)
			},
		},
		{
			Name: "kafka_delete_topic_config", Group: "Topic",
			Description: "删除 Topic 的单个配置项，使其恢复默认值。",
			InputSchema: mcpObjectSchema(map[string]interface{}{
				"cluster": clusterProp,
				"topic":   topicProp,
				"config":  mcpStringProp("配置项名称，如 retention.ms"),
			}, "cluster", "topic", "config"),
			Handler: a.deleteTopicConfig,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				config := mcpStringArg(args, "config")
				if config == "" {
					return mcpCall{}, errors.New("config is required")
				}
				return a.mcpTopicCall(http.MethodDelete, "/configs/"+config, args, nil, nil)
			},
		},
		{
			Name: "kafka_read_messages", Group: "Message",
			Description: "读取 Topic 消息。支持按分区、offset、时间范围、Key/Value 关键字过滤。partition 可传 all 查询全部分区。",
			InputSchema: mcpObjectSchema(map[string]interface{}{
				"cluster":         clusterProp,
				"topic":           topicProp,
				"partition":       mcpStringProp("分区号；传 all 表示全部分区"),
				"autoOffsetReset": mcpEnumProp("offset 起点的兜底策略", "earliest", "newest"),
				"offset":          mcpIntProp("起始 offset"),
				"end_offset":      mcpIntProp("结束 offset（包含）"),
				"start_time_ms":   mcpIntProp("开始时间，毫秒时间戳"),
				"end_time_ms":     mcpIntProp("结束时间，毫秒时间戳"),
				"key_search":      mcpStringProp("Key 包含匹配（忽略大小写）"),
				"value_search":    mcpStringProp("Value 包含匹配（忽略大小写）"),
				"count":           mcpIntProp("最多返回消息数，默认 20"),
				"timeout_ms":      mcpIntProp("拉取超时毫秒数，默认 1500"),
			}, "cluster", "topic"),
			Handler: a.topicData,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				query := url.Values{}
				mcpAddQuery(query, args, "partition", "autoOffsetReset", "offset", "end_offset", "start_time_ms", "end_time_ms", "key_search", "value_search", "count", "timeout_ms")
				return a.mcpTopicCall(http.MethodGet, "/data", args, query, nil)
			},
		},
		{
			Name: "kafka_send_message", Group: "Message",
			Description: "向 Topic 发送消息。可发送单条（key/value）或多条（messages 数组，最多 1000 条）。",
			InputSchema: mcpObjectSchema(map[string]interface{}{
				"cluster":   clusterProp,
				"topic":     topicProp,
				"partition": mcpIntProp("指定分区，不传则由 Kafka 自动分区"),
				"key":       mcpStringProp("消息 Key"),
				"value":     mcpStringProp("消息 Value"),
				"messages": mcpArrayProp("批量消息，元素为 {partition,key,value}", map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"partition": map[string]interface{}{"type": "integer"},
						"key":       map[string]interface{}{"type": "string"},
						"value":     map[string]interface{}{"type": "string"},
					},
				}),
			}, "cluster", "topic"),
			Handler: a.sendMessage,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				body := mcpCopy(args, "topic", "partition", "key", "value", "messages")
				return a.mcpClusterCall(http.MethodPost, "/kafka-api/clusters/:cluster/messages", args, nil, nil, body)
			},
		},
		{
			Name: "kafka_delete_records", Group: "Message",
			Description: "删除消息记录。当前平台版本未启用该能力时会返回不支持。",
			InputSchema: mcpObjectSchema(map[string]interface{}{
				"cluster":   clusterProp,
				"topic":     topicProp,
				"partition": mcpIntProp("分区号"),
				"offset":    mcpIntProp("删除起始 offset"),
			}, "cluster"),
			Handler: a.deleteRecords,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				body := mcpCopy(args, "topic", "partition", "offset")
				return a.mcpClusterCall(http.MethodDelete, "/kafka-api/clusters/:cluster/messages", args, nil, nil, body)
			},
		},
		{
			Name: "kafka_list_consumer_groups", Group: "Consumer Group",
			Description: "列出集群内的 Consumer Group（同时返回 Topic 列表）。",
			InputSchema: mcpObjectSchema(map[string]interface{}{"cluster": clusterProp}, "cluster"),
			Handler:     a.clusterOverview,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				return a.mcpClusterCall(http.MethodGet, "/kafka-api/clusters/:cluster/overview", args, nil, nil, nil)
			},
		},
		{
			Name: "kafka_get_group_summaries", Group: "Consumer Group",
			Description: "批量查询 Consumer Group 摘要（Topic、分区数等）。",
			InputSchema: mcpObjectSchema(map[string]interface{}{
				"cluster":   clusterProp,
				"group_ids": mcpArrayProp("Consumer Group ID 列表", map[string]interface{}{"type": "string"}),
			}, "cluster", "group_ids"),
			Handler: a.groupSummary,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				query := url.Values{}
				query.Set("group_ids", strings.Join(mcpStringSliceArg(args, "group_ids"), ","))
				return a.mcpClusterCall(http.MethodGet, "/kafka-api/clusters/:cluster/groups/summary", args, nil, query, nil)
			},
		},
		{
			Name: "kafka_get_group_detail", Group: "Consumer Group",
			Description: "查询 Consumer Group 详情：成员、订阅 Topic、各分区 committed offset、end offset 和 Lag。",
			InputSchema: mcpObjectSchema(map[string]interface{}{
				"cluster": clusterProp,
				"group":   groupProp,
				"topic":   mcpStringProp("按 Topic 名称过滤"),
			}, "cluster", "group"),
			Handler: a.groupDetail,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				group := mcpStringArg(args, "group")
				if group == "" {
					return mcpCall{}, errors.New("group is required")
				}
				query := url.Values{}
				mcpAddQuery(query, args, "topic")
				return a.mcpClusterCall(http.MethodGet, "/kafka-api/clusters/:cluster/groups/"+group, args, map[string]string{"group": group}, query, nil)
			},
		},
		{
			Name: "kafka_get_group_history", Group: "Consumer Group",
			Description: "查询 Consumer Group 历史指标。当前版本返回空列表，保留接口兼容性。",
			InputSchema: mcpObjectSchema(map[string]interface{}{"cluster": clusterProp, "group": groupProp}, "cluster", "group"),
			Handler:     a.groupHistory,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				group := mcpStringArg(args, "group")
				if group == "" {
					return mcpCall{}, errors.New("group is required")
				}
				return a.mcpClusterCall(http.MethodGet, "/kafka-api/clusters/:cluster/groups/"+group+"/history", args, map[string]string{"group": group}, nil, nil)
			},
		},
		{
			Name: "kafka_create_group", Group: "Consumer Group", Description: "在 KafkaVista 登记一个 Consumer Group（真正的 offset 由消费者提交）。",
			InputSchema: mcpObjectSchema(map[string]interface{}{"cluster": clusterProp, "group": groupProp}, "cluster", "group"),
			Handler:     a.createGroupPlaceholder,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				group := mcpStringArg(args, "group")
				if group == "" {
					return mcpCall{}, errors.New("group is required")
				}
				return a.mcpClusterCall(http.MethodPost, "/kafka-api/clusters/:cluster/groups/"+group, args, map[string]string{"group": group}, nil, nil)
			},
		},
		{
			Name: "kafka_delete_group", Group: "Consumer Group",
			Description: "删除 Consumer Group。请确保相关消费者已停止。",
			InputSchema: mcpObjectSchema(map[string]interface{}{"cluster": clusterProp, "group": groupProp}, "cluster", "group"),
			Handler:     a.deleteGroup,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				group := mcpStringArg(args, "group")
				if group == "" {
					return mcpCall{}, errors.New("group is required")
				}
				return a.mcpClusterCall(http.MethodDelete, "/kafka-api/clusters/:cluster/groups/"+group, args, map[string]string{"group": group}, nil, nil)
			},
		},
		{
			Name: "kafka_delete_group_topic", Group: "Consumer Group",
			Description: "解除 Consumer Group 与指定 Topic 的订阅关系（删除该 Group 在该 Topic 上的已提交 offset）。",
			InputSchema: mcpObjectSchema(map[string]interface{}{"cluster": clusterProp, "group": groupProp, "topic": topicProp}, "cluster", "group", "topic"),
			Handler:     a.deleteGroupTopic,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				group := mcpStringArg(args, "group")
				topic := mcpStringArg(args, "topic")
				if group == "" || topic == "" {
					return mcpCall{}, errors.New("group and topic are required")
				}
				return a.mcpClusterCall(http.MethodDelete, "/kafka-api/clusters/:cluster/groups/"+group+"/topics/"+topic, args, map[string]string{"group": group, "topic": topic}, nil, nil)
			},
		},
		{
			Name: "kafka_list_permissions", Group: "Permission",
			Description: "查询指定集群下用户级和角色级的 Kafka 权限配置。",
			InputSchema: mcpObjectSchema(map[string]interface{}{"cluster": clusterProp}, "cluster"),
			Handler:     a.listPermissions,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				return a.mcpClusterCall(http.MethodGet, "/kafka-api/clusters/:cluster/permissions", args, nil, nil, nil)
			},
		},
		{
			Name: "kafka_save_permissions", Group: "Permission",
			Description: "保存 Kafka 权限。subject_type 为 user 时使用 username，为 role 时使用 role；actions 为权限动作列表。",
			InputSchema: mcpObjectSchema(map[string]interface{}{
				"cluster":      clusterProp,
				"subject_type": mcpEnumProp("授权对象类型", "user", "role"),
				"username":     mcpStringProp("用户名（subject_type=user 时必填）"),
				"role":         mcpStringProp("角色名（subject_type=role 时必填）"),
				"actions": mcpArrayProp("权限动作列表", map[string]interface{}{
					"type": "string",
					"enum": model.AllActions,
				}),
			}, "cluster", "subject_type", "actions"),
			Handler: a.savePermissions,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				body := mcpCopy(args, "subject_type", "username", "role", "actions")
				return a.mcpClusterCall(http.MethodPut, "/kafka-api/clusters/:cluster/permissions", args, nil, nil, body)
			},
		},
		{
			Name: "kafka_list_actions", Group: "Permission",
			Description: "列出平台支持的全部 Kafka 原子权限动作。",
			InputSchema: mcpObjectSchema(map[string]interface{}{}),
			Handler:     a.actions,
			Build: func(map[string]interface{}) (mcpCall, error) {
				return mcpCall{Method: http.MethodGet, Path: "/kafka-api/actions"}, nil
			},
		},
		{
			Name: "kafka_list_audit_logs", Group: "Audit",
			Description: "查询 KafkaVista 操作审计日志，支持按时间、集群、动作、用户筛选和分页。",
			InputSchema: mcpObjectSchema(map[string]interface{}{
				"start_time": mcpStringProp("开始时间，RFC3339 或 YYYY-MM-DD HH:mm:ss"),
				"end_time":   mcpStringProp("结束时间，RFC3339 或 YYYY-MM-DD HH:mm:ss"),
				"cluster_id": mcpStringProp("集群 ID"),
				"action":     mcpStringProp("动作，如 topic_create / message_send"),
				"username":   mcpStringProp("操作人"),
				"page":       mcpIntProp("页码"),
				"page_size":  mcpIntProp("每页条数"),
			}),
			Handler: a.auditLogs,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				query := url.Values{}
				mcpAddQuery(query, args, "start_time", "end_time", "cluster_id", "action", "username", "page", "page_size")
				return mcpCall{Method: http.MethodGet, Path: "/kafka-api/audit-logs", Query: query}, nil
			},
		},
		{
			Name: "kafka_list_migrations", Group: "Migration",
			Description: "列出 Kafka 平滑迁移任务。",
			InputSchema: mcpObjectSchema(map[string]interface{}{}),
			Handler:     a.listMigrations,
			Build: func(map[string]interface{}) (mcpCall, error) {
				return mcpCall{Method: http.MethodGet, Path: "/kafka-api/migrations"}, nil
			},
		},
		{
			Name: "kafka_check_migration", Group: "Migration",
			Description: "迁移前检查：校验源/目标集群连通性、Topic 列表及目标端同名 Topic 冲突。",
			InputSchema: mcpObjectSchema(map[string]interface{}{
				"source_cluster_id": mcpStringProp("源集群 ID"),
				"target_cluster_id": mcpStringProp("目标集群 ID"),
				"topics":            mcpArrayProp("要迁移的 Topic；传空表示全部", map[string]interface{}{"type": "string"}),
			}, "source_cluster_id", "target_cluster_id"),
			Handler: a.checkMigration,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				body := mcpCopy(args, "source_cluster_id", "target_cluster_id", "topics", "create_topics", "copy_topic_configs", "copy_data", "overwrite_existing_topics", "incremental_sync", "batch_size", "throttle_ms", "incremental_poll_ms", "max_messages_per_topic", "replication_factor", "target_partitions")
				return mcpCall{Method: http.MethodPost, Path: "/kafka-api/migrations/check", Body: body}, nil
			},
		},
		{
			Name: "kafka_create_migration", Group: "Migration",
			Description: "创建 Kafka 平滑迁移任务。支持复制 Topic 元数据、配置、消息及实时增量同步。",
			InputSchema: mcpObjectSchema(map[string]interface{}{
				"source_cluster_id":         mcpStringProp("源集群 ID"),
				"target_cluster_id":         mcpStringProp("目标集群 ID"),
				"topics":                    mcpArrayProp("要迁移的 Topic；传空表示全部", map[string]interface{}{"type": "string"}),
				"create_topics":             mcpBoolProp("是否在目标端自动创建 Topic"),
				"copy_topic_configs":        mcpBoolProp("是否复制 Topic 配置"),
				"copy_data":                 mcpBoolProp("是否复制消息数据"),
				"overwrite_existing_topics": mcpBoolProp("目标端同名 Topic 是否覆盖"),
				"incremental_sync":          mcpBoolProp("是否开启实时增量同步"),
				"batch_size":                mcpIntProp("批大小，默认 50"),
				"throttle_ms":               mcpIntProp("批次间隔毫秒，默认 500"),
				"incremental_poll_ms":       mcpIntProp("增量同步轮询间隔毫秒"),
				"max_messages_per_topic":    mcpIntProp("每个 Topic 最大消息数，0 表示不限制"),
				"replication_factor":        mcpIntProp("目标端副本因子"),
				"target_partitions":         mcpIntProp("目标端分区数"),
			}, "source_cluster_id", "target_cluster_id"),
			Handler: a.createMigration,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				body := mcpCopy(args, "source_cluster_id", "target_cluster_id", "topics", "create_topics", "copy_topic_configs", "copy_data", "overwrite_existing_topics", "incremental_sync", "batch_size", "throttle_ms", "incremental_poll_ms", "max_messages_per_topic", "replication_factor", "target_partitions")
				return mcpCall{Method: http.MethodPost, Path: "/kafka-api/migrations", Body: body}, nil
			},
		},
		{
			Name: "kafka_get_migration", Group: "Migration",
			Description: "查询单个迁移任务的状态、进度和错误信息。",
			InputSchema: mcpObjectSchema(map[string]interface{}{"job": mcpStringProp("迁移任务 ID")}, "job"),
			Handler:     a.getMigration,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				job := mcpStringArg(args, "job")
				if job == "" {
					return mcpCall{}, errors.New("job is required")
				}
				return mcpCall{Method: http.MethodGet, Path: "/kafka-api/migrations/" + job, Params: map[string]string{"job": job}}, nil
			},
		},
		{
			Name: "kafka_stop_migration", Group: "Migration",
			Description: "停止正在运行的迁移任务。",
			InputSchema: mcpObjectSchema(map[string]interface{}{"job": mcpStringProp("迁移任务 ID")}, "job"),
			Handler:     a.stopMigration,
			Build: func(args map[string]interface{}) (mcpCall, error) {
				job := mcpStringArg(args, "job")
				if job == "" {
					return mcpCall{}, errors.New("job is required")
				}
				return mcpCall{Method: http.MethodPost, Path: "/kafka-api/migrations/" + job + "/stop", Params: map[string]string{"job": job}}, nil
			},
		},
	}
}
