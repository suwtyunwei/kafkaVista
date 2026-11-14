package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"

	"kafkavista/server/internal/config"
	"kafkavista/server/internal/model"
	"kafkavista/server/internal/store"
)

func TestMCPEndToEnd(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dir := t.TempDir()
	cfg := config.Config{
		HTTPAddr:                ":0",
		DatabasePath:            filepath.Join(dir, "test.db"),
		JWTSecret:               "test-secret",
		JWTExpireHours:          1,
		DefaultUser:             "admin",
		DefaultPassword:         "admin",
		DefaultClusterName:      "default",
		DefaultBootstrapServers: "127.0.0.1:1",
		KafkaRequestTimeoutMS:   100,
		KafkaMaxPollRecords:     100,
		KafkaMaxValueLength:     1024,
		KafkaMaxResponseBytes:   1024 * 1024,
	}
	db, err := store.Open(cfg)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	if err := store.Init(db, cfg); err != nil {
		t.Fatalf("init database: %v", err)
	}
	router := NewRouter(cfg, db)

	login := mcpTestRequest(t, router, http.MethodPost, "/api/auth/login", "", map[string]interface{}{"username": "admin", "password": "admin"})
	if login.Code != http.StatusOK {
		t.Fatalf("login status = %d body=%s", login.Code, login.Body.String())
	}
	var loginBody struct {
		Data struct {
			AccessToken string `json:"access_token"`
		} `json:"data"`
	}
	if err := json.Unmarshal(login.Body.Bytes(), &loginBody); err != nil || loginBody.Data.AccessToken == "" {
		t.Fatalf("login token missing: %v %s", err, login.Body.String())
	}
	jwt := loginBody.Data.AccessToken

	configResp := mcpTestRequest(t, router, http.MethodGet, "/api/admin/mcp", jwt, nil)
	if configResp.Code != http.StatusOK {
		t.Fatalf("get mcp config status = %d body=%s", configResp.Code, configResp.Body.String())
	}
	var configBody struct {
		Data MCPSettings `json:"data"`
	}
	if err := json.Unmarshal(configResp.Body.Bytes(), &configBody); err != nil || configBody.Data.Token == "" {
		t.Fatalf("mcp token missing: %v %s", err, configResp.Body.String())
	}
	mcpToken := configBody.Data.Token

	enableResp := mcpTestRequest(t, router, http.MethodPut, "/api/admin/mcp", jwt, map[string]interface{}{"enabled": true, "username": "agent"})
	if enableResp.Code != http.StatusOK {
		t.Fatalf("enable mcp status = %d body=%s", enableResp.Code, enableResp.Body.String())
	}

	unauthorized := mcpTestRequest(t, router, http.MethodPost, "/mcp", "wrong-token", map[string]interface{}{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": map[string]interface{}{}})
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorized.Code)
	}

	initialize := mcpTestRequest(t, router, http.MethodPost, "/api/mcp", mcpToken, map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params":  map[string]interface{}{"protocolVersion": "2025-06-18", "capabilities": map[string]interface{}{}, "clientInfo": map[string]interface{}{"name": "test", "version": "1.0.0"}},
	})
	if initialize.Code != http.StatusOK {
		t.Fatalf("initialize status = %d body=%s", initialize.Code, initialize.Body.String())
	}
	var initializeBody struct {
		Result struct {
			ProtocolVersion string `json:"protocolVersion"`
			ServerInfo      struct {
				Name string `json:"name"`
			} `json:"serverInfo"`
		} `json:"result"`
	}
	if err := json.Unmarshal(initialize.Body.Bytes(), &initializeBody); err != nil {
		t.Fatalf("decode initialize: %v", err)
	}
	if initializeBody.Result.ProtocolVersion != "2025-06-18" || initializeBody.Result.ServerInfo.Name != "kafkaVista" {
		t.Fatalf("unexpected initialize result: %s", initialize.Body.String())
	}

	initialized := mcpTestRequest(t, router, http.MethodPost, "/api/mcp", mcpToken, map[string]interface{}{"jsonrpc": "2.0", "method": "notifications/initialized"})
	if initialized.Code != http.StatusAccepted {
		t.Fatalf("initialized notification status = %d", initialized.Code)
	}

	pingResp := mcpTestRequest(t, router, http.MethodPost, "/api/mcp", mcpToken, map[string]interface{}{"jsonrpc": "2.0", "id": 9, "method": "ping"})
	var pingBody map[string]json.RawMessage
	if err := json.Unmarshal(pingResp.Body.Bytes(), &pingBody); err != nil {
		t.Fatalf("decode ping: %v", err)
	}
	if _, ok := pingBody["result"]; !ok {
		t.Fatalf("ping response must include result: %s", pingResp.Body.String())
	}

	toolsResp := mcpTestRequest(t, router, http.MethodPost, "/api/mcp", mcpToken, map[string]interface{}{"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
	if toolsResp.Code != http.StatusOK {
		t.Fatalf("tools/list status = %d body=%s", toolsResp.Code, toolsResp.Body.String())
	}
	var toolsBody struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal(toolsResp.Body.Bytes(), &toolsBody); err != nil {
		t.Fatalf("decode tools/list: %v", err)
	}
	if len(toolsBody.Result.Tools) != 35 {
		t.Fatalf("expected 35 Kafka tools, got %d tools", len(toolsBody.Result.Tools))
	}

	callResp := mcpTestRequest(t, router, http.MethodPost, "/api/mcp", mcpToken, map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      3,
		"method":  "tools/call",
		"params":  map[string]interface{}{"name": "kafka_list_clusters", "arguments": map[string]interface{}{}},
	})
	if callResp.Code != http.StatusOK {
		t.Fatalf("tools/call status = %d body=%s", callResp.Code, callResp.Body.String())
	}
	var callBody struct {
		Result struct {
			IsError bool `json:"isError"`
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
	}
	if err := json.Unmarshal(callResp.Body.Bytes(), &callBody); err != nil {
		t.Fatalf("decode tools/call: %v", err)
	}
	if callBody.Result.IsError || len(callBody.Result.Content) == 0 || !bytes.Contains([]byte(callBody.Result.Content[0].Text), []byte("default")) {
		t.Fatalf("unexpected list clusters result: %s", callResp.Body.String())
	}

	createResp := mcpTestRequest(t, router, http.MethodPost, "/api/mcp", mcpToken, map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      5,
		"method":  "tools/call",
		"params": map[string]interface{}{"name": "kafka_create_cluster", "arguments": map[string]interface{}{
			"name":              "sasl-test",
			"bootstrap_servers": "127.0.0.1:2",
			"security_protocol": "SASL_SSL",
			"sasl_mechanism":    "PLAIN",
			"sasl_username":     "mcp-user",
			"sasl_password":     "mcp-secret",
			"description":       "created by mcp",
		}},
	})
	if createResp.Code != http.StatusOK {
		t.Fatalf("create cluster status = %d body=%s", createResp.Code, createResp.Body.String())
	}
	var createBody struct {
		Result struct {
			IsError bool `json:"isError"`
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
	}
	if err := json.Unmarshal(createResp.Body.Bytes(), &createBody); err != nil {
		t.Fatalf("decode create cluster: %v", err)
	}
	if createBody.Result.IsError {
		t.Fatalf("create cluster failed: %s", createResp.Body.String())
	}
	var stored model.KafkaCluster
	if err := db.First(&stored, "name = ?", "sasl-test").Error; err != nil {
		t.Fatalf("load created cluster: %v", err)
	}
	if stored.SASLPassword != "mcp-secret" {
		t.Fatalf("MCP create cluster did not persist SASL password")
	}

	updateResp := mcpTestRequest(t, router, http.MethodPost, "/api/mcp", mcpToken, map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      6,
		"method":  "tools/call",
		"params": map[string]interface{}{"name": "kafka_update_cluster", "arguments": map[string]interface{}{
			"cluster":     stored.ID,
			"description": "updated by mcp",
		}},
	})
	if updateResp.Code != http.StatusOK {
		t.Fatalf("update cluster status = %d body=%s", updateResp.Code, updateResp.Body.String())
	}
	var updateBody struct {
		Result struct {
			IsError bool `json:"isError"`
		} `json:"result"`
	}
	if err := json.Unmarshal(updateResp.Body.Bytes(), &updateBody); err != nil {
		t.Fatalf("decode update cluster: %v", err)
	}
	if updateBody.Result.IsError {
		t.Fatalf("update cluster failed: %s", updateResp.Body.String())
	}
	if err := db.First(&stored, "id = ?", stored.ID).Error; err != nil {
		t.Fatalf("reload updated cluster: %v", err)
	}
	if stored.SASLPassword != "mcp-secret" || stored.Description != "updated by mcp" {
		t.Fatalf("update cluster did not merge correctly: password=%q description=%q", stored.SASLPassword, stored.Description)
	}

	unknownResp := mcpTestRequest(t, router, http.MethodPost, "/api/mcp", mcpToken, map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      4,
		"method":  "tools/call",
		"params":  map[string]interface{}{"name": "not_exists", "arguments": map[string]interface{}{}},
	})
	var unknownBody struct {
		Error *mcpRPCError `json:"error"`
	}
	if err := json.Unmarshal(unknownResp.Body.Bytes(), &unknownBody); err != nil || unknownBody.Error == nil || unknownBody.Error.Code != -32602 {
		t.Fatalf("unexpected unknown tool response: %s", unknownResp.Body.String())
	}
}

func mcpTestRequest(t *testing.T, handler http.Handler, method, path, token string, body interface{}) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		reader = bytes.NewReader(payload)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder
}
