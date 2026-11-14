package api

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"kafkavista/server/internal/config"
	"kafkavista/server/internal/model"
	"kafkavista/server/internal/store"
)

func TestDefaultAdminCannotBeDemotedOrDisabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db, cfg := adminTestStore(t)
	router, jwt := adminTestLogin(t, cfg, db)

	demote := mcpTestRequest(t, router, http.MethodPut, "/api/admin/users/admin", jwt, map[string]interface{}{"role": "user"})
	if demote.Code != http.StatusBadRequest {
		t.Fatalf("demote default admin status = %d body=%s", demote.Code, demote.Body.String())
	}
	disable := mcpTestRequest(t, router, http.MethodPut, "/api/admin/users/admin", jwt, map[string]interface{}{"is_active": false})
	if disable.Code != http.StatusBadRequest {
		t.Fatalf("disable default admin status = %d body=%s", disable.Code, disable.Body.String())
	}

	var admin model.AppUser
	if err := db.Where("username = ?", "admin").First(&admin).Error; err != nil {
		t.Fatalf("load default admin: %v", err)
	}
	if admin.Role != "admin" || !admin.IsActive {
		t.Fatalf("default admin was modified: role=%q active=%v", admin.Role, admin.IsActive)
	}

	admin.Role = "user"
	admin.IsActive = false
	if err := db.Save(&admin).Error; err != nil {
		t.Fatalf("demote admin directly: %v", err)
	}
	if err := store.Init(db, cfg); err != nil {
		t.Fatalf("re-init store: %v", err)
	}
	if err := db.Where("username = ?", "admin").First(&admin).Error; err != nil {
		t.Fatalf("reload default admin: %v", err)
	}
	if admin.Role != "admin" || !admin.IsActive {
		t.Fatalf("store.Init did not restore default admin: role=%q active=%v", admin.Role, admin.IsActive)
	}
}

func TestAdminKafkaPermissionsAreReadOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db, cfg := adminTestStore(t)
	router, jwt := adminTestLogin(t, cfg, db)

	createUser := mcpTestRequest(t, router, http.MethodPost, "/api/admin/users", jwt, map[string]interface{}{"username": "alice", "password": "alice-pass", "role": "user"})
	if createUser.Code != http.StatusOK {
		t.Fatalf("create user status = %d body=%s", createUser.Code, createUser.Body.String())
	}
	clusters := mcpTestRequest(t, router, http.MethodGet, "/api/kafka/clusters", jwt, nil)
	var clusterBody struct {
		Data struct {
			Items []struct {
				ID string `json:"id"`
			} `json:"items"`
		} `json:"data"`
	}
	if err := json.Unmarshal(clusters.Body.Bytes(), &clusterBody); err != nil || len(clusterBody.Data.Items) == 0 {
		t.Fatalf("load clusters: %v %s", err, clusters.Body.String())
	}
	clusterID := clusterBody.Data.Items[0].ID

	for _, payload := range []map[string]interface{}{
		{"subject_type": "user", "username": "admin", "actions": []string{"cluster_view"}},
		{"subject_type": "role", "role": "admin", "actions": []string{"cluster_view"}},
	} {
		resp := mcpTestRequest(t, router, http.MethodPut, "/api/kafka/clusters/"+clusterID+"/permissions", jwt, payload)
		if resp.Code != http.StatusBadRequest {
			t.Fatalf("save admin permission status = %d body=%s", resp.Code, resp.Body.String())
		}
	}

	userPermission := mcpTestRequest(t, router, http.MethodPut, "/api/kafka/clusters/"+clusterID+"/permissions", jwt, map[string]interface{}{"subject_type": "user", "username": "alice", "actions": []string{"cluster_view"}})
	if userPermission.Code != http.StatusOK {
		t.Fatalf("save user permission status = %d body=%s", userPermission.Code, userPermission.Body.String())
	}

	list := mcpTestRequest(t, router, http.MethodGet, "/api/kafka/clusters/"+clusterID+"/permissions", jwt, nil)
	var listBody struct {
		Data struct {
			Items []struct {
				SubjectType string   `json:"subject_type"`
				Subject     string   `json:"subject"`
				Actions     []string `json:"actions"`
				ReadOnly    bool     `json:"read_only"`
			} `json:"items"`
		} `json:"data"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &listBody); err != nil {
		t.Fatalf("decode permissions: %v", err)
	}
	adminRows := 0
	for _, item := range listBody.Data.Items {
		if item.Subject != "admin" {
			continue
		}
		adminRows++
		if !item.ReadOnly || len(item.Actions) != len(model.AllActions) {
			t.Fatalf("admin permission should be read-only with all actions: %+v", item)
		}
	}
	if adminRows != 2 {
		t.Fatalf("expected admin user and admin role rows, got %d: %s", adminRows, list.Body.String())
	}

	var count int64
	db.Model(&model.KafkaPermission{}).Where("cluster_id = ? AND username = ?", clusterID, "admin").Count(&count)
	if count != 0 {
		t.Fatalf("admin permission rows should not be created, got %d", count)
	}
}

func adminTestStore(t *testing.T) (*gorm.DB, config.Config) {
	t.Helper()
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
	return db, cfg
}

func adminTestLogin(t *testing.T, cfg config.Config, db *gorm.DB) (http.Handler, string) {
	t.Helper()
	router := NewRouter(cfg, db)
	login := mcpTestRequest(t, router, http.MethodPost, "/api/auth/login", "", map[string]interface{}{"username": "admin", "password": "admin"})
	if login.Code != http.StatusOK {
		t.Fatalf("login status = %d body=%s", login.Code, login.Body.String())
	}
	var body struct {
		Data struct {
			AccessToken string `json:"access_token"`
		} `json:"data"`
	}
	if err := json.Unmarshal(login.Body.Bytes(), &body); err != nil || body.Data.AccessToken == "" {
		t.Fatalf("login token missing: %v %s", err, login.Body.String())
	}
	return router, body.Data.AccessToken
}
