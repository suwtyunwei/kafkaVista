package api

import (
	"net/http"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
)

func apiDocs(router *gin.Engine, a *API) gin.HandlerFunc {
	return func(c *gin.Context) {
		routes := router.Routes()
		items := make([]gin.H, 0, len(routes))
		paths := gin.H{}
		enterprise := a.enterpriseEnabled()
		for _, route := range routes {
			if route.Path == "/api/docs" || route.Path == "/metrics" || strings.HasPrefix(route.Path, "/metrics/") {
				continue
			}
			if !enterprise && apiDocEnterpriseOnly(route.Path) {
				continue
			}
			group := apiDocGroup(route.Path)
			auth := apiDocAuth(route.Path)
			item := gin.H{"method": route.Method, "path": route.Path, "group": group, "auth": auth, "summary": apiDocSummary(route.Method, route.Path)}
			items = append(items, item)
			pathItem, _ := paths[route.Path].(gin.H)
			if pathItem == nil {
				pathItem = gin.H{}
				paths[route.Path] = pathItem
			}
			pathItem[strings.ToLower(route.Method)] = gin.H{"summary": item["summary"], "tags": []string{group}, "security": apiDocSecurity(auth)}
		}
		sort.SliceStable(items, func(i, j int) bool {
			left := items[i]["path"].(string) + items[i]["method"].(string)
			right := items[j]["path"].(string) + items[j]["method"].(string)
			return left < right
		})
		c.JSON(http.StatusOK, gin.H{"code": 0, "data": gin.H{"title": "kafkaVista API Docs", "version": "1.0.0", "items": items, "openapi": gin.H{"openapi": "3.0.3", "info": gin.H{"title": "kafkaVista API", "version": "1.0.0"}, "paths": paths, "components": gin.H{"securitySchemes": gin.H{"bearerAuth": gin.H{"type": "http", "scheme": "bearer", "bearerFormat": "JWT"}}}}}})
	}
}

func apiDocEnterpriseOnly(path string) bool {
	return strings.Contains(path, "/migrations")
}

func apiDocGroup(path string) string {
	switch {
	case strings.HasPrefix(path, "/api/auth"):
		return "Auth"
	case strings.HasPrefix(path, "/api/admin"):
		return "Admin"
	case strings.Contains(path, "/migrations"):
		return "Migration"
	case strings.Contains(path, "/audit-logs"):
		return "Audit"
	case strings.Contains(path, "/permissions"):
		return "Kafka Auth"
	case strings.Contains(path, "/clusters"):
		return "Kafka"
	case strings.HasPrefix(path, "/api/license"):
		return "License"
	case strings.HasPrefix(path, "/api/app") || strings.HasPrefix(path, "/api/health"):
		return "System"
	default:
		return "Other"
	}
}

func apiDocAuth(path string) string {
	if strings.HasPrefix(path, "/api/auth/login") || strings.HasPrefix(path, "/api/auth/sso") || strings.HasPrefix(path, "/api/auth/oidc") || strings.HasPrefix(path, "/api/app/status") || strings.HasPrefix(path, "/api/health") || strings.HasPrefix(path, "/api/license/tokens") {
		return "public"
	}
	if strings.HasPrefix(path, "/api/admin") || strings.Contains(path, "/migrations") || strings.Contains(path, "/audit-logs") {
		return "admin"
	}
	return "bearer"
}

func apiDocSecurity(auth string) []gin.H {
	if auth == "public" {
		return []gin.H{}
	}
	return []gin.H{{"bearerAuth": []string{}}}
}

func apiDocSummary(method, path string) string {
	switch {
	case strings.Contains(path, "/migrations"):
		return apiDocMethodName(method) + "迁移任务"
	case strings.Contains(path, "/audit-logs"):
		return apiDocMethodName(method) + "操作审计日志"
	case strings.Contains(path, "/permissions"):
		return apiDocMethodName(method) + "Kafka 权限配置"
	case strings.Contains(path, "/topics") && strings.Contains(path, "/data"):
		return apiDocMethodName(method) + "Topic 消息数据"
	case strings.Contains(path, "/topics") && strings.Contains(path, "/configs"):
		return apiDocMethodName(method) + "Topic 配置"
	case strings.Contains(path, "/topics"):
		return apiDocMethodName(method) + "Kafka Topic"
	case strings.Contains(path, "/groups"):
		return apiDocMethodName(method) + "Consumer Group"
	case strings.Contains(path, "/messages"):
		return apiDocMethodName(method) + "Kafka 消息"
	case strings.Contains(path, "/clusters"):
		return apiDocMethodName(method) + "Kafka 实例"
	case strings.HasPrefix(path, "/api/auth/login"):
		return "账号登录"
	case strings.HasPrefix(path, "/api/auth/sso"):
		return "获取单点登录配置"
	case strings.HasPrefix(path, "/api/auth/oidc"):
		return "OIDC 登录回调"
	case strings.HasPrefix(path, "/api/admin/settings"):
		return apiDocMethodName(method) + "系统设置"
	case strings.HasPrefix(path, "/api/admin/users"):
		return apiDocMethodName(method) + "系统用户"
	case strings.HasPrefix(path, "/api/admin/roles"):
		return apiDocMethodName(method) + "系统角色"
	case strings.HasPrefix(path, "/api/license"):
		return apiDocMethodName(method) + "License"
	case strings.HasPrefix(path, "/api/app/status"):
		return "获取应用状态"
	case strings.HasPrefix(path, "/api/health"):
		return "健康检查"
	default:
		name := strings.Trim(path, "/")
		name = strings.ReplaceAll(name, "/", " ")
		name = strings.ReplaceAll(name, ":", "")
		if name == "" {
			name = "根路径"
		}
		return apiDocMethodName(method) + name
	}
}

func apiDocMethodName(method string) string {
	switch method {
	case http.MethodGet:
		return "查询"
	case http.MethodPost:
		return "创建"
	case http.MethodPut:
		return "更新"
	case http.MethodDelete:
		return "删除"
	default:
		return method + " "
	}
}
