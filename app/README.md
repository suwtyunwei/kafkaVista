# KafkaVista Wiki / 使用文档

KafkaVista is an enterprise-oriented Kafka operations platform built with Go + Gin and Vue 3 + Vite. It provides a unified console for Kafka cluster governance, topic and message inspection, consumer group observability, fine-grained authorization, identity integration, and alerting workflows.

KafkaVista 是面向企业级 Kafka 运维场景的统一管理平台，后端使用 Go + Gin，前端使用 Vue 3 + Vite。平台聚合集群治理、Topic 与消息检索、Consumer Group 观测、细粒度授权、统一身份认证和监控告警能力，帮助团队以更低成本完成 Kafka 日常运维、权限管控和风险发现。

## Contents / 目录

- [Overview / 概览](#overview--概览)
- [Feature List / 功能列表](#feature-list--功能列表)
- [Deployment / 部署](#deployment--部署)
- [Environment Variables / 环境变量](#environment-variables--环境变量)
- [Login And SSO / 登录与单点登录](#login-and-sso--登录与单点登录)
- [LDAP Configuration / LDAP 配置](#ldap-configuration--ldap-配置)
- [OIDC Keycloak Configuration / OIDC Keycloak 配置](#oidc-keycloak-configuration--oidc-keycloak-配置)
- [Monitoring And Alerting / 监控与告警](#monitoring-and-alerting--监控与告警)
- [Rule Config File / 规则配置文件](#rule-config-file--规则配置文件)
- [Notification Channels / 通知渠道](#notification-channels--通知渠道)
- [Full Edition Token API / 完整版 Token 接口](#full-edition-token-api--完整版-token-接口)
- [Common Issues / 常见问题](#common-issues--常见问题)

## Overview / 概览

### Modules / 模块

| Path / 路径 | Description / 说明 |
| --- | --- |
| `server/` | Go/Gin backend API. 默认监听 `:8080`。 |
| `web/` | Vue frontend. 开发服务默认监听 `3000`。 |
| `data/` | SQLite runtime data directory. SQLite 运行数据目录。 |

### Default Ports / 默认端口

| Edition / 版本 | Web / 前端 | API / 后端 |
| --- | --- | --- |
| Full Edition / 完整版 | `http://服务器IP:3004` | `http://服务器IP:8080` |

## Feature List / 功能列表

- Kafka cluster management / Kafka 集群连接管理：Full Edition has no instance limit; Community Edition can manage up to 10 Kafka instances. 完整版不限实例数量；社区版最多管理 10 个 Kafka 实例。
- Topic management / Topic 管理：topic list, partitions, replicas, message count, storage size, and metadata. 查看 Topic、分区、副本、消息量、存储大小和元数据。
- Message query and pull / 消息查询与拉取：pull messages by topic and view Key, Value, Header; search by time, keyword, start offset, or inclusive offset range. 支持按 Topic 拉取消息并查看 Key、Value、Header；支持按时间、关键字、起始 offset 或包含结束边界的 offset 范围检索。
- Consumer Group management / 消费组管理：groups, members, offsets, lag, and consume status. 查看消费组、成员、offset、lag 和消费状态。
- Kafka authorization / Kafka 授权：control accessible clusters, topics, and consumer groups by user or role. 按用户或角色控制可访问的集群、Topic 和消费组。
- Users and roles / 用户与角色：local users, roles, permissions, and LDAP/OIDC role assignment. 支持本地用户、角色、权限和 LDAP/OIDC 用户角色分配。
- LDAP login / LDAP 登录：connection test, user sync, and LDAP password login. 支持连接测试、用户同步和 LDAP 密码登录。
- OIDC / Keycloak：OIDC SSO, automatic user creation, and role claim mapping. 支持 OIDC 单点登录、自动创建用户和角色 Claim 映射。
- Prometheus metrics / Prometheus 指标：`/metrics` endpoint for Kafka metrics. 提供 `/metrics` 指标出口。
- Monitoring alerts / 监控告警：consumer lag, broker offline, under-replicated partitions, offline partitions, topic count, log size, message rate drop, empty consumer members, and broker resource usage. 内置消费积压、Broker 离线、副本不足、离线分区、Topic 数量、日志大小、消息速率下降、消费组成员为空和 Broker 资源使用率规则。
- Notifications / 通知渠道：DingTalk, Feishu, and SMTP email with test sending. 支持钉钉、飞书和 SMTP 邮件通知，并支持测试发送。
- Alert rule config / 告警规则配置文件：edit and restore built-in alert rules in the UI. 支持页面编辑和恢复内置告警规则。
- UI settings / 界面设置：Chinese/English language switch and light/dark theme. 支持中英文切换和亮色/暗色主题。

Notes / 备注：

- Full Edition License supports token activation, token generation, and expiration inspection.
- 完整版 License 支持完整版 Token 激活、生成和查询有效期。

## Deployment / 部署

### Docker Compose Deployment / Docker Compose 部署

```bash
docker compose pull
docker compose up -d --remove-orphans
```

### Local Development / 本地开发

Backend / 后端：

```bash
cd server
go run ./cmd/kafkavista
```

Frontend / 前端：

```bash
cd web
npm install
npm run dev
```

Default admin account: `admin` / `admin`.

默认管理员账号：`admin` / `admin`。

Production environments must set `AUTH_DEFAULT_PASSWORD` and `JWT_SECRET`.

生产环境必须设置 `AUTH_DEFAULT_PASSWORD` 和 `JWT_SECRET`。

## Environment Variables / 环境变量

| Variable / 变量 | Default / 默认值 | Description / 说明 |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | Backend listen address. 后端监听地址。 |
| `KAFKA_DATABASE_PATH` | `./data/kafkavista.db` | SQLite database path. SQLite 数据文件路径。 |
| `JWT_SECRET` | built-in fallback | JWT signing secret. JWT 签名密钥。 |
| `JWT_EXPIRE_HOURS` | `24` | Access token expiration hours. 登录 Token 有效小时数。 |
| `AUTH_DEFAULT_USER` | `admin` | Default admin username. 默认管理员用户名。 |
| `AUTH_DEFAULT_PASSWORD` | `admin` | Default admin password. 默认管理员密码。 |
| `LICENSE_ADMIN_TOKEN` | empty / 空 | Admin secret for generating Full Edition tokens. 完整版授权 Token 生成接口管理密钥。 |
| `KAFKA_CLUSTER_NAME` | `default` | Default Kafka cluster name. 默认 Kafka 集群名称。 |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Default Kafka bootstrap servers. 默认 Kafka 地址。 |
| `KAFKA_REQUEST_TIMEOUT_MS` | `6000` | Kafka request timeout. Kafka 请求超时。 |
| `KAFKA_MAX_POLL_RECORDS` | `50000` | Maximum poll records. 最大拉取消息数。 |
| `KAFKA_MAX_VALUE_LENGTH` | `1048576` | Maximum displayed value length. 最大消息内容长度。 |
| `KAFKA_MAX_RESPONSE_BYTES` | `10485760` | Maximum Kafka response bytes. 最大 Kafka 响应大小。 |
| `EMAIL_SMTP_HOST` | empty / 空 | SMTP host for email alerts. 邮件告警 SMTP 服务器。 |
| `EMAIL_SMTP_PORT` | `587` | SMTP port. SMTP 端口。 |
| `EMAIL_SMTP_USERNAME` | empty / 空 | SMTP username. SMTP 用户名。 |
| `EMAIL_SMTP_PASSWORD` | empty / 空 | SMTP password. SMTP 密码。 |
| `EMAIL_FROM` | empty / 空 | Sender address. 发件人地址。 |
| `EMAIL_TO` | empty / 空 | Recipient list, comma-separated. 收件人地址，多个用逗号分隔。 |
| `EMAIL_USE_TLS` | `false` | Use implicit SSL/TLS. 465 端口通常设为 `true`，587 端口通常保持 `false`。 |

Example `.env` / `.env` 示例：

```env
JWT_SECRET=change-this-secret
AUTH_DEFAULT_USER=admin
AUTH_DEFAULT_PASSWORD=change-this-password
LICENSE_ADMIN_TOKEN=change-this-license-admin-token
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

## Login And SSO / 登录与单点登录

### Local Admin Login / 本地管理员登录

The default admin user can always log in with username and password.

默认管理员始终可以使用用户名密码登录。

### LDAP And OIDC Together / LDAP 与 OIDC 同时开启

LDAP and OIDC are mutually exclusive in settings. Enable one or the other, not both. The local `admin` user still uses local password login.

LDAP 和 OIDC 在设置里互斥，只能启用一个。本地 `admin` 仍走本地密码登录。

### OIDC User Auto Provisioning / OIDC 用户自动新增

After a successful OIDC callback, KafkaVista automatically creates or updates the user with `source=oidc`.

OIDC 回调成功后，KafkaVista 会自动新增或更新 `source=oidc` 的用户。

Username claim fallback order:

用户名 Claim 取值顺序：

1. Configured `username_claim` / 配置的 `username_claim`
2. `preferred_username`
3. `email`
4. `sub`

OIDC callback URL must point to the maintained Full Edition service (`3004` / `8080`). Keycloak must allow the Full Edition callback URL.

OIDC 回调地址必须指向当前维护的完整版服务（`3004` / `8080`），Keycloak 必须允许完整版回调地址。

## LDAP Configuration / LDAP 配置

Open `Settings -> Basic -> LDAP Settings`.

打开 `系统设置 -> 基础设置 -> LDAP 设置`。

| Field / 字段 | Description / 说明 |
| --- | --- |
| LDAP URL | Example: `ldap://ldap.example.com:389`. LDAP 服务地址。 |
| Bind DN | LDAP bind account DN. LDAP 绑定账号 DN。 |
| Bind Password | LDAP bind password. LDAP 绑定账号密码。 |
| Base DN | User search base DN. 用户搜索根 DN。 |
| User Filter | Example: `(uid=%s)`. `%s` is replaced by username. `%s` 会替换为用户名。 |
| Display Name Attribute | Example: `cn` or `sn`. 显示名属性。 |
| Email Attribute | Example: `mail`. 邮箱属性。 |
| StartTLS | Enable only when LDAP server supports it. 仅 LDAP 服务支持时开启。 |

Actions / 操作：

- `Test Connection / 测试连接`: verifies LDAP connection and search.
- `Sync Users / 一键拉取用户`: imports LDAP users into KafkaVista user list.

## OIDC Keycloak Configuration / OIDC Keycloak 配置

Open `Settings -> Basic -> OIDC / Keycloak Settings`.

打开 `系统设置 -> 基础设置 -> OIDC / Keycloak 设置`。

| Field / 字段 | Example / 示例 | Description / 说明 |
| --- | --- | --- |
| Issuer URL | `https://keycloak.example.com/realms/master` | OIDC issuer. 必须能访问 `/.well-known/openid-configuration`。 |
| Redirect URL | `http://host:3004/api/auth/oidc/callback` | Callback URL registered in Keycloak. 必须在 Keycloak client 中登记。 |
| Client ID | `kafka` | Keycloak client ID. |
| Client Secret | `******` | Keycloak client secret. 留空保存时表示不修改。 |
| Scopes | `openid profile email` | OIDC scopes. |
| Username Claim | `preferred_username` | Username field from userinfo. 用户名字段。 |
| Role Claim | `roles` | Optional role claim. 可选角色字段。 |
| Admin Roles | `admin,kafkavista-admin` | Users with these roles become KafkaVista admin. 命中这些角色会成为管理员。 |

### Keycloak Required Settings / Keycloak 必要配置

- Client must enable Authorization Code flow.
- Valid Redirect URIs must include KafkaVista callback URL.
- Web Origins should include KafkaVista origin when required.
- If `redirect_uri` is invalid, Keycloak returns `Invalid parameter: redirect_uri`.

- Client 需要启用 Authorization Code Flow。
- Valid Redirect URIs 必须包含 KafkaVista 回调地址。
- 必要时 Web Origins 需要包含 KafkaVista 域名。
- 如果回调地址未登记，Keycloak 会返回 `无效的参数: redirect_uri`。

Example callback URLs / 回调地址示例：

```text
http://localhost:3004/api/auth/oidc/callback
```

## Monitoring And Alerting / 监控与告警

KafkaVista keeps Prometheus exporter and alerting settings. The realtime Monitor chart page has been removed.

KafkaVista 保留 Prometheus Exporter 和告警配置；实时 Monitor 趋势图页面已移除。

### Prometheus Exporter / Prometheus 指标导出

Open `Settings -> Monitoring Metrics`.

打开 `系统设置 -> 监控指标`。

| URL / 地址 | Description / 说明 |
| --- | --- |
| `/metrics` | Export metrics for all active clusters. 导出所有启用集群指标。 |
| `/metrics/:cluster` | Export metrics for one cluster by ID or name. 按集群 ID 或名称导出指标。 |

### Alerting / 告警

Open `Settings -> Monitoring Alerts`.

打开 `系统设置 -> 监控告警`。

Important behavior / 重要行为：

- Alerts are sent only when `Enable Monitoring Alerts / 开启监控告警` is enabled.
- If alerting is disabled, background scanning and test sending are blocked.
- Notification test buttons are disabled when alerting is disabled.

- 只有开启 `开启监控告警` 后才会发送告警。
- 未开启时，后台扫描和测试发送都会被阻止。
- 未开启时，通知渠道测试按钮会置灰。

Supported alert rules / 当前支持的规则：

| Rule Key / 规则 Key | Description / 说明 |
| --- | --- |
| `consumer_group_lag` | Total consumer group lag threshold. 消费组总积压阈值。 |
| `broker_offline` | Broker connection/offline alert. Broker 连接失败或可用数不足。 |
| `topic_partition_count` | Topic count threshold. Topic 数量阈值。 |
| `consumer_member_zero` | Consumer groups with zero members. 无成员消费组数量。 |

Configured but requiring additional exporter/Kafka metrics / 已配置但需要额外指标支持：

| Rule Key / 规则 Key | Description / 说明 |
| --- | --- |
| `under_replicated_partition` | Under replicated partitions. 副本不同步分区。 |
| `offline_partition` | Offline partitions. 离线分区。 |
| `topic_log_size` | Topic log size threshold. Topic 日志容量阈值。 |
| `message_rate_drop` | Message rate drop percentage. 消息速率下降比例。 |
| `broker_resource_usage` | Broker resource usage. Broker 资源使用率。 |

## Rule Config File / 规则配置文件

In `Monitoring Alerts`, click the small `Rule Config / 规则配置文件` button in the panel header.

在 `监控告警` 页，点击标题右侧的小按钮 `规则配置文件`。

The dedicated page is:

独立页面地址：

```text
/settings/alert-rules
```

JSON fields / JSON 字段：

| Field / 字段 | Type / 类型 | Description / 说明 |
| --- | --- | --- |
| `key` | string | Rule key. 规则唯一标识。 |
| `name` | string | Display name. 显示名。 |
| `enabled` | boolean | Enable this rule. 是否启用该规则。 |
| `threshold` | number | Rule threshold. 阈值。 |
| `unit` | string | Unit label. 单位。 |
| `direction` | string | Compare direction, usually `>=`. 比较方向，通常是 `>=`。 |

Example / 示例：

```json
[
  {
    "key": "consumer_group_lag",
    "name": "Consumer Group Lag",
    "enabled": true,
    "threshold": 10000,
    "unit": "messages",
    "direction": ">="
  }
]
```

Workflow / 操作流程：

1. Click `Rule Config / 规则配置文件`.
2. Edit JSON.
3. Click `Apply Config / 应用配置`.
4. Click `Save Settings / 保存设置`.

1. 点击 `规则配置文件`。
2. 编辑 JSON。
3. 点击 `应用配置`。
4. 点击 `保存设置`。

## Notification Channels / 通知渠道

Open `Settings -> Monitoring Alerts -> Notification Channels`.

打开 `系统设置 -> 监控告警 -> 通知渠道`。

| Channel / 渠道 | Payload / 发送格式 | Description / 说明 |
| --- | --- | --- |
| DingTalk / 钉钉 | Markdown | DingTalk custom robot webhook. 钉钉自定义机器人。 |
| Feishu / 飞书 | Interactive card | Feishu bot webhook. 飞书群机器人。 |
| Email Gateway / 邮件网关 | JSON with `subject`, `content`, `text`, `html` | External email gateway webhook. 外部邮件网关。 |

Test sending requires alerting to be enabled.

测试发送要求先开启监控告警。

## Full Edition Token API / 完整版 Token 接口

These APIs are independent and do not require user login. Generating tokens requires `LICENSE_ADMIN_TOKEN`.

这些接口是独立接口，不依赖用户登录。生成 Token 需要配置 `LICENSE_ADMIN_TOKEN`。

### Generate Full Edition Token / 生成完整版 Token

Request / 请求：

```bash
curl -X POST 'http://服务器IP:8080/api/license/tokens' \
  -H 'Content-Type: application/json' \
  -H 'X-License-Admin-Token: 你的LICENSE_ADMIN_TOKEN' \
  -d '{"days":365}'
```

You can also specify an expiration time directly:

也可以直接指定过期时间：

```bash
curl -X POST 'http://服务器IP:8080/api/license/tokens' \
  -H 'Content-Type: application/json' \
  -H 'X-License-Admin-Token: 你的LICENSE_ADMIN_TOKEN' \
  -d '{"expires_at":"2027-05-21T00:00:00Z"}'
```

Response / 返回：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "token": "KV-ENTERPRISE-...",
    "edition": "full",
    "expires_at": "2027-05-21T00:00:00Z",
    "valid": true
  }
}
```

### Inspect Token Expiration / 查询 Token 时间

Request / 请求：

```bash
curl 'http://服务器IP:8080/api/license/tokens/inspect?token=KV-ENTERPRISE-...'
```

Response / 返回：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "edition": "full",
    "raw_edition": "enterprise",
    "expires_at": "2027-05-21T00:00:00Z",
    "remaining_seconds": 31536000,
    "valid": true
  }
}
```

## Common Issues / 常见问题

### Keycloak returns `Invalid parameter: redirect_uri` / Keycloak 返回 `无效的参数: redirect_uri`

Reason: the callback URL is not registered in Keycloak client Valid Redirect URIs.

原因：KafkaVista 回调地址没有登记到 Keycloak client 的 Valid Redirect URIs。

Fix / 修复：

```text
http://你的服务器IP:3004/api/auth/oidc/callback
```

### OIDC login does not create user / OIDC 用户没有创建成功

Check whether the callback URL points to the maintained Full Edition service at `3004`.

检查 OIDC 回调地址是否指向当前维护的完整版服务 `3004`。

### Alert test button is disabled / 告警测试按钮置灰

Enable `Monitoring Alerts / 监控告警` first, then configure at least one webhook.

先开启 `监控告警`，再配置至少一个 webhook。

### The realtime Monitor chart page is missing / 实时 Monitor 趋势图页面不见了

This page has been intentionally removed. Prometheus exporter and alerting remain available.

实时 Monitor 趋势图页面已按需求移除，Prometheus 指标导出和监控告警仍保留。
