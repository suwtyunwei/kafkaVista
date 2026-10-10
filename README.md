# KafkaVista

KafkaVista 是面向企业级 Kafka 运维场景的统一管理平台，聚焦集群治理、Topic 与消息检索、Consumer Group 观测、细粒度授权、统一身份认证以及监控告警闭环。平台以轻量化部署为基础，提供清晰的 Web 控制台和标准化 API，帮助团队把 Kafka 日常运维、权限管控和风险发现集中到一个可审计、可扩展的入口。

## 核心功能

### 集群管理

- **多集群统一纳管**：一套平台管理多个 Kafka 集群，支持 SSL / SASL / SCRAM 认证接入。
- **Broker 与 Topic 全景视图**：实时查看 Broker 状态、Topic 分区分布、Leader 副本与 LogDirs 信息。
- **Topic 生命周期管理**：创建 / 删除 Topic、动态调整分区数与副本数、在线修改 Topic 级别配置。

### 消息检索

- **多维消息查询**：按时间范围、Key / Value 关键字、单分区 Offset 及 Offset 区间精确检索消息。
- **实时消息流**：SSE 长连接推送消费到的消息，无需手动刷新。
- **批量消息写入**：支持向指定 Topic 批量发送消息，便于功能验证与压测造数。

### 消费组治理

- **消费组监控**：查看消费组成员、消费位移（Current Offset）与积压（Lag）指标。
- **位移管理**：重置消费位移到 earliest / latest / 指定 Offset，支持按分区精确重置。
- **消费组删除**：清理无效消费组，保持集群整洁。

### 权限与审计

- **RBAC 细粒度授权**：用户级与角色级双轨授权，按集群维度隔离，11 个原子权限动作可灵活组合。
- **统一身份认证**：内置 LDAP / OIDC（Keycloak）单点登录，支持 JWT Token 签发与续签。
- **全链路操作审计**：所有变更类操作（Topic 增删改、消息发送、消费组位移重置、用户 / 角色变更等）自动记录到审计日志，支持按操作者、动作、集群、时间范围检索。

### 监控告警

- **Prometheus 指标导出**：原生 `/metrics` 端点，输出 Broker、Topic 分区、消费组 Lag 等核心指标，兼容 Grafana 仪表盘。
- **内置告警引擎**：9 条开箱即用的告警规则（消费组积压、Broker 离线、Under-Replicated Partition、Offline Partition 等），支持 60s 冷却去重。
- **多渠道通知**：钉钉 Markdown、飞书 Interactive Card、SMTP 邮件、外部 Webhook，支持通知测试。

### Kafka 平滑迁移

- **集群到集群迁移**：源集群 → 目集群全量复制 Topic 元数据（分区、副本、配置）与消息数据。
- **实时增量同步**：全量迁移完成后切换为增量模式，持续追平源端最新消息。
- **限速与分批**：可配置批大小、批间隔、每 Topic 最大消息数，降低对源端与目标端 Kafka 的性能冲击。
- **迁移进度可视化**：前端实时轮询迁移任务状态，查看各 Topic 复制进度与异常告警。

> 迁移说明：Kafka 不允许生产端指定目标 offset，因此迁移后目标 offset 不保证与源端完全一致。

## 快速开始

项目根目录提供一键安装脚本，自动检测环境并选择最佳部署方式：

**Linux / macOS：**

```bash
chmod +x install.sh
./install.sh              # 自动检测环境，选择最佳部署方式
./install.sh --docker     # Docker Compose 部署
./install.sh --source     # 源码编译部署
./install.sh --dev        # 本地开发模式（热重载）
./install.sh --help       # 查看帮助
```

**Windows PowerShell：**

```powershell
.\install.ps1                          # 自动检测
.\install.ps1 -Mode docker              # Docker Compose
.\install.ps1 -Mode source              # 源码编译
.\install.ps1 -Mode dev                # 开发模式
.\install.ps1 -KafkaServers "broker:9092" -AdminPassword "mypass"
```

脚本会自动：检测 Go/Node/Docker 版本 → 生成 JWT 密钥 → 创建 `.env` 配置 → 构建并启动服务 → 健康检查 → 输出访问地址。

> 如需手动部署，请参考下方[部署](#部署)章节。

## 平台截图

<table>
  <tr>
    <td width="50%" align="center"><b>登录页</b></td>
    <td width="50%" align="center"><b>Kafka 集群列表</b></td>
  </tr>
  <tr>
    <td><img src="docs/images/login.png" alt="登录页" /></td>
    <td><img src="docs/images/cluster-list.png" alt="集群列表" /></td>
  </tr>
  <tr>
    <td colspan="2" align="center"><b>Kafka 平滑迁移</b></td>
  </tr>
  <tr>
    <td colspan="2"><img src="docs/images/migration.png" alt="Kafka 平滑迁移" /></td>
  </tr>
</table>

## 源码目录

| 路径 | 说明 |
|---|---|
| `install.sh` | 一键安装脚本（Linux / macOS） |
| `install.ps1` | 一键安装脚本（Windows） |
| `docker-compose.yml` | Docker Compose 编排（Web `3004`、API `8080`） |
| `app/server/` | Go 1.26 + Gin 后端 API |
| `app/web/` | Vue 3 + Vite 前端 |
| `app/server/Dockerfile` | 后端镜像构建 |
| `app/web/Dockerfile` | 前端镜像构建 |
| `app/web/nginx.conf` | 前端 Nginx 配置（含 API 代理） |

## 环境要求

- Go ≥ 1.26（见 `app/server/go.mod`）
- Node ≥ 18、npm ≥ 9
- Docker ≥ 20.10（使用 Docker 部署时）
- Docker Compose ≥ 2.0（使用 Compose 部署时）

---

## 部署

### 方式一：Docker Compose 部署（推荐）

**步骤 1：创建 `.env` 配置文件（可选）**

在项目根目录创建 `.env` 文件，按需修改配置：

```bash
# 在项目根目录执行
cat > .env <<'EOF'
JWT_SECRET=change-me-in-production
AUTH_DEFAULT_USER=admin
AUTH_DEFAULT_PASSWORD=admin
KAFKA_CLUSTER_NAME=default
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
CORS_ALLOWED_ORIGINS=
EOF
```

**步骤 2：创建数据目录**

```bash
mkdir -p /data/local/kafkaVista/data
```

**步骤 3：拉取镜像并启动**

```bash
# 在项目根目录执行（docker-compose.yml 在根目录）
docker compose pull
docker compose up -d --remove-orphans
```

> 镜像从阿里云容器镜像服务（ACR）拉取，无需本地构建。如需从源码构建镜像，请参考[方式二](#方式二docker-镜像手动构建)。

**步骤 4：访问**

| 服务 | 地址 | 说明 |
|---|---|---|
| 前端 Web | `http://localhost:3004` | Nginx 托管前端静态文件 |
| 后端 API | `http://localhost:8080` | Go 后端 + SQLite |
| 默认账号 | `admin` / `admin` | 首次登录后请修改密码 |

数据持久化在宿主机 `/data/local/kafkaVista/data` 目录，SQLite 数据库文件为 `kafkavista.db`。

**环境变量配置**（在 `.env` 文件或 `docker compose` 命令中设置）：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `JWT_SECRET` | `change-me-in-production` | JWT 签名密钥，**生产环境必须修改** |
| `AUTH_DEFAULT_USER` | `admin` | 默认管理员用户名 |
| `AUTH_DEFAULT_PASSWORD` | `admin` | 默认管理员密码 |
| `KAFKA_CLUSTER_NAME` | `default` | 初始 Kafka 集群名称 |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | 初始 Kafka 集群地址 |
| `KAFKA_REQUEST_TIMEOUT_MS` | `6000` | Kafka 请求超时（毫秒） |
| `KAFKA_MAX_POLL_RECORDS` | `50000` | 单次拉取最大记录数 |
| `KAFKA_MAX_VALUE_LENGTH` | `1048576` | 单条消息最大值长度（字节） |
| `CORS_ALLOWED_ORIGINS` | （空） | CORS 白名单，逗号分隔，默认允许 `localhost:3000` |
| `EMAIL_SMTP_HOST` | （空） | 邮件告警 SMTP 主机 |
| `EMAIL_SMTP_PORT` | `587` | 邮件告警 SMTP 端口 |
| `EMAIL_SMTP_USERNAME` | （空） | 邮件告警用户名 |
| `EMAIL_SMTP_PASSWORD` | （空） | 邮件告警密码 |
| `EMAIL_FROM` | （空） | 发件人地址 |
| `EMAIL_TO` | （空） | 收件人地址 |
| `EMAIL_USE_TLS` | `false` | 是否启用 TLS |

**日志与停止：**

```bash
docker compose logs -f                    # 查看所有服务日志
docker compose logs -f kafkavista-server   # 仅后端日志
docker compose down                       # 停止并移除容器
docker compose pull && docker compose up -d  # 拉取最新镜像并重启
```

### 方式二：Docker 镜像手动构建

**步骤 1：构建后端镜像**

```bash
docker build -t kafkavista-server:latest -f app/server/Dockerfile app/server/
```

**步骤 2：构建前端镜像**

```bash
docker build -t kafkavista-web:latest -f app/web/Dockerfile app/web/
```

**步骤 3：创建 Docker 网络**

```bash
docker network create kafkavista-net
```

**步骤 4：启动后端容器**

```bash
docker run -d \
  --name kafkavista-server \
  --restart unless-stopped \
  --network kafkavista-net \
  -p 8080:8080 \
  -v /data/local/kafkaVista/data:/app/data \
  -e JWT_SECRET="your-secret-key" \
  -e AUTH_DEFAULT_USER=admin \
  -e AUTH_DEFAULT_PASSWORD=admin \
  -e KAFKA_BOOTSTRAP_SERVERS=broker1:9092 \
  crpi-y1il03c3mx8rejur.cn-hangzhou.personal.cr.aliyuncs.com/aisoftkit/kafkavista-server:latest
```

**步骤 5：启动前端容器**

```bash
docker run -d \
  --name kafkavista-web \
  --restart unless-stopped \
  --network kafkavista-net \
  -p 3004:80 \
  crpi-y1il03c3mx8rejur.cn-hangzhou.personal.cr.aliyuncs.com/aisoftkit/kafkavista-web:latest
```

> 前端 Nginx 配置（`app/web/nginx.conf`）将 `/api` 和 `/kafka-api` 代理到 `http://kafkavista-server:8080`。使用 `--network` 确保容器间互通。

### 方式三：源码编译部署

**步骤 1：Go 后端编译**

```bash
cd app/server

# 下载依赖
export GOPROXY=https://goproxy.cn,direct
export GOSUMDB=sum.golang.google.cn
go mod download

# 编译二进制（Linux）
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o kafkavista ./cmd/kafkavista

# 编译二进制（macOS Apple Silicon）
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o kafkavista ./cmd/kafkavista

# 编译二进制（Windows PowerShell）
$env:CGO_ENABLED=0; $env:GOOS="windows"; $env:GOARCH="amd64"
go build -trimpath -ldflags="-s -w" -o kafkavista.exe ./cmd/kafkavista
```

**步骤 2：Node 前端编译**

```bash
cd app/web

# 安装依赖
npm install

# 生产构建（输出到 dist/）
npm run build
```

构建产物在 `app/web/dist/` 目录。

**步骤 3：启动后端**

```bash
# 创建数据目录
mkdir -p /app/data

# 设置环境变量（JWT_SECRET 必填）
export HTTP_ADDR=:8080
export KAFKA_DATABASE_PATH=/app/data/kafkavista.db
export JWT_SECRET=your-secret-key
export AUTH_DEFAULT_PASSWORD=admin

# 启动
./kafkavista
```

**步骤 4：用 Nginx 托管前端**

将 `app/web/dist/` 目录部署到 Nginx，配置如下：

```nginx
server {
    listen 80;
    root /usr/share/nginx/html;
    index index.html;

    location / {
        try_files $uri $uri/ /index.html;
    }

    location /api {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    location /kafka-api {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
    }

    location /metrics {
        proxy_pass http://127.0.0.1:8080;
    }
}
```

---

## 本地开发

后端热重载：

```bash
cd app/server
GOPROXY=https://goproxy.cn,direct GOSUMDB=sum.golang.google.cn go run ./cmd/kafkavista
```

指定后端端口：

```bash
./kafkavista --port 8081
./kafkavista --addr :8081
HTTP_ADDR=:8081 ./kafkavista
```

前端热重载：

```bash
cd app/web
npm install
npm run dev
```

前端开发服务默认监听 `http://localhost:3000`，接口通过 Vite 代理到 `http://localhost:8080`。

---

## 关注公众号

扫码关注公众号，获取项目更新与使用交流。

<img src="docs/images/wechat-qrcode.jpg" alt="关注公众号" width="220" />

