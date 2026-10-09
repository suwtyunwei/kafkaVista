#!/usr/bin/env bash
# KafkaVista 一键安装脚本 (Linux / macOS)
# 用法: ./install.sh [--docker | --source | --dev]
#   --docker  使用 Docker Compose 部署（默认，如 Docker 可用）
#   --source  从源码编译部署
#   --dev     本地开发模式（前后端热重载）

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$SCRIPT_DIR/app"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info()    { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; }
step()    { echo -e "${BLUE}[STEP]${NC} $*"; }

# 默认参数
MODE=""
DATA_DIR="/data/local/kafkaVista/data"
WEB_PORT=3004
API_PORT=8080
JWT_SECRET="${JWT_SECRET:-$(openssl rand -hex 32 2>/dev/null || echo "change-me-$(date +%s)")}"

# 解析参数
while [[ $# -gt 0 ]]; do
  case $1 in
    --docker)  MODE="docker";  shift;;
    --source)  MODE="source";  shift;;
    --dev)     MODE="dev";     shift;;
    --help|-h)
      echo "KafkaVista 一键安装脚本"
      echo ""
      echo "用法: ./install.sh [选项]"
      echo ""
      echo "选项:"
      echo "  --docker   使用 Docker Compose 部署（默认）"
      echo "  --source   从源码编译部署"
      echo "  --dev      本地开发模式（前后端热重载）"
      echo "  --help     显示帮助信息"
      echo ""
      echo "环境变量:"
      echo "  JWT_SECRET              JWT 签名密钥（自动生成）"
      echo "  AUTH_DEFAULT_PASSWORD   默认管理员密码（默认 admin）"
      echo "  KAFKA_BOOTSTRAP_SERVERS Kafka 集群地址（默认 localhost:9092）"
      echo "  DATA_DIR                数据目录（默认 $DATA_DIR）"
      exit 0
      ;;
    *) error "未知参数: $1"; exit 1;;
  esac
done

# ============================================================
# 检测工具
# ============================================================
has() { command -v "$1" >/dev/null 2>&1; }

check_docker() {
  if has docker; then
    if docker compose version >/dev/null 2>&1; then
      COMPOSE=(docker compose)
      return 0
    elif has docker-compose; then
      COMPOSE=(docker-compose)
      return 0
    fi
  fi
  return 1
}

check_go() {
  if ! has go; then
    return 1
  fi
  local ver
  ver=$(go version 2>/dev/null | grep -oP 'go\K[0-9]+\.[0-9]+' || echo "0")
  local major minor
  IFS='.' read -r major minor <<< "$ver"
  if [[ "$major" -lt 1 || ("$major" -eq 1 && "$minor" -lt 26) ]]; then
    warn "Go 版本过低: go$ver，需要 >= 1.26"
    return 1
  fi
  return 0
}

check_node() {
  if ! has node; then return 1; fi
  local ver
  ver=$(node --version 2>/dev/null | grep -oP 'v\K[0-9]+' || echo "0")
  if [[ "$ver" -lt 18 ]]; then
    warn "Node 版本过低: v$ver，需要 >= 18"
    return 1
  fi
  return 0
}

# ============================================================
# 自动选择部署模式
# ============================================================
if [[ -z "$MODE" ]]; then
  if check_docker; then
    MODE="docker"
    info "检测到 Docker，将使用 Docker Compose 部署"
  elif check_go && check_node; then
    MODE="source"
    info "检测到 Go + Node，将从源码编译部署"
  else
    error "未检测到 Docker 或 Go+Node 环境，无法自动部署"
    echo ""
    echo "请安装以下任一组合："
    echo "  1. Docker + Docker Compose（推荐）"
    echo "     https://docs.docker.com/get-docker/"
    echo "  2. Go >= 1.26 + Node >= 18"
    echo "     https://go.dev/dl/"
    echo "     https://nodejs.org/"
    exit 1
  fi
fi

# ============================================================
# Docker Compose 部署
# ============================================================
install_docker() {
  step "Docker Compose 部署"
  cd "$SCRIPT_DIR"

  # 创建数据目录
  info "创建数据目录: $DATA_DIR"
  sudo mkdir -p "$DATA_DIR" 2>/dev/null || mkdir -p "$DATA_DIR" 2>/dev/null || true
  sudo chown -R "$(id -u):$(id -g)" "$DATA_DIR" 2>/dev/null || true

  # 创建 .env
  if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
    info "生成 .env 配置文件"
    cat > "$SCRIPT_DIR/.env" <<EOF
# KafkaVista 配置
JWT_SECRET=$JWT_SECRET
AUTH_DEFAULT_USER=admin
AUTH_DEFAULT_PASSWORD=${AUTH_DEFAULT_PASSWORD:-admin}
KAFKA_CLUSTER_NAME=${KAFKA_CLUSTER_NAME:-default}
KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}

# Kafka 调优
KAFKA_REQUEST_TIMEOUT_MS=6000
KAFKA_MAX_POLL_RECORDS=50000
KAFKA_MAX_VALUE_LENGTH=1048576
KAFKA_MAX_RESPONSE_BYTES=10485760

# 邮件告警（可选）
EMAIL_SMTP_HOST=${EMAIL_SMTP_HOST:-}
EMAIL_SMTP_PORT=${EMAIL_SMTP_PORT:-587}
EMAIL_SMTP_USERNAME=${EMAIL_SMTP_USERNAME:-}
EMAIL_SMTP_PASSWORD=${EMAIL_SMTP_PASSWORD:-}
EMAIL_FROM=${EMAIL_FROM:-}
EMAIL_TO=${EMAIL_TO:-}
EMAIL_USE_TLS=${EMAIL_USE_TLS:-false}
CORS_ALLOWED_ORIGINS=${CORS_ALLOWED_ORIGINS:-}
EOF
    info ".env 已生成，请按需修改后重新运行"
  fi

  # 构建并启动
  step "构建镜像并启动服务..."
  "${COMPOSE[@]}" up -d --build --remove-orphans

  # 等待服务就绪
  step "等待后端服务就绪..."
  local retries=30
  while [[ $retries -gt 0 ]]; do
    if curl -sf "http://localhost:$API_PORT/api/health" >/dev/null 2>&1; then
      info "后端服务已就绪"
      break
    fi
    retries=$((retries - 1))
    sleep 2
  done
  if [[ $retries -eq 0 ]]; then
    warn "后端服务未在 60 秒内就绪，请检查日志: ${COMPOSE[*]} logs kafkavista-server"
  fi

  print_result
}

# ============================================================
# 源码编译部署
# ============================================================
install_source() {
  step "源码编译部署"

  # 检查依赖
  if ! check_go; then
    error "Go >= 1.26 未安装"
    echo "  安装: https://go.dev/dl/"
    exit 1
  fi
  if ! check_node; then
    error "Node >= 18 未安装"
    echo "  安装: https://nodejs.org/"
    exit 1
  fi
  if ! has npm; then
    error "npm 未安装"
    exit 1
  fi

  info "Go: $(go version)"
  info "Node: $(node --version)"

  # 创建数据目录
  local local_data="$APP_DIR/data"
  mkdir -p "$local_data"
  info "数据目录: $local_data"

  # 编译后端
  step "编译后端..."
  (
    cd "$APP_DIR/server"
    export GOPROXY="${GOPROXY:-https://goproxy.cn,direct}"
    export GOSUMDB="${GOSUMDB:-sum.golang.google.cn}"
    go mod download
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o kafkavista ./cmd/kafkavista
  )
  info "后端编译完成: app/server/kafkavista"

  # 编译前端
  step "编译前端..."
  (
    cd "$APP_DIR/web"
    npm install --no-fund --no-audit
    npm run build
  )
  info "前端构建完成: app/web/dist/"

  # 启动服务
  step "启动服务..."

  # 停止旧进程
  if [ -f "$SCRIPT_DIR/.kafkavista.pid" ]; then
    local old_pid
    old_pid=$(cat "$SCRIPT_DIR/.kafkavista.pid" 2>/dev/null || echo "")
    if [ -n "$old_pid" ] && kill -0 "$old_pid" 2>/dev/null; then
      warn "停止旧进程 (PID: $old_pid)"
      kill "$old_pid" 2>/dev/null || true
      sleep 1
    fi
    rm -f "$SCRIPT_DIR/.kafkavista.pid"
  fi

  # 启动后端
  export HTTP_ADDR=":$API_PORT"
  export KAFKA_DATABASE_PATH="$local_data/kafkavista.db"
  export JWT_SECRET="$JWT_SECRET"
  export AUTH_DEFAULT_PASSWORD="${AUTH_DEFAULT_PASSWORD:-admin}"
  export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

  cd "$APP_DIR/server"
  nohup ./kafkavista > "$SCRIPT_DIR/kafkavista.log" 2>&1 &
  echo $! > "$SCRIPT_DIR/.kafkavista.pid"
  info "后端已启动 (PID: $(cat "$SCRIPT_DIR/.kafkavista.pid"))"

  # 启动前端（使用 npx serve 或 python 静态服务器）
  if has npx; then
    cd "$APP_DIR/web"
    nohup npx serve -s dist -l "$WEB_PORT" > "$SCRIPT_DIR/kafkavista-web.log" 2>&1 &
    echo $! > "$SCRIPT_DIR/.kafkavista-web.pid"
    info "前端已启动 (PID: $(cat "$SCRIPT_DIR/.kafkavista-web.pid"))"
  else
    warn "npx 未安装，前端静态文件位于 app/web/dist/，请用 Nginx 或其他静态服务器托管"
  fi

  # 等待后端就绪
  step "等待后端服务就绪..."
  local retries=15
  while [[ $retries -gt 0 ]]; do
    if curl -sf "http://localhost:$API_PORT/api/health" >/dev/null 2>&1; then
      info "后端服务已就绪"
      break
    fi
    retries=$((retries - 1))
    sleep 2
  done

  print_result
}

# ============================================================
# 开发模式
# ============================================================
install_dev() {
  step "本地开发模式"

  if ! check_go; then error "Go >= 1.26 未安装"; exit 1; fi
  if ! check_node; then error "Node >= 18 未安装"; exit 1; fi

  mkdir -p "$APP_DIR/data"

  # 安装前端依赖
  step "安装前端依赖..."
  (cd "$APP_DIR/web" && npm install)

  # 启动后端
  step "启动后端 (go run)..."
  cd "$APP_DIR/server"
  export GOPROXY="${GOPROXY:-https://goproxy.cn,direct}"
  export GOSUMDB="${GOSUMDB:-sum.golang.google.cn}"
  export KAFKA_DATABASE_PATH="$APP_DIR/data/kafkavista.db"
  export JWT_SECRET="$JWT_SECRET"
  export AUTH_DEFAULT_PASSWORD="${AUTH_DEFAULT_PASSWORD:-admin}"
  nohup go run ./cmd/kafkavista > "$SCRIPT_DIR/kafkavista-dev.log" 2>&1 &
  echo $! > "$SCRIPT_DIR/.kafkavista-dev.pid"
  info "后端开发服务启动中 (PID: $(cat "$SCRIPT_DIR/.kafkavista-dev.pid"))"

  # 启动前端
  step "启动前端 (vite dev)..."
  cd "$APP_DIR/web"
  nohup npx vite --host 0.0.0.0 --port 3000 > "$SCRIPT_DIR/vite-dev.log" 2>&1 &
  echo $! > "$SCRIPT_DIR/.vite-dev.pid"
  info "前端开发服务启动中 (PID: $(cat "$SCRIPT_DIR/.vite-dev.pid"))"

  sleep 3
  echo ""
  echo -e "${GREEN}========================================${NC}"
  echo -e "${GREEN}  KafkaVista 开发模式已启动${NC}"
  echo -e "${GREEN}========================================${NC}"
  echo ""
  echo "  前端:    http://localhost:3000"
  echo "  后端:    http://localhost:$API_PORT"
  echo "  账号:    admin / ${AUTH_DEFAULT_PASSWORD:-admin}"
  echo ""
  echo "  后端日志: tail -f $SCRIPT_DIR/kafkavista-dev.log"
  echo "  前端日志: tail -f $SCRIPT_DIR/vite-dev.log"
  echo ""
  echo "  停止: kill \$(cat $SCRIPT_DIR/.kafkavista-dev.pid) \$(cat $SCRIPT_DIR/.vite-dev.pid)"
  echo ""
}

# ============================================================
# 输出结果
# ============================================================
print_result() {
  echo ""
  echo -e "${GREEN}========================================${NC}"
  echo -e "${GREEN}  KafkaVista 部署完成！${NC}"
  echo -e "${GREEN}========================================${NC}"
  echo ""
  echo "  前端:    http://localhost:$WEB_PORT"
  echo "  后端:    http://localhost:$API_PORT"
  echo "  账号:    admin / ${AUTH_DEFAULT_PASSWORD:-admin}"
  echo ""
  if [[ "$MODE" == "docker" ]]; then
    echo "  日志:    cd app && ${COMPOSE[*]} logs -f"
    echo "  停止:    cd app && ${COMPOSE[*]} down"
    echo "  重启:    cd app && ${COMPOSE[*]} restart"
  else
    echo "  日志:    tail -f $SCRIPT_DIR/kafkavista.log"
    echo "  停止:    kill \$(cat $SCRIPT_DIR/.kafkavista.pid)"
  fi
  echo ""
  echo -e "${YELLOW}  请及时修改默认密码！${NC}"
  echo ""
}

# ============================================================
# 执行
# ============================================================
case "$MODE" in
  docker)  install_docker;;
  source)  install_source;;
  dev)     install_dev;;
  *)       error "未知模式: $MODE"; exit 1;;
esac
