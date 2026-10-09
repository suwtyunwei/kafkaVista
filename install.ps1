# KafkaVista 一键安装脚本 (Windows PowerShell)
# 用法: .\install.ps1 [-Mode docker|source|dev]
#   docker  使用 Docker Compose 部署（默认，如 Docker 可用）
#   source  从源码编译部署
#   dev     本地开发模式（前后端热重载）

param(
    [ValidateSet("docker", "source", "dev", "auto")]
    [string]$Mode = "auto",

    [string]$DataDir = "",
    [int]$WebPort = 3004,
    [int]$ApiPort = 8080,
    [string]$JwtSecret = "",
    [string]$KafkaServers = "localhost:9092",
    [string]$AdminPassword = "admin"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$AppDir = Join-Path $ScriptDir "app"

function Write-Info    { Write-Host "[INFO] $args" -ForegroundColor Green }
function Write-Warn    { Write-Host "[WARN] $args" -ForegroundColor Yellow }
function Write-Error  { Write-Host "[ERROR] $args" -ForegroundColor Red }
function Write-Step    { Write-Host "[STEP] $args" -ForegroundColor Cyan }

# 生成随机 JWT Secret
if ([string]::IsNullOrEmpty($JwtSecret)) {
    $bytes = New-Object byte[] 32
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    $JwtSecret = -join ($bytes | ForEach-Object { $_.ToString("x2") })
}

# 数据目录
if ([string]::IsNullOrEmpty($DataDir)) {
    $DataDir = Join-Path $AppDir "data"
}

# ============================================================
# 检测工具
# ============================================================
function Test-GoVersion {
    try {
        $ver = & go version 2>$null
        if ($LASTEXITCODE -ne 0 -or -not $ver) { return $false }
        if ($ver -match "go(\d+)\.(\d+)") {
            $major = [int]$Matches[1]; $minor = [int]$Matches[2]
            if ($major -lt 1 -or ($major -eq 1 -and $minor -lt 26)) {
                Write-Warn "Go 版本过低: $ver, 需要 >= 1.26"
                return $false
            }
            return $true
        }
    } catch {}
    return $false
}

function Test-NodeVersion {
    try {
        $ver = & node --version 2>$null
        if ($LASTEXITCODE -ne 0 -or -not $ver) { return $false }
        if ($ver -match "v(\d+)") {
            if ([int]$Matches[1] -lt 18) {
                Write-Warn "Node 版本过低: $ver, 需要 >= 18"
                return $false
            }
            return $true
        }
    } catch {}
    return $false
}

function Test-Docker {
    try {
        $null = & docker compose version 2>$null
        if ($LASTEXITCODE -eq 0) { return $true }
        $null = & docker-compose version 2>$null
        return ($LASTEXITCODE -eq 0)
    } catch { return $false }
}

# ============================================================
# 自动选择模式
# ============================================================
if ($Mode -eq "auto") {
    if (Test-Docker) {
        $Mode = "docker"
        Write-Info "检测到 Docker, 使用 Docker Compose 部署"
    } elseif (Test-GoVersion -and (Test-NodeVersion)) {
        $Mode = "source"
        Write-Info "检测到 Go + Node, 从源码编译部署"
    } else {
        Write-Error "未检测到 Docker 或 Go+Node 环境, 无法自动部署"
        Write-Host ""
        Write-Host "请安装以下任一组合:"
        Write-Host "  1. Docker Desktop (推荐)"
        Write-Host "     https://www.docker.com/products/docker-desktop/"
        Write-Host "  2. Go >= 1.26 + Node >= 18"
        Write-Host "     https://go.dev/dl/"
        Write-Host "     https://nodejs.org/"
        exit 1
    }
}

# ============================================================
# Docker Compose 部署
# ============================================================
function Install-Docker {
    Write-Step "Docker Compose 部署"

    Set-Location $ScriptDir

    # 数据目录
    $hostDataDir = "/data/local/kafkaVista/data"
    Write-Info "数据目录: $hostDataDir (容器内 /app/data)"
    if ($PSVersionTable.Platform -eq "Unix") {
        New-Item -ItemType Directory -Force -Path $hostDataDir | Out-Null
    }

    # .env 文件
    $envFile = Join-Path $ScriptDir ".env"
    if (-not (Test-Path $envFile)) {
        Write-Info "生成 .env 配置文件"
        $envContent = @"
JWT_SECRET=$JwtSecret
AUTH_DEFAULT_USER=admin
AUTH_DEFAULT_PASSWORD=$AdminPassword
KAFKA_CLUSTER_NAME=default
KAFKA_BOOTSTRAP_SERVERS=$KafkaServers
KAFKA_REQUEST_TIMEOUT_MS=6000
KAFKA_MAX_POLL_RECORDS=50000
KAFKA_MAX_VALUE_LENGTH=1048576
KAFKA_MAX_RESPONSE_BYTES=10485760
EMAIL_SMTP_HOST=
EMAIL_SMTP_PORT=587
EMAIL_SMTP_USERNAME=
EMAIL_SMTP_PASSWORD=
EMAIL_FROM=
EMAIL_TO=
EMAIL_USE_TLS=false
CORS_ALLOWED_ORIGINS=
"@
        [System.IO.File]::WriteAllText($envFile, $envContent)
        Write-Info ".env 已生成, 请按需修改后重新运行"
    }

    # 构建并启动
    Write-Step "构建镜像并启动服务..."
    if ($null -ne (& docker compose version 2>$null)) {
        docker compose up -d --build --remove-orphans
    } else {
        docker-compose up -d --build --remove-orphans
    }
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Docker Compose 启动失败"
        exit 1
    }

    # 等待服务就绪
    Write-Step "等待后端服务就绪..."
    $retries = 30
    while ($retries -gt 0) {
        try {
            $null = Invoke-RestMethod -Uri "http://localhost:$ApiPort/api/health" -TimeoutSec 3
            Write-Info "后端服务已就绪"
            break
        } catch {
            $retries--
            Start-Sleep -Seconds 2
        }
    }
    if ($retries -eq 0) {
        Write-Warn "后端服务未在 60 秒内就绪, 请检查: docker compose logs kafkavista-server"
    }

    Print-Result
}

# ============================================================
# 源码编译部署
# ============================================================
function Install-Source {
    Write-Step "源码编译部署"

    # 检查依赖
    if (-not (Test-GoVersion)) {
        Write-Error "Go >= 1.26 未安装"
        Write-Host "  安装: https://go.dev/dl/"
        exit 1
    }
    if (-not (Test-NodeVersion)) {
        Write-Error "Node >= 18 未安装"
        Write-Host "  安装: https://nodejs.org/"
        exit 1
    }

    $goVer = & go version 2>$null
    $nodeVer = & node --version 2>$null
    Write-Info "Go: $goVer"
    Write-Info "Node: $nodeVer"

    # 数据目录
    New-Item -ItemType Directory -Force -Path $DataDir | Out-Null
    Write-Info "数据目录: $DataDir"

    # 编译后端
    Write-Step "编译后端..."
    $serverDir = Join-Path $AppDir "server"
    Push-Location $serverDir
    try {
        $env:GOPROXY = "https://goproxy.cn,direct"
        $env:GOSUMDB = "sum.golang.google.cn"
        go mod download
        if ($LASTEXITCODE -ne 0) { Write-Error "go mod download 失败"; exit 1 }

        $env:CGO_ENABLED = "0"
        $env:GOOS = "windows"
        $env:GOARCH = "amd64"
        go build -trimpath -ldflags="-s -w" -o kafkavista.exe ./cmd/kafkavista
        if ($LASTEXITCODE -ne 0) { Write-Error "后端编译失败"; exit 1 }
        Write-Info "后端编译完成: app/server/kafkavista.exe"
    } finally {
        Pop-Location
    }

    # 编译前端
    Write-Step "编译前端..."
    $webDir = Join-Path $AppDir "web"
    Push-Location $webDir
    try {
        npm install --no-fund --no-audit
        if ($LASTEXITCODE -ne 0) { Write-Error "npm install 失败"; exit 1 }
        npm run build
        if ($LASTEXITCODE -ne 0) { Write-Error "前端构建失败"; exit 1 }
        Write-Info "前端构建完成: app/web/dist/"
    } finally {
        Pop-Location
    }

    # 停止旧进程
    $pidFile = Join-Path $ScriptDir ".kafkavista.pid"
    if (Test-Path $pidFile) {
        $oldPid = Get-Content $pidFile -ErrorAction SilentlyContinue
        if ($oldPid -and (Get-Process -Id $oldPid -ErrorAction SilentlyContinue)) {
            Write-Warn "停止旧进程 (PID: $oldPid)"
            Stop-Process -Id $oldPid -Force -ErrorAction SilentlyContinue
            Start-Sleep -Seconds 1
        }
        Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
    }

    # 启动后端
    Write-Step "启动后端服务..."
    $env:HTTP_ADDR = ":$ApiPort"
    $env:KAFKA_DATABASE_PATH = Join-Path $DataDir "kafkavista.db"
    $env:JWT_SECRET = $JwtSecret
    $env:AUTH_DEFAULT_PASSWORD = $AdminPassword
    $env:KAFKA_BOOTSTRAP_SERVERS = $KafkaServers

    $exePath = Join-Path $serverDir "kafkavista.exe"
    $logFile = Join-Path $ScriptDir "kafkavista.log"
    $proc = Start-Process -FilePath $exePath -WorkingDirectory $serverDir `
        -WindowStyle Hidden -RedirectStandardOutput $logFile -RedirectStandardError "$logFile.err" -PassThru
    Set-Content -Path $pidFile -Value $proc.Id
    Write-Info "后端已启动 (PID: $($proc.Id))"

    # 等待后端就绪
    Write-Step "等待后端服务就绪..."
    $retries = 15
    while ($retries -gt 0) {
        try {
            $null = Invoke-RestMethod -Uri "http://localhost:$ApiPort/api/health" -TimeoutSec 3
            Write-Info "后端服务已就绪"
            break
        } catch {
            $retries--
            Start-Sleep -Seconds 2
        }
    }
    if ($retries -eq 0) {
        Write-Warn "后端服务未就绪, 请检查日志: $logFile"
    }

    Print-Result
}

# ============================================================
# 开发模式
# ============================================================
function Install-Dev {
    Write-Step "本地开发模式"

    if (-not (Test-GoVersion)) { Write-Error "Go >= 1.26 未安装"; exit 1 }
    if (-not (Test-NodeVersion)) { Write-Error "Node >= 18 未安装"; exit 1 }

    $dataDir = Join-Path $AppDir "data"
    New-Item -ItemType Directory -Force -Path $dataDir | Out-Null

    # 安装前端依赖
    Write-Step "安装前端依赖..."
    Push-Location (Join-Path $AppDir "web")
    npm install --no-fund --no-audit
    Pop-Location

    # 启动后端
    Write-Step "启动后端 (go run)..."
    $serverDir = Join-Path $AppDir "server"
    $env:GOPROXY = "https://goproxy.cn,direct"
    $env:GOSUMDB = "sum.golang.google.cn"
    $env:KAFKA_DATABASE_PATH = Join-Path $dataDir "kafkavista.db"
    $env:JWT_SECRET = $JwtSecret
    $env:AUTH_DEFAULT_PASSWORD = $AdminPassword
    $env:HTTP_ADDR = ":$ApiPort"

    $beLog = Join-Path $ScriptDir "kafkavista-dev.log"
    $beProc = Start-Process -FilePath "go" -ArgumentList "run", "./cmd/kafkavista" `
        -WorkingDirectory $serverDir -WindowStyle Hidden `
        -RedirectStandardOutput $beLog -RedirectStandardError "$beLog.err" -PassThru
    Set-Content -Path (Join-Path $ScriptDir ".kafkavista-dev.pid") -Value $beProc.Id
    Write-Info "后端开发服务启动中 (PID: $($beProc.Id))"

    # 启动前端
    Write-Step "启动前端 (vite dev)..."
    $webDir = Join-Path $AppDir "web"
    $feLog = Join-Path $ScriptDir "vite-dev.log"
    $feProc = Start-Process -FilePath "npx" -ArgumentList "vite", "--host", "127.0.0.1", "--port", "3000" `
        -WorkingDirectory $webDir -WindowStyle Hidden `
        -RedirectStandardOutput $feLog -RedirectStandardError "$feLog.err" -PassThru
    Set-Content -Path (Join-Path $ScriptDir ".vite-dev.pid") -Value $feProc.Id
    Write-Info "前端开发服务启动中 (PID: $($feProc.Id))"

    Start-Sleep -Seconds 3

    Write-Host ""
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "  KafkaVista 开发模式已启动" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "  前端:    http://localhost:3000"
    Write-Host "  后端:    http://localhost:$ApiPort"
    Write-Host "  账号:    admin / $AdminPassword"
    Write-Host ""
    Write-Host "  后端日志: Get-Content $beLog -Wait -Tail 20"
    Write-Host "  前端日志: Get-Content $feLog -Wait -Tail 20"
    Write-Host ""
    Write-Host "  停止: 停止对应进程或关闭终端"
    Write-Host ""
}

# ============================================================
# 输出结果
# ============================================================
function Print-Result {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "  KafkaVista 部署完成!" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "  前端:    http://localhost:$WebPort"
    Write-Host "  后端:    http://localhost:$ApiPort"
    Write-Host "  账号:    admin / $AdminPassword"
    Write-Host ""
    if ($Mode -eq "docker") {
        Write-Host "  日志:    cd app; docker compose logs -f"
        Write-Host "  停止:    cd app; docker compose down"
        Write-Host "  重启:    cd app; docker compose restart"
    } else {
        Write-Host "  日志:    Get-Content $(Join-Path $ScriptDir 'kafkavista.log') -Wait -Tail 20"
        Write-Host "  停止:    Stop-Process -Id (Get-Content $(Join-Path $ScriptDir '.kafkavista.pid'))"
    }
    Write-Host ""
    Write-Host "  请及时修改默认密码!" -ForegroundColor Yellow
    Write-Host ""
}

# ============================================================
# 执行
# ============================================================
switch ($Mode) {
    "docker" { Install-Docker }
    "source" { Install-Source }
    "dev"    { Install-Dev }
    default  { Write-Error "未知模式: $Mode"; exit 1 }
}
