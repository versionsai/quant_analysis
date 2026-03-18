#!/bin/bash
# ===========================================
# 量化选股推送服务 - NAS部署脚本
# 适用于群晖/威联通等NAS设备
# ===========================================

set -e

# 配置
CONTAINER_NAME="quant-stock-bot"
IMAGE_NAME="quant-stock-bot"
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

echo_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

echo_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查Docker
check_docker() {
    if ! command -v docker &> /dev/null; then
        echo_error "Docker未安装，请先安装Docker"
        exit 1
    fi
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        echo_error "Docker Compose未安装，请先安装Docker Compose"
        exit 1
    fi
    echo_info "Docker环境检查通过"
}

# 初始化配置
init_config() {
    cd "$PROJECT_DIR"
    
    if [ ! -f .env ]; then
        if [ -f .env.example ]; then
            cp .env.example .env
            echo_info "已创建 .env 配置文件，请编辑修改"
            echo_warn "请设置 BARK_KEY 后再继续"
            exit 0
        else
            echo_error "缺少 .env 配置文件"
            exit 1
        fi
    fi
    
    # 检查BARK_KEY
    source .env
    if [ -z "$BARK_KEY" ] || [ "$BARK_KEY" = "your_bark_key_here" ]; then
        echo_error "请先在 .env 中设置 BARK_KEY"
        exit 1
    fi
}

# 构建镜像
build() {
    echo_info "开始构建Docker镜像..."
    cd "$PROJECT_DIR"
    
    if docker compose build; then
        echo_info "镜像构建成功"
    else
        echo_error "镜像构建失败"
        exit 1
    fi
}

# 启动服务
start() {
    echo_info "启动服务..."
    cd "$PROJECT_DIR"
    
    docker compose up -d
    echo_info "服务已启动"
    
    # 显示状态
    status
}

# 停止服务
stop() {
    echo_info "停止服务..."
    cd "$PROJECT_DIR"
    docker compose stop
    echo_info "服务已停止"
}

# 重启服务
restart() {
    stop
    start
}

# 查看日志
logs() {
    cd "$PROJECT_DIR"
    docker compose logs -f --tail=50
}

# 查看状态
status() {
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo_info "容器运行中:"
        docker ps --filter "name=${CONTAINER_NAME}" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    else
        echo_warn "容器未运行"
    fi
}

# 手动推送一次
push() {
    echo_info "执行单次推送..."
    cd "$PROJECT_DIR"
    docker compose exec -T quant-bot python main.py --mode realtime
}

# 更新服务
update() {
    echo_info "更新服务..."
    cd "$PROJECT_DIR"
    
    # 拉取最新代码（如果是git目录）
    if [ -d .git ]; then
        echo_info "拉取最新代码..."
        git pull origin main || echo_warn "代码拉取失败，继续使用当前版本"
    fi
    
    # 重新构建
    build
    
    # 重启
    restart
    
    echo_info "更新完成"
}

# 清理
clean() {
    echo_warn "清理将删除所有容器和镜像，是否继续? (y/n)"
    read -r confirm
    if [ "$confirm" != "y" ]; then
        echo_info "已取消"
        return
    fi
    
    cd "$PROJECT_DIR"
    docker compose down -v
    docker rmi ${IMAGE_NAME}:latest 2>/dev/null || true
    echo_info "清理完成"
}

# 帮助
usage() {
    echo "用法: $0 <command>"
    echo ""
    echo "命令:"
    echo "  init    初始化配置"
    echo "  build   构建Docker镜像"
    echo "  start   启动服务"
    echo "  stop    停止服务"
    echo "  restart 重启服务"
    echo "  logs    查看日志"
    echo "  status  查看状态"
    echo "  push    手动推送一次"
    echo "  update  更新服务"
    echo "  clean   清理环境"
    echo ""
    echo "示例:"
    echo "  $0 init      # 首次运行时初始化"
    echo "  $0 build    # 构建镜像"
    echo "  $0 start    # 启动服务"
    echo "  $0 logs     # 查看日志"
}

# 主函数
case "${1:-}" in
    init)
        check_docker
        init_config
        ;;
    build)
        check_docker
        build
        ;;
    start)
        check_docker
        init_config
        start
        ;;
    stop)
        stop
        ;;
    restart)
        restart
        ;;
    logs)
        logs
        ;;
    status)
        status
        ;;
    push)
        push
        ;;
    update)
        check_docker
        update
        ;;
    clean)
        clean
        ;;
    *)
        usage
        ;;
esac
