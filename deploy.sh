#!/bin/bash
# 部署脚本 - 在云服务器上执行
# 用法: bash deploy.sh

set -e

echo "=== V2Ray 订阅聚合平台 部署 ==="

# 1. 创建项目目录
PROJECT_DIR="/opt/v2ray-aggregator"
mkdir -p "$PROJECT_DIR/output"

# 2. 复制文件（假设你已将文件 scp 到服务器）
cp v2ray_aggregator.py "$PROJECT_DIR/"
cp requirements.txt "$PROJECT_DIR/"

# 3. 创建 Python 虚拟环境
cd "$PROJECT_DIR"
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt -q

# 4. 测试运行
echo "测试运行..."
python3 v2ray_aggregator.py --verbose

# 5. 设置 cron 定时任务（每 6 小时执行一次）
CRON_CMD="0 */6 * * * cd $PROJECT_DIR && $PROJECT_DIR/venv/bin/python3 v2ray_aggregator.py --verbose >> $PROJECT_DIR/cron.log 2>&1"

# 检查是否已有相同 cron
(crontab -l 2>/dev/null | grep -v "v2ray_aggregator" ; echo "$CRON_CMD") | crontab -

echo ""
echo "=== 部署完成 ==="
echo "项目目录: $PROJECT_DIR"
echo "订阅文件: $PROJECT_DIR/output/best_nodes.txt"
echo "定时任务: 每 6 小时自动更新"
echo ""
echo "下一步: 配置 Nginx/Caddy 将 output/best_nodes.txt 暴露为 HTTP 订阅地址"
