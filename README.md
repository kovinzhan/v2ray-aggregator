# V2Ray 订阅聚合平台

从互联网多个免费订阅源采集节点，去重后测速筛选，生成个人最优订阅列表。

## 功能

- **多源采集**：支持动态源（如米贝分享每日更新）和静态订阅链接
- **协议支持**：vmess / vless / ss / trojan
- **智能去重**：按 `协议+地址+端口` 去重
- **并发测速**：TCP ping 测延迟 + 丢包率，50线程并发
- **TOP N 筛选**：综合评分（延迟70% + 丢包30%）选出最优节点
- **标准输出**：生成 base64 编码订阅文件，兼容所有 v2ray 客户端

## 文件结构

```
v2ray-aggregator/
├── v2ray_aggregator.py   # 核心脚本
├── requirements.txt      # Python 依赖
├── deploy.sh             # 云服务器一键部署
├── nginx.conf            # Nginx 配置示例
├── output/               # 输出目录（自动创建）
│   ├── best_nodes.txt        # 最新订阅文件
│   ├── best_nodes_20260624.txt  # 带日期备份
│   └── report.json           # 测速报告
└── README.md
```

## 快速开始

### 方式一：GitHub Actions 自动运行（推荐）

1. Fork 本仓库或创建新仓库，将代码推上去
2. 进入仓库 Settings → Actions → General → Workflow permissions → 选择 **Read and write permissions**
3. 完成！GitHub Actions 会每天中午 12:00 (UTC+8) 自动执行
4. 也可以手动触发：Actions → V2Ray Subscription Aggregator → Run workflow

#### 订阅地址

脚本执行后会将结果提交到 `output/best_nodes.txt`，你的个人订阅地址为：

```
https://raw.githubusercontent.com/你的用户名/你的仓库名/main/output/best_nodes.txt
```

> **注意**：如果仓库是 Private，需要用带 token 的 URL：
> ```
> https://raw.githubusercontent.com/用户名/仓库名/main/output/best_nodes.txt?token=你的PAT
> ```
> 或者使用 [GitHub Gist](https://gist.github.com) 存放（公开 Gist 无需 token）。

在 v2ray/clash 客户端中添加此地址即可自动获取最优节点。

### 方式二：本地测试

```bash
pip install -r requirements.txt
python3 v2ray_aggregator.py --top 10
```

### 方式三：云服务器部署

```bash
scp -r v2ray-aggregator/ root@你的IP:/opt/
ssh root@你的IP
cd /opt/v2ray-aggregator
bash deploy.sh
```

## 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--top` | 10 | 筛选最优节点数 |
| `--workers` | 50 | 并发测试线程数 |
| `--ping-count` | 5 | 每个节点 TCP ping 次数 |
| `--output` | ./output | 输出目录路径 |

示例：

```bash
# 筛选 TOP 20，100 线程并发，每节点 ping 10 次
python3 v2ray_aggregator.py --top 20 --workers 100 --ping-count 10
```

## 添加订阅源

编辑 `v2ray_aggregator.py` 中的 `SUBSCRIBE_URLS` 列表：

```python
SUBSCRIBE_URLS = [
    # 动态源（需自定义解析逻辑）
    {"name": "mibei77", "type": "dynamic", "category_url": "https://www.mibei77.com/category/jiedian"},

    # 静态订阅链接（直接返回 base64 内容）
    {"name": "freesub1", "type": "static", "url": "https://example.com/sub1.txt"},
    {"name": "freesub2", "type": "static", "url": "https://example.com/sub2.txt"},
]
```

## 定时任务

部署脚本已自动配置 cron，每天 12:00 执行。手动修改：

```bash
crontab -e
# 修改时间，如改为每 6 小时执行
0 */6 * * * cd /opt/v2ray-aggregator && /opt/v2ray-aggregator/venv/bin/python3 v2ray_aggregator.py --top 10 >> cron.log 2>&1
```

## 节点评分算法

```
score = avg_latency_ms × 0.7 + loss_rate × 1000 × 0.3
```

- 延迟权重 70%：优先选延迟低的节点
- 丢包权重 30%：排除不稳定节点
- 过滤条件：延迟 > 1000ms 或丢包 > 40% 直接排除
