#!/usr/bin/env python3
"""
V2Ray 订阅聚合平台
功能：多源采集 → 解析去重 → 延迟丢包测试 → 筛选TOP10 → 生成订阅
部署：云服务器 + cron 定时执行
"""

import re
import sys
import json
import time
import base64
import socket
import logging
import argparse
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("请安装 requests: pip install requests")
    sys.exit(1)

# ============================================================
# 配置
# ============================================================

# 订阅源列表（可自行添加更多）
SUBSCRIBE_URLS = [
    # 米贝分享（动态获取，每日更新）
    {"name": "mibei77", "type": "dynamic", "category_url": "https://www.mibei77.com/category/jiedian"},
    # 以下为示例静态订阅源，替换为你实际收集到的链接
    # {"name": "source2", "type": "static", "url": "https://example.com/sub.txt"},
    # {"name": "source3", "type": "static", "url": "https://example.com/sub2.txt"},
]

# 测速配置
TEST_CONFIG = {
    "tcp_ping_count": 5,        # 每个节点 TCP ping 次数
    "tcp_ping_timeout": 3,      # 单次超时（秒）
    "max_workers": 50,          # 并发测试线程数
    "top_n": 10,                # 筛选最优节点数
    "max_latency_ms": 1000,     # 最大可接受延迟（ms）
    "max_loss_rate": 0.4,       # 最大可接受丢包率
}

# HTTP 请求头
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# 输出目录
OUTPUT_DIR = Path(__file__).parent / "output"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ============================================================
# 第一步：多源采集
# ============================================================

def fetch_mibei77_dynamic():
    """动态获取米贝分享当日订阅链接内容"""
    category_url = "https://www.mibei77.com/category/jiedian"
    resp = requests.get(category_url, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    # 提取最新帖子链接
    matches = re.findall(r'href="(https://www\.mibei77\.com/\d+\.html)"', resp.text)
    if not matches:
        raise Exception("米贝分享：未找到帖子链接")

    post_url = matches[0]
    resp2 = requests.get(post_url, headers=HEADERS, timeout=30)
    resp2.raise_for_status()

    # 提取 v2ray 订阅链接
    v2ray_links = re.findall(r'(https://mm\.mibei77\.com/[^\s<"\']+\.txt)', resp2.text)
    if not v2ray_links:
        raise Exception("米贝分享：未找到订阅链接")

    # 下载订阅内容
    sub_resp = requests.get(v2ray_links[0], headers=HEADERS, timeout=30)
    sub_resp.raise_for_status()
    return sub_resp.text.strip()


def fetch_static_subscription(url):
    """获取静态订阅源内容"""
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text.strip()


def collect_all_subscriptions():
    """采集所有订阅源，返回原始文本列表"""
    raw_contents = []

    for source in SUBSCRIBE_URLS:
        try:
            if source["type"] == "dynamic" and source.get("category_url"):
                if "mibei77" in source.get("category_url", ""):
                    content = fetch_mibei77_dynamic()
                    raw_contents.append(content)
                    logger.info(f"✓ [{source['name']}] 采集成功")
            elif source["type"] == "static":
                content = fetch_static_subscription(source["url"])
                raw_contents.append(content)
                logger.info(f"✓ [{source['name']}] 采集成功")
        except Exception as e:
            logger.warning(f"✗ [{source['name']}] 采集失败: {e}")

    return raw_contents


# ============================================================
# 第二步：解析节点
# ============================================================

def decode_base64(text):
    """base64 解码，兼容非标准 padding"""
    text = text.strip()
    padding = 4 - len(text) % 4
    if padding != 4:
        text += "=" * padding
    try:
        return base64.b64decode(text).decode("utf-8", errors="ignore")
    except Exception:
        return ""


def parse_vmess(uri):
    """解析 vmess:// 链接"""
    try:
        raw = uri.replace("vmess://", "")
        decoded = decode_base64(raw)
        config = json.loads(decoded)
        return {
            "protocol": "vmess",
            "address": config.get("add", ""),
            "port": int(config.get("port", 0)),
            "name": config.get("ps", ""),
            "raw": uri,
            "uid": f"vmess:{config.get('add')}:{config.get('port')}",
        }
    except Exception:
        return None


def parse_vless(uri):
    """解析 vless:// 链接"""
    try:
        parsed = urllib.parse.urlparse(uri)
        return {
            "protocol": "vless",
            "address": parsed.hostname or "",
            "port": int(parsed.port or 0),
            "name": urllib.parse.unquote(parsed.fragment or ""),
            "raw": uri,
            "uid": f"vless:{parsed.hostname}:{parsed.port}",
        }
    except Exception:
        return None


def parse_ss(uri):
    """解析 ss:// 链接"""
    try:
        uri_clean = uri.replace("ss://", "")
        # 处理带 # 名称的情况
        name = ""
        if "#" in uri_clean:
            uri_clean, name = uri_clean.rsplit("#", 1)
            name = urllib.parse.unquote(name)

        # 有些格式是 base64@host:port，有些整体 base64
        if "@" in uri_clean:
            _, host_port = uri_clean.split("@", 1)
            host, port = host_port.rsplit(":", 1)
            port = int(port.split("?")[0].split("/")[0])
        else:
            decoded = decode_base64(uri_clean)
            if "@" in decoded:
                _, host_port = decoded.split("@", 1)
                host, port = host_port.rsplit(":", 1)
                port = int(port.split("?")[0].split("/")[0])
            else:
                return None

        return {
            "protocol": "ss",
            "address": host,
            "port": port,
            "name": name,
            "raw": uri,
            "uid": f"ss:{host}:{port}",
        }
    except Exception:
        return None


def parse_trojan(uri):
    """解析 trojan:// 链接"""
    try:
        parsed = urllib.parse.urlparse(uri)
        return {
            "protocol": "trojan",
            "address": parsed.hostname or "",
            "port": int(parsed.port or 443),
            "name": urllib.parse.unquote(parsed.fragment or ""),
            "raw": uri,
            "uid": f"trojan:{parsed.hostname}:{parsed.port}",
        }
    except Exception:
        return None


def parse_nodes(raw_contents):
    """解析所有订阅内容为节点列表"""
    nodes = []
    parsers = {
        "vmess://": parse_vmess,
        "vless://": parse_vless,
        "ss://": parse_ss,
        "trojan://": parse_trojan,
    }

    for content in raw_contents:
        # 尝试 base64 解码
        decoded = decode_base64(content)
        if not decoded:
            decoded = content

        for line in decoded.splitlines():
            line = line.strip()
            if not line:
                continue
            for prefix, parser in parsers.items():
                if line.startswith(prefix):
                    node = parser(line)
                    if node and node["address"] and node["port"]:
                        nodes.append(node)
                    break

    return nodes


def deduplicate_nodes(nodes):
    """按 地址+端口+协议 去重"""
    seen = set()
    unique = []
    for node in nodes:
        if node["uid"] not in seen:
            seen.add(node["uid"])
            unique.append(node)
    return unique


# ============================================================
# 第三步：测速（延迟 + 丢包）
# ============================================================

def tcp_ping(host, port, timeout=3):
    """单次 TCP ping，返回延迟(ms)，失败返回 None"""
    try:
        start = time.time()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, port))
        sock.close()
        return (time.time() - start) * 1000
    except Exception:
        return None


def test_node(node, ping_count=5, timeout=3):
    """测试单个节点，返回延迟和丢包率"""
    results = []
    for _ in range(ping_count):
        latency = tcp_ping(node["address"], node["port"], timeout)
        results.append(latency)

    successes = [r for r in results if r is not None]
    total = len(results)
    loss_rate = 1.0 - len(successes) / total if total > 0 else 1.0
    avg_latency = sum(successes) / len(successes) if successes else float("inf")

    return {
        **node,
        "avg_latency_ms": round(avg_latency, 1),
        "loss_rate": round(loss_rate, 3),
        "success_count": len(successes),
        "total_count": total,
    }


def batch_test_nodes(nodes):
    """并发测试所有节点"""
    config = TEST_CONFIG
    results = []
    total = len(nodes)

    logger.info(f"开始测速，共 {total} 个节点，并发 {config['max_workers']} 线程...")

    with ThreadPoolExecutor(max_workers=config["max_workers"]) as executor:
        futures = {
            executor.submit(
                test_node, node, config["tcp_ping_count"], config["tcp_ping_timeout"]
            ): node
            for node in nodes
        }

        done_count = 0
        for future in as_completed(futures):
            done_count += 1
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.debug(f"测试异常: {e}")

            if done_count % 20 == 0 or done_count == total:
                logger.info(f"  进度: {done_count}/{total}")

    return results


# ============================================================
# 第四步：筛选 TOP N + 生成订阅
# ============================================================

def select_best_nodes(test_results, top_n=10, max_latency=1000, max_loss=0.4):
    """筛选最优节点：丢包率低 + 延迟小"""
    # 过滤不可用节点
    valid = [
        r for r in test_results
        if r["avg_latency_ms"] < max_latency and r["loss_rate"] <= max_loss
    ]

    # 综合评分：延迟权重 0.7 + 丢包权重 0.3
    for node in valid:
        node["score"] = node["avg_latency_ms"] * 0.7 + node["loss_rate"] * 1000 * 0.3

    # 按评分排序
    valid.sort(key=lambda x: x["score"])

    return valid[:top_n]


def generate_subscription(nodes):
    """将节点列表生成标准 base64 订阅内容"""
    lines = [node["raw"] for node in nodes]
    content = "\n".join(lines)
    return base64.b64encode(content.encode("utf-8")).decode("utf-8")


# ============================================================
# 主流程
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="V2Ray 订阅聚合 - 采集/去重/测速/筛选")
    parser.add_argument("--top", type=int, default=TEST_CONFIG["top_n"], help="筛选最优节点数 (默认10)")
    parser.add_argument("--workers", type=int, default=TEST_CONFIG["max_workers"], help="并发线程数 (默认50)")
    parser.add_argument("--output", type=str, default=None, help="输出目录 (默认 ./output)")
    parser.add_argument("--ping-count", type=int, default=TEST_CONFIG["tcp_ping_count"], help="每节点ping次数 (默认5)")
    args = parser.parse_args()

    TEST_CONFIG["top_n"] = args.top
    TEST_CONFIG["max_workers"] = args.workers
    TEST_CONFIG["tcp_ping_count"] = args.ping_count

    output_dir = Path(args.output) if args.output else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("V2Ray 订阅聚合平台 启动")
    logger.info("=" * 60)

    # Step 1: 采集
    logger.info("\n[1/4] 采集订阅源...")
    raw_contents = collect_all_subscriptions()
    if not raw_contents:
        logger.error("所有订阅源采集失败，退出")
        sys.exit(1)

    # Step 2: 解析去重
    logger.info("\n[2/4] 解析节点并去重...")
    nodes = parse_nodes(raw_contents)
    logger.info(f"  解析得到 {len(nodes)} 个节点")
    unique_nodes = deduplicate_nodes(nodes)
    logger.info(f"  去重后剩余 {len(unique_nodes)} 个节点")

    if not unique_nodes:
        logger.error("无可用节点，退出")
        sys.exit(1)

    # Step 3: 测速
    logger.info("\n[3/4] 节点测速...")
    test_results = batch_test_nodes(unique_nodes)

    # Step 4: 筛选 + 输出
    logger.info("\n[4/4] 筛选最优节点...")
    best_nodes = select_best_nodes(
        test_results,
        top_n=TEST_CONFIG["top_n"],
        max_latency=TEST_CONFIG["max_latency_ms"],
        max_loss=TEST_CONFIG["max_loss_rate"],
    )

    if not best_nodes:
        logger.warning("未筛选到可用节点（所有节点延迟过高或丢包严重）")
        # 退而求其次，取延迟最低的
        test_results.sort(key=lambda x: x["avg_latency_ms"])
        best_nodes = test_results[:TEST_CONFIG["top_n"]]

    # 输出结果
    logger.info(f"\n{'='*60}")
    logger.info(f"最优 {len(best_nodes)} 个节点：")
    logger.info(f"{'序号':<4} {'协议':<8} {'地址':<40} {'延迟(ms)':<10} {'丢包率':<8} {'名称'}")
    logger.info(f"{'-'*100}")
    for i, node in enumerate(best_nodes, 1):
        logger.info(
            f"{i:<4} {node['protocol']:<8} {node['address']}:{node['port']:<30} "
            f"{node['avg_latency_ms']:<10} {node['loss_rate']*100:.0f}%{'':<5} {node.get('name', '')[:30]}"
        )

    # 生成订阅文件
    sub_content = generate_subscription(best_nodes)
    today = datetime.now().strftime("%Y%m%d")

    # 写入文件
    sub_file = output_dir / "best_nodes.txt"
    sub_file.write_text(sub_content, encoding="utf-8")

    sub_file_dated = output_dir / f"best_nodes_{today}.txt"
    sub_file_dated.write_text(sub_content, encoding="utf-8")

    # 写入详细报告 JSON
    report = {
        "generated_at": datetime.now().isoformat(),
        "total_sources": len(SUBSCRIBE_URLS),
        "total_nodes_parsed": len(nodes),
        "unique_nodes": len(unique_nodes),
        "tested_nodes": len(test_results),
        "selected_nodes": len(best_nodes),
        "best_nodes": [
            {
                "rank": i + 1,
                "protocol": n["protocol"],
                "address": n["address"],
                "port": n["port"],
                "name": n.get("name", ""),
                "avg_latency_ms": n["avg_latency_ms"],
                "loss_rate": n["loss_rate"],
            }
            for i, n in enumerate(best_nodes)
        ],
    }
    report_file = output_dir / "report.json"
    report_file.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    logger.info(f"\n输出文件：")
    logger.info(f"  订阅文件: {sub_file}")
    logger.info(f"  带日期备份: {sub_file_dated}")
    logger.info(f"  测速报告: {report_file}")
    logger.info(f"\n完成！将 {sub_file} 通过 Web 服务暴露即可作为订阅地址。")


if __name__ == "__main__":
    main()
