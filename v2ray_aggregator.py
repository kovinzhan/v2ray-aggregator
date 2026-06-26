#!/usr/bin/env python3
"""
V2Ray 订阅聚合平台
功能：多源采集 → 解析去重 → 真实延迟测试(多轮+抖动+CDN识别) → 筛选TOP10 → 生成订阅
部署：云服务器 + cron 定时执行
"""

import os
import re
import sys
import json
import time
import base64
import shutil
import socket
import struct
import signal
import logging
import zipfile
import tarfile
import argparse
import platform
import tempfile
import statistics
import subprocess
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
    "tcp_ping_count": 3,        # 每个节点 TCP ping 次数（初筛阶段，减少次数加快速度）
    "tcp_ping_timeout": 5,      # 单次超时（秒）
    "max_workers": 30,          # 并发测试线程数
    "top_n": 10,                # 最终筛选最优节点数
    "max_latency_ms": 2000,     # 最大可接受延迟（ms）
    "max_loss_rate": 0.4,       # 最大可接受丢包率
    "test_rounds": 2,           # TCP/TLS 初筛轮次（减少，主要靠 xray 二次验证）
    "round_interval": 1,        # 轮次间隔（秒）
    "tls_test_enabled": True,   # 是否进行 TLS 握手测试
    "dns_resolve_first": True,  # 先 DNS 解析
    # xray-core 真实代理测速配置
    "xray_enabled": True,       # 是否启用 xray-core 真实代理测试
    "xray_test_count": 3,       # 每个节点通过代理请求次数
    "xray_test_timeout": 10,    # 代理请求超时（秒）
    "xray_startup_wait": 2,     # xray 进程启动等待（秒）
    "xray_candidate_count": 30, # 初筛后进入 xray 测试的候选节点数
    "xray_max_workers": 10,     # xray 测试并发数（单进程模式，可适当提高）
    "xray_test_url": "http://www.gstatic.com/generate_204",  # 连通性检测 URL
}

# 已知 CDN IP 段（这些 IP 的 TCP ping 不代表真实节点延迟）
CDN_IP_RANGES = [
    # Fastly
    ("151.101.0.0", "151.101.255.255"),
    ("199.232.0.0", "199.232.255.255"),
    # Cloudflare
    ("104.16.0.0", "104.31.255.255"),
    ("172.64.0.0", "172.71.255.255"),
    ("173.245.48.0", "173.245.63.255"),
    ("103.21.244.0", "103.22.255.255"),
    ("141.101.64.0", "141.101.127.255"),
    ("108.162.192.0", "108.162.255.255"),
    ("190.93.240.0", "190.93.255.255"),
    ("188.114.96.0", "188.114.111.255"),
    ("197.234.240.0", "197.234.243.255"),
    ("198.41.128.0", "198.41.255.255"),
    # Akamai 常见
    ("23.0.0.0", "23.79.255.255"),
    # AWS CloudFront
    ("13.32.0.0", "13.35.255.255"),
    ("52.84.0.0", "52.85.255.255"),
    ("99.84.0.0", "99.84.255.255"),
    ("143.204.0.0", "143.204.255.255"),
    ("204.246.164.0", "204.246.179.255"),
]

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
# 第三步：真实测速（多维度：TCP+TLS+DNS+CDN识别+多轮+抖动）
# ============================================================

def ip_to_int(ip_str):
    """IP 地址转整数"""
    return struct.unpack("!I", socket.inet_aton(ip_str))[0]


def is_cdn_ip(ip_str):
    """检测 IP 是否属于已知 CDN 范围"""
    try:
        ip_int = ip_to_int(ip_str)
        for start, end in CDN_IP_RANGES:
            if ip_to_int(start) <= ip_int <= ip_to_int(end):
                return True
    except Exception:
        pass
    return False


def dns_resolve(host, timeout=5):
    """DNS 解析，返回 (IP列表, 解析耗时ms)"""
    try:
        socket.setdefaulttimeout(timeout)
        start = time.time()
        ips = socket.gethostbyname_ex(host)[2]
        elapsed = (time.time() - start) * 1000
        return ips, elapsed
    except Exception:
        return [], 0


def tcp_ping(host, port, timeout=5):
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


def tls_handshake_ping(host, port, timeout=5):
    """TLS 握手测试 —— 比 TCP ping 更接近真实代理延迟
    包含 TCP连接 + TLS协商 的完整时间"""
    import ssl
    try:
        start = time.time()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        wrapped = context.wrap_socket(sock, server_hostname=host)
        wrapped.connect((host, port))
        elapsed = (time.time() - start) * 1000
        wrapped.close()
        return elapsed
    except Exception:
        return None


def test_node(node, ping_count=5, timeout=5):
    """多维度测试单个节点"""
    address = node["address"]
    port = node["port"]

    # 1. DNS 预解析（如果是域名）
    resolved_ip = address
    is_domain = not re.match(r'^\d+\.\d+\.\d+\.\d+$', address)
    dns_time_ms = 0

    if is_domain and TEST_CONFIG.get("dns_resolve_first"):
        ips, dns_time_ms = dns_resolve(address, timeout)
        if ips:
            resolved_ip = ips[0]  # 使用解析后的 IP 进行测试
        else:
            # DNS 解析失败，节点不可用
            return {
                **node,
                "avg_latency_ms": float("inf"),
                "min_latency_ms": float("inf"),
                "max_latency_ms": float("inf"),
                "jitter_ms": float("inf"),
                "loss_rate": 1.0,
                "success_count": 0,
                "total_count": ping_count,
                "is_cdn": False,
                "dns_time_ms": 0,
                "test_method": "dns_fail",
            }

    # 2. CDN 检测
    cdn_detected = is_cdn_ip(resolved_ip)

    # 3. 多轮测速
    all_latencies = []
    rounds = TEST_CONFIG.get("test_rounds", 3)
    round_interval = TEST_CONFIG.get("round_interval", 1)
    use_tls = TEST_CONFIG.get("tls_test_enabled", True) and port in (443, 8443, 2053, 2083, 2087, 2096)

    for round_idx in range(rounds):
        if round_idx > 0:
            time.sleep(round_interval)

        for _ in range(ping_count):
            if use_tls:
                # TLS 握手延迟（更真实）
                latency = tls_handshake_ping(address, port, timeout)
            else:
                # TCP ping
                latency = tcp_ping(resolved_ip, port, timeout)
            all_latencies.append(latency)

    # 4. 统计分析
    successes = [r for r in all_latencies if r is not None]
    total = len(all_latencies)
    loss_rate = 1.0 - len(successes) / total if total > 0 else 1.0

    if successes:
        avg_latency = statistics.mean(successes)
        min_latency = min(successes)
        max_latency = max(successes)
        # 计算抖动（标准差）—— 越低越稳定
        jitter = statistics.stdev(successes) if len(successes) > 1 else 0
        # 去除最高和最低值后的平均（trimmed mean）—— 更准确
        if len(successes) > 4:
            sorted_s = sorted(successes)
            trimmed = sorted_s[1:-1]  # 去掉最快和最慢
            avg_latency = statistics.mean(trimmed)
    else:
        avg_latency = float("inf")
        min_latency = float("inf")
        max_latency = float("inf")
        jitter = float("inf")

    test_method = "tls" if use_tls else "tcp"

    return {
        **node,
        "avg_latency_ms": round(avg_latency, 1),
        "min_latency_ms": round(min_latency, 1),
        "max_latency_ms": round(max_latency, 1),
        "jitter_ms": round(jitter, 1),
        "loss_rate": round(loss_rate, 3),
        "success_count": len(successes),
        "total_count": total,
        "is_cdn": cdn_detected,
        "dns_time_ms": round(dns_time_ms, 1),
        "test_method": test_method,
    }


def batch_test_nodes(nodes):
    """并发测试所有节点"""
    config = TEST_CONFIG
    results = []
    total = len(nodes)

    rounds = config.get("test_rounds", 3)
    logger.info(f"开始测速，共 {total} 个节点，{rounds} 轮 × {config['tcp_ping_count']} 次/轮")
    logger.info(f"  并发 {config['max_workers']} 线程，TLS测试: {'开启' if config.get('tls_test_enabled') else '关闭'}")

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
                # 实时显示测试结果
                if result["avg_latency_ms"] < float("inf"):
                    cdn_tag = " [CDN]" if result.get("is_cdn") else ""
                    logger.debug(
                        f"  {result['address']}:{result['port']} → "
                        f"{result['avg_latency_ms']}ms (±{result['jitter_ms']}ms) "
                        f"丢包{result['loss_rate']*100:.0f}% "
                        f"[{result['test_method']}]{cdn_tag}"
                    )
            except Exception as e:
                logger.debug(f"测试异常: {e}")

            if done_count % 20 == 0 or done_count == total:
                logger.info(f"  进度: {done_count}/{total}")

    return results


# ============================================================
# 第三步 B：xray-core 真实代理测速
# ============================================================

XRAY_VERSION = "1.8.24"
XRAY_DIR = Path(__file__).parent / ".xray"


def get_xray_download_url():
    """根据当前系统架构生成 xray-core 下载 URL"""
    system = platform.system().lower()
    machine = platform.machine().lower()

    if system == "linux":
        if machine in ("x86_64", "amd64"):
            arch = "linux-64"
        elif machine in ("aarch64", "arm64"):
            arch = "linux-arm64-v8a"
        else:
            arch = "linux-64"
    elif system == "darwin":
        if machine in ("arm64", "aarch64"):
            arch = "macos-arm64-v8a"
        else:
            arch = "macos-64"
    elif system == "windows":
        arch = "windows-64"
    else:
        arch = "linux-64"

    return (
        f"https://github.com/XTLS/Xray-core/releases/download/"
        f"v{XRAY_VERSION}/Xray-{arch}.zip"
    )


def ensure_xray_binary():
    """确保 xray 二进制文件存在，不存在则自动下载"""
    xray_bin = XRAY_DIR / ("xray.exe" if platform.system() == "Windows" else "xray")

    if xray_bin.exists():
        logger.info(f"  xray-core 已存在: {xray_bin}")
        return str(xray_bin)

    XRAY_DIR.mkdir(parents=True, exist_ok=True)
    url = get_xray_download_url()
    zip_path = XRAY_DIR / "xray.zip"

    logger.info(f"  下载 xray-core v{XRAY_VERSION} ...")
    logger.info(f"  URL: {url}")

    resp = requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()
    with open(zip_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info(f"  解压 xray-core ...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(XRAY_DIR)

    zip_path.unlink()

    # 赋予执行权限
    if platform.system() != "Windows":
        os.chmod(xray_bin, 0o755)

    logger.info(f"  xray-core 就绪: {xray_bin}")
    return str(xray_bin)


def build_xray_outbound(node, tag="proxy"):
    """
    根据节点信息构建单个 xray outbound 配置块
    返回 outbound dict，失败返回 None
    """
    protocol = node["protocol"]
    address = node["address"]
    port = node["port"]
    raw_uri = node["raw"]

    if protocol == "vmess":
        try:
            decoded = json.loads(decode_base64(raw_uri.replace("vmess://", "")))
        except Exception:
            return None

        net = decoded.get("net", "tcp")
        tls_val = decoded.get("tls", "")

        stream = {"network": net}
        if net == "ws":
            stream["wsSettings"] = {
                "path": decoded.get("path", "/"),
                "headers": {"Host": decoded.get("host", address)},
            }
        elif net == "grpc":
            stream["grpcSettings"] = {"serviceName": decoded.get("path", "")}

        if tls_val == "tls":
            stream["security"] = "tls"
            stream["tlsSettings"] = {
                "serverName": decoded.get("sni") or decoded.get("host") or address,
                "allowInsecure": True,
            }

        outbound = {
            "tag": tag,
            "protocol": "vmess",
            "settings": {"vnext": [{
                "address": address,
                "port": port,
                "users": [{
                    "id": decoded.get("id", ""),
                    "alterId": int(decoded.get("aid", 0)),
                    "security": decoded.get("scy", "auto"),
                }],
            }]},
            "streamSettings": stream,
        }

    elif protocol == "vless":
        parsed = urllib.parse.urlparse(raw_uri)
        params = dict(urllib.parse.parse_qsl(parsed.query))
        uuid = parsed.username or ""

        net = params.get("type", "tcp")
        security = params.get("security", "none")

        stream = {"network": net}
        if net == "ws":
            stream["wsSettings"] = {
                "path": params.get("path", "/"),
                "headers": {"Host": params.get("host", address)},
            }
        elif net == "grpc":
            stream["grpcSettings"] = {"serviceName": params.get("serviceName", "")}
        elif net == "httpupgrade":
            stream["httpupgradeSettings"] = {
                "path": params.get("path", "/"),
                "host": params.get("host", address),
            }

        if security == "tls":
            stream["security"] = "tls"
            stream["tlsSettings"] = {
                "serverName": params.get("sni", address),
                "allowInsecure": True,
            }
        elif security == "reality":
            stream["security"] = "reality"
            stream["realitySettings"] = {
                "serverName": params.get("sni", ""),
                "fingerprint": params.get("fp", "chrome"),
                "publicKey": params.get("pbk", ""),
                "shortId": params.get("sid", ""),
            }

        flow = params.get("flow", "")
        user = {"id": uuid, "encryption": "none"}
        if flow:
            user["flow"] = flow

        outbound = {
            "tag": tag,
            "protocol": "vless",
            "settings": {"vnext": [{
                "address": address,
                "port": port,
                "users": [user],
            }]},
            "streamSettings": stream,
        }

    elif protocol == "trojan":
        parsed = urllib.parse.urlparse(raw_uri)
        params = dict(urllib.parse.parse_qsl(parsed.query))
        password = parsed.username or ""

        net = params.get("type", "tcp")
        security = params.get("security", "tls")

        stream = {"network": net}
        if net == "ws":
            stream["wsSettings"] = {
                "path": params.get("path", "/"),
                "headers": {"Host": params.get("host", address)},
            }
        elif net == "grpc":
            stream["grpcSettings"] = {"serviceName": params.get("serviceName", "")}

        if security == "tls" or security == "":
            stream["security"] = "tls"
            stream["tlsSettings"] = {
                "serverName": params.get("sni", address),
                "allowInsecure": True,
            }

        outbound = {
            "tag": tag,
            "protocol": "trojan",
            "settings": {"servers": [{
                "address": address,
                "port": port,
                "password": password,
            }]},
            "streamSettings": stream,
        }

    elif protocol == "ss":
        # 解析 ss:// URI
        uri_clean = raw_uri.replace("ss://", "")
        if "#" in uri_clean:
            uri_clean = uri_clean.rsplit("#", 1)[0]

        if "@" in uri_clean:
            method_pass_b64, _ = uri_clean.split("@", 1)
            try:
                method_pass = decode_base64(method_pass_b64)
            except Exception:
                method_pass = method_pass_b64
            if ":" in method_pass:
                method, password = method_pass.split(":", 1)
            else:
                return None
        else:
            decoded_ss = decode_base64(uri_clean)
            if "@" in decoded_ss:
                method_pass, _ = decoded_ss.split("@", 1)
                if ":" in method_pass:
                    method, password = method_pass.split(":", 1)
                else:
                    return None
            else:
                return None

        outbound = {
            "tag": tag,
            "protocol": "shadowsocks",
            "settings": {"servers": [{
                "address": address,
                "port": port,
                "method": method,
                "password": password,
            }]},
        }
    else:
        return None

    return outbound


def find_free_ports(count):
    """一次性分配 count 个可用的本地端口（避免重复）"""
    sockets = []
    ports = []
    for _ in range(count):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        ports.append(s.getsockname()[1])
        sockets.append(s)
    # 全部绑定完再释放，避免被其他进程抢占
    for s in sockets:
        s.close()
    return ports


def build_xray_multi_config(nodes_with_ports):
    """
    为多个节点生成一个合并的 xray 配置：
    - 每个节点一个 inbound（不同端口） + 一个 outbound
    - 用 routing 规则按 inboundTag 分发到对应 outbound
    返回 (config_dict, failed_indices)
    """
    inbounds = []
    outbounds = []
    routing_rules = []
    failed_indices = set()

    for idx, (node, socks_port) in enumerate(nodes_with_ports):
        in_tag = f"in-{idx}"
        out_tag = f"out-{idx}"

        outbound = build_xray_outbound(node, tag=out_tag)
        if outbound is None:
            failed_indices.add(idx)
            continue

        inbounds.append({
            "tag": in_tag,
            "port": socks_port,
            "listen": "127.0.0.1",
            "protocol": "socks",
            "settings": {"udp": False},
        })
        outbounds.append(outbound)
        routing_rules.append({
            "type": "field",
            "inboundTag": [in_tag],
            "outboundTag": out_tag,
        })

    if not outbounds:
        return None, failed_indices

    # 添加一个 direct 出站作为兜底
    outbounds.append({"tag": "direct", "protocol": "freedom"})

    config = {
        "log": {"loglevel": "error"},
        "inbounds": inbounds,
        "outbounds": outbounds,
        "routing": {
            "domainStrategy": "AsIs",
            "rules": routing_rules,
        },
    }

    return config, failed_indices


def xray_test_via_proxy(node, socks_port, test_count=3, timeout=10):
    """
    通过已启动的 xray 代理端口测试单个节点的延迟
    （不负责管理 xray 进程，只发 HTTP 请求）
    """
    proxy = f"socks5h://127.0.0.1:{socks_port}"
    test_url = TEST_CONFIG.get("xray_test_url", "http://www.gstatic.com/generate_204")
    latencies = []

    for i in range(test_count):
        try:
            start = time.time()
            resp = requests.get(
                test_url,
                proxies={"http": proxy, "https": proxy},
                timeout=timeout,
                headers=HEADERS,
            )
            elapsed = (time.time() - start) * 1000
            if resp.status_code in (200, 204):
                latencies.append(elapsed)
            else:
                latencies.append(None)
        except Exception:
            latencies.append(None)

        if i < test_count - 1:
            time.sleep(0.5)

    successes = [l for l in latencies if l is not None]
    if successes:
        avg = statistics.mean(successes)
        jitter = statistics.stdev(successes) if len(successes) > 1 else 0
        return {
            **node,
            "xray_ok": True,
            "xray_avg_ms": round(avg, 1),
            "xray_min_ms": round(min(successes), 1),
            "xray_max_ms": round(max(successes), 1),
            "xray_jitter_ms": round(jitter, 1),
            "xray_success": len(successes),
            "xray_total": test_count,
            "xray_latencies": [round(l, 1) if l else None for l in latencies],
            "xray_error": "",
        }
    else:
        return {
            **node, "xray_ok": False, "xray_avg_ms": float("inf"),
            "xray_latencies": latencies, "xray_error": "all_requests_failed",
        }


def batch_xray_test(xray_bin, candidate_nodes):
    """
    单进程多节点并发测速：
    1. 为所有候选节点分配端口，生成一个合并配置
    2. 启动 1 个 xray 进程（所有节点共享）
    3. 并发通过各端口测速
    4. 关闭进程，清理资源
    """
    config = TEST_CONFIG
    total = len(candidate_nodes)
    max_workers = config.get("xray_max_workers", 5)
    test_count = config.get("xray_test_count", 3)
    timeout = config.get("xray_test_timeout", 10)
    startup_wait = config.get("xray_startup_wait", 2)

    logger.info(f"  xray 真实代理测速（单进程模式）：{total} 个候选，并发 {max_workers}")
    logger.info(f"  每节点 {test_count} 次请求，超时 {timeout}s")

    # 1. 分配端口
    ports = find_free_ports(total)
    nodes_with_ports = list(zip(candidate_nodes, ports))

    # 2. 生成合并配置
    xray_config, failed_indices = build_xray_multi_config(nodes_with_ports)

    results = []
    # 记录配置构建失败的节点
    for idx in failed_indices:
        node = candidate_nodes[idx]
        results.append({
            **node, "xray_ok": False, "xray_avg_ms": float("inf"),
            "xray_latencies": [], "xray_error": "config_build_failed",
        })

    if xray_config is None:
        logger.warning("  所有节点配置构建失败，跳过 xray 测速")
        return results

    # 3. 写配置 & 启动 xray
    tmp_dir = tempfile.mkdtemp(prefix="xray_multi_")
    config_file = Path(tmp_dir) / "config.json"
    config_file.write_text(json.dumps(xray_config, indent=2), encoding="utf-8")

    xray_proc = None
    try:
        # 先验证 xray 二进制可执行性
        try:
            version_result = subprocess.run(
                [xray_bin, "version"],
                capture_output=True, timeout=10,
            )
            logger.info(f"  xray 版本: {version_result.stdout.decode(errors='ignore').splitlines()[0] if version_result.stdout else '未知'}")
            if version_result.returncode != 0:
                err_msg = version_result.stderr.decode(errors="ignore")[:300]
                logger.error(f"  xray 二进制不可用: {err_msg}")
                for idx, (node, _) in enumerate(nodes_with_ports):
                    if idx not in failed_indices:
                        results.append({
                            **node, "xray_ok": False, "xray_avg_ms": float("inf"),
                            "xray_latencies": [], "xray_error": f"xray_binary_invalid: {err_msg[:100]}",
                        })
                return results
        except Exception as ve:
            logger.error(f"  xray 二进制验证失败: {ve}")

        # 启动 xray 进程，同时捕获 stdout 和 stderr 以便调试
        xray_proc = subprocess.Popen(
            [xray_bin, "run", "-c", str(config_file)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid if platform.system() != "Windows" else None,
        )

        # 等待 xray 启动，逐步检查（最多等 startup_wait * 2 秒）
        max_wait = startup_wait * 2
        waited = 0
        check_interval = 0.5
        while waited < max_wait:
            time.sleep(check_interval)
            waited += check_interval
            if xray_proc.poll() is not None:
                break
            # 尝试连接第一个节点的端口来确认 xray 已就绪
            if waited >= startup_wait:
                first_testable = next(
                    ((idx, port) for idx, (_, port) in enumerate(nodes_with_ports) if idx not in failed_indices),
                    None
                )
                if first_testable:
                    try:
                        test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        test_sock.settimeout(1)
                        test_sock.connect(("127.0.0.1", first_testable[1]))
                        test_sock.close()
                        logger.info(f"  xray 端口就绪（等待 {waited:.1f}s）")
                        break
                    except Exception:
                        pass

        if xray_proc.poll() is not None:
            stderr_out = xray_proc.stderr.read().decode(errors="ignore")[:500]
            stdout_out = xray_proc.stdout.read().decode(errors="ignore")[:500]
            exit_code = xray_proc.returncode
            logger.error(f"  xray 进程启动失败 (exit_code={exit_code})")
            if stderr_out:
                logger.error(f"  stderr: {stderr_out}")
            if stdout_out:
                logger.error(f"  stdout: {stdout_out}")
            # 记录配置文件内容（前500字符）用于调试
            try:
                cfg_preview = config_file.read_text()[:500]
                logger.error(f"  配置预览: {cfg_preview}")
            except Exception:
                pass
            for idx, (node, _) in enumerate(nodes_with_ports):
                if idx not in failed_indices:
                    results.append({
                        **node, "xray_ok": False, "xray_avg_ms": float("inf"),
                        "xray_latencies": [], "xray_error": f"xray_crashed(exit={exit_code}): {stderr_out[:100]}",
                    })
            return results

        logger.info(f"  xray 进程已启动 (PID={xray_proc.pid})，开始并发测速...")

        # 4. 并发测所有节点
        testable = [(idx, node, port) for idx, (node, port) in enumerate(nodes_with_ports)
                     if idx not in failed_indices]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    xray_test_via_proxy, node, port, test_count, timeout
                ): (idx, node)
                for idx, node, port in testable
            }

            done_count = 0
            for future in as_completed(futures):
                done_count += 1
                idx, node = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    status = "✓" if result.get("xray_ok") else "✗"
                    avg = result.get("xray_avg_ms", "∞")
                    err = result.get("xray_error", "")
                    name = result.get("name", "")[:20]
                    logger.info(
                        f"  [{done_count}/{len(testable)}] {status} {node['address']}:{node['port']} "
                        f"→ {avg}ms {f'({err})' if err else ''} {name}"
                    )
                except Exception as e:
                    results.append({
                        **node, "xray_ok": False, "xray_avg_ms": float("inf"),
                        "xray_latencies": [], "xray_error": str(e),
                    })
                    logger.warning(f"  [{done_count}/{len(testable)}] 测试异常: {e}")

    except Exception as e:
        logger.error(f"  xray 测速整体异常: {e}")
    finally:
        # 清理：杀掉唯一的 xray 进程
        if xray_proc and xray_proc.poll() is None:
            try:
                if platform.system() != "Windows":
                    os.killpg(os.getpgid(xray_proc.pid), signal.SIGTERM)
                else:
                    xray_proc.terminate()
                xray_proc.wait(timeout=5)
            except Exception:
                try:
                    xray_proc.kill()
                except Exception:
                    pass
        shutil.rmtree(tmp_dir, ignore_errors=True)

    logger.info(f"  xray 测速完成，共 {len(results)} 个结果")
    return results


# ============================================================
# 第四步：筛选 TOP N + 生成订阅
# ============================================================

def select_best_nodes(test_results, top_n=10, max_latency=2000, max_loss=0.4):
    """
    筛选最优节点 —— 综合评分模型
    维度：延迟(40%) + 稳定性/抖动(25%) + 丢包率(25%) + CDN惩罚(10%)
    """
    # 过滤不可用节点
    valid = [
        r for r in test_results
        if r["avg_latency_ms"] < max_latency
        and r["avg_latency_ms"] != float("inf")
        and r["loss_rate"] <= max_loss
    ]

    if not valid:
        logger.warning("无满足条件的节点，放宽标准重试...")
        valid = [
            r for r in test_results
            if r["avg_latency_ms"] < float("inf") and r["loss_rate"] < 1.0
        ]

    if not valid:
        return []

    # 找到最大值用于归一化
    max_avg = max(n["avg_latency_ms"] for n in valid) or 1
    max_jitter = max(n.get("jitter_ms", 0) for n in valid) or 1

    for node in valid:
        avg = node["avg_latency_ms"]
        jitter = node.get("jitter_ms", 0)
        loss = node["loss_rate"]
        is_cdn = node.get("is_cdn", False)

        # 归一化到 0~1 范围
        norm_latency = avg / max_avg
        norm_jitter = jitter / max_jitter if max_jitter > 0 else 0
        norm_loss = loss

        # CDN 惩罚：CDN IP 的 TCP 延迟不可信，加分惩罚
        cdn_penalty = 0.3 if is_cdn else 0

        # 综合评分（越低越好）
        #   延迟权重 40%：低延迟优先
        #   抖动权重 25%：稳定性高优先
        #   丢包权重 25%：低丢包优先
        #   CDN惩罚 10%：CDN IP 降优先级
        score = (
            norm_latency * 0.40
            + norm_jitter * 0.25
            + norm_loss * 0.25
            + cdn_penalty * 0.10
        )
        node["score"] = round(score, 4)

        # 稳定性等级标记
        if jitter < 20 and loss == 0:
            node["stability"] = "★★★"
        elif jitter < 50 and loss <= 0.1:
            node["stability"] = "★★"
        elif jitter < 100 and loss <= 0.3:
            node["stability"] = "★"
        else:
            node["stability"] = "☆"

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
    parser = argparse.ArgumentParser(description="V2Ray 订阅聚合 - 采集/去重/真实测速/筛选")
    parser.add_argument("--top", type=int, default=TEST_CONFIG["top_n"], help="筛选最优节点数 (默认10)")
    parser.add_argument("--workers", type=int, default=TEST_CONFIG["max_workers"], help="并发线程数 (默认30)")
    parser.add_argument("--output", type=str, default=None, help="输出目录 (默认 ./output)")
    parser.add_argument("--ping-count", type=int, default=TEST_CONFIG["tcp_ping_count"], help="每轮ping次数 (默认5)")
    parser.add_argument("--rounds", type=int, default=TEST_CONFIG.get("test_rounds", 3), help="测速轮次 (默认3)")
    parser.add_argument("--no-tls", action="store_true", help="禁用 TLS 握手测试（只用 TCP ping）")
    parser.add_argument("--no-xray", action="store_true", help="禁用 xray-core 真实代理测试（只用 TCP/TLS 初筛）")
    parser.add_argument("--xray-candidates", type=int, default=TEST_CONFIG.get("xray_candidate_count", 30),
                        help="初筛后进入 xray 测试的候选节点数 (默认30)")
    parser.add_argument("--timeout", type=int, default=TEST_CONFIG["tcp_ping_timeout"], help="单次超时秒数 (默认5)")
    parser.add_argument("--verbose", "-v", action="store_true", help="显示详细测速日志")
    args = parser.parse_args()

    TEST_CONFIG["top_n"] = args.top
    TEST_CONFIG["max_workers"] = args.workers
    TEST_CONFIG["tcp_ping_count"] = args.ping_count
    TEST_CONFIG["test_rounds"] = args.rounds
    TEST_CONFIG["tcp_ping_timeout"] = args.timeout
    if args.no_tls:
        TEST_CONFIG["tls_test_enabled"] = False
    if args.no_xray:
        TEST_CONFIG["xray_enabled"] = False
    TEST_CONFIG["xray_candidate_count"] = args.xray_candidates
    if args.verbose:
        logging.getLogger(__name__).setLevel(logging.DEBUG)

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

    # Step 3: 阶段一 —— TCP/TLS 快速初筛
    logger.info("\n[3/5] 阶段一：TCP/TLS 快速初筛...")
    test_results = batch_test_nodes(unique_nodes)

    # 初筛排序，选出候选节点
    logger.info("\n[4/5] 初筛结果排序...")
    preliminary_best = select_best_nodes(
        test_results,
        top_n=TEST_CONFIG.get("xray_candidate_count", 30),
        max_latency=TEST_CONFIG["max_latency_ms"],
        max_loss=TEST_CONFIG["max_loss_rate"],
    )

    if not preliminary_best:
        logger.warning("初筛无可用节点，取延迟最低的作为候选...")
        test_results.sort(key=lambda x: x["avg_latency_ms"])
        preliminary_best = [r for r in test_results if r["avg_latency_ms"] < float("inf")]
        preliminary_best = preliminary_best[:TEST_CONFIG.get("xray_candidate_count", 30)]

    logger.info(f"  初筛通过 {len(preliminary_best)} 个候选节点")

    # Step 4: 阶段二 —— xray-core 真实代理验证
    if TEST_CONFIG.get("xray_enabled", True) and preliminary_best:
        logger.info(f"\n[5/5] 阶段二：xray-core 真实代理测速...")
        try:
            xray_bin = ensure_xray_binary()
            xray_results = batch_xray_test(xray_bin, preliminary_best)

            # 只保留 xray 测试通过的节点
            xray_ok_nodes = [r for r in xray_results if r.get("xray_ok")]
            xray_fail_count = len(xray_results) - len(xray_ok_nodes)

            logger.info(f"\n  xray 测试完成: {len(xray_ok_nodes)} 可用 / {xray_fail_count} 不可用")

            if xray_ok_nodes:
                # 用 xray 真实延迟重新评分
                for node in xray_ok_nodes:
                    # xray 延迟作为主要指标，覆盖 TCP/TLS 延迟
                    node["real_latency_ms"] = node["xray_avg_ms"]
                    node["avg_latency_ms"] = node["xray_avg_ms"]
                    node["jitter_ms"] = node.get("xray_jitter_ms", 0)

                # 重新筛选排序
                best_nodes = select_best_nodes(
                    xray_ok_nodes,
                    top_n=TEST_CONFIG["top_n"],
                    max_latency=TEST_CONFIG["max_latency_ms"],
                    max_loss=1.0,  # xray 已经验证过可用，放宽丢包限制
                )
            else:
                logger.warning("xray 测试全部失败，回退使用初筛结果")
                best_nodes = preliminary_best[:TEST_CONFIG["top_n"]]

        except Exception as e:
            logger.error(f"xray-core 测速失败: {e}，回退使用初筛结果")
            best_nodes = preliminary_best[:TEST_CONFIG["top_n"]]
    else:
        if not TEST_CONFIG.get("xray_enabled", True):
            logger.info("\n[5/5] xray-core 测试已禁用，使用初筛结果")
        best_nodes = preliminary_best[:TEST_CONFIG["top_n"]]

    if not best_nodes:
        logger.warning("未筛选到可用节点")
        test_results.sort(key=lambda x: x["avg_latency_ms"])
        best_nodes = test_results[:TEST_CONFIG["top_n"]]

    # 输出结果
    xray_mode = TEST_CONFIG.get("xray_enabled", True)
    logger.info(f"\n{'='*80}")
    logger.info(f"最优 {len(best_nodes)} 个节点（{'xray 真实代理' if xray_mode else '初筛'}评分排序）：")
    if xray_mode and best_nodes and best_nodes[0].get("xray_ok") is not None:
        logger.info(f"{'序号':<4} {'协议':<7} {'地址':<30} {'真实延迟':<10} {'抖动':<8} "
                    f"{'稳定':<5} {'CDN':<5} {'名称'}")
        logger.info(f"{'-'*110}")
        for i, node in enumerate(best_nodes, 1):
            cdn_tag = "是" if node.get("is_cdn") else "-"
            stability = node.get("stability", "?")
            xray_avg = node.get("xray_avg_ms", "-")
            logger.info(
                f"{i:<4} {node['protocol']:<7} {node['address']}:{node['port']:<20} "
                f"{xray_avg:<10} {node.get('jitter_ms', 0):<8} "
                f"{stability:<5} {cdn_tag:<5} {node.get('name', '')[:30]}"
            )
    else:
        logger.info(f"{'序号':<4} {'协议':<7} {'地址':<35} {'延迟(ms)':<10} {'抖动':<8} "
                    f"{'丢包':<6} {'稳定':<5} {'CDN':<5} {'方式':<5} {'名称'}")
        logger.info(f"{'-'*120}")
        for i, node in enumerate(best_nodes, 1):
            cdn_tag = "是" if node.get("is_cdn") else "-"
            stability = node.get("stability", "?")
            test_method = node.get("test_method", "tcp")
            logger.info(
                f"{i:<4} {node['protocol']:<7} {node['address']}:{node['port']:<25} "
                f"{node['avg_latency_ms']:<10} {node.get('jitter_ms', 0):<8} "
                f"{node['loss_rate']*100:.0f}%{'':<3} {stability:<5} {cdn_tag:<5} "
                f"{test_method:<5} {node.get('name', '')[:30]}"
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
        "test_config": {
            "rounds": TEST_CONFIG.get("test_rounds", 3),
            "pings_per_round": TEST_CONFIG["tcp_ping_count"],
            "timeout_s": TEST_CONFIG["tcp_ping_timeout"],
            "tls_test": TEST_CONFIG.get("tls_test_enabled", False),
            "xray_enabled": TEST_CONFIG.get("xray_enabled", True),
            "xray_test_count": TEST_CONFIG.get("xray_test_count", 3),
            "xray_test_url": TEST_CONFIG.get("xray_test_url", ""),
        },
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
                "tcp_latency_ms": n.get("min_latency_ms", n.get("avg_latency_ms", 0)),
                "xray_real_latency_ms": n.get("xray_avg_ms", None),
                "xray_min_ms": n.get("xray_min_ms", None),
                "xray_max_ms": n.get("xray_max_ms", None),
                "jitter_ms": n.get("jitter_ms", 0),
                "loss_rate": n.get("loss_rate", 0),
                "is_cdn": n.get("is_cdn", False),
                "xray_ok": n.get("xray_ok", None),
                "stability": n.get("stability", "?"),
                "score": n.get("score", 0),
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
