#!/usr/bin/env python3
"""
V2Ray 订阅聚合平台
功能：多源采集 → 解析去重 → TCP/TLS初筛 → xray真实代理验证 → 输出所有可用节点订阅
策略：不限数量，只筛可用性，让客户端自行测速选最优
部署：GitHub Actions 定时执行 / 云服务器 cron
"""

import os
import re
import sys
import json
import time
import base64
import shutil
import socket
import signal
import logging
import zipfile
import argparse
import platform
import tempfile
import statistics
import subprocess
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import sources as source_module

try:
    import requests
except ImportError:
    print("请安装 requests: pip install requests")
    sys.exit(1)

# ============================================================
# 配置
# ============================================================

# 测速配置
TEST_CONFIG = {
    "tcp_ping_count": 2,        # 每个节点 TCP ping 次数（初筛阶段，减少次数加快速度）
    "tcp_ping_timeout": 3,      # 单次超时（秒）
    "max_workers": 500,         # 并发测试线程数
    "max_latency_ms": 2000,     # 最大可接受延迟（ms）
    "max_loss_rate": 0.4,       # 最大可接受丢包率
    "test_rounds": 1,           # TCP/TLS 初筛轮次（减少，主要靠 xray 二次验证）
    "round_interval": 1,        # 轮次间隔（秒）
    "tls_test_enabled": True,   # 是否进行 TLS 握手测试
    "dns_resolve_first": True,  # 先 DNS 解析
    # xray-core 真实代理测速配置
    # xray-core 真实代理测速始终启用
    "xray_test_count": 2,       # 每个节点通过代理请求次数
    "xray_test_timeout": 8,     # 代理请求超时（秒）
    "xray_startup_wait": 2,     # xray 进程启动等待（秒）
    "xray_max_workers": 100,    # xray 测试并发数（单进程模式，所有节点共用一个进程）
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
# 第一步：多源采集（实现在 sources/ 模块中，每个源一个文件）
# ============================================================


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


# 国家关键词映射表（模块级常量，避免每次调用重建）
_COUNTRY_MAP = {
    # 中文名称
    "美国": "美国", "香港": "香港", "台湾": "台湾", "日本": "日本",
    "韩国": "韩国", "新加坡": "新加坡", "英国": "英国", "德国": "德国",
    "法国": "法国", "加拿大": "加拿大", "澳大利亚": "澳大利亚",
    "澳洲": "澳大利亚", "印度": "印度", "俄罗斯": "俄罗斯",
    "荷兰": "荷兰", "巴西": "巴西", "土耳其": "土耳其",
    "阿根廷": "阿根廷", "越南": "越南", "泰国": "泰国",
    "马来西亚": "马来西亚", "印尼": "印尼", "菲律宾": "菲律宾",
    "意大利": "意大利", "西班牙": "西班牙", "瑞士": "瑞士",
    "瑞典": "瑞典", "挪威": "挪威", "芬兰": "芬兰",
    "波兰": "波兰", "乌克兰": "乌克兰", "以色列": "以色列",
    "南非": "南非", "墨西哥": "墨西哥", "智利": "智利",
    "哥伦比亚": "哥伦比亚", "爱尔兰": "爱尔兰", "新西兰": "新西兰",
    "埃及": "埃及", "罗马尼亚": "罗马尼亚", "捷克": "捷克",
    "匈牙利": "匈牙利", "奥地利": "奥地利", "比利时": "比利时",
    "丹麦": "丹麦", "葡萄牙": "葡萄牙", "希腊": "希腊",
    "哈萨克斯坦": "哈萨克斯坦", "巴基斯坦": "巴基斯坦",
    "孟加拉": "孟加拉", "尼日利亚": "尼日利亚",
    # 英文国家代码 / 名称
    "US": "美国", "USA": "美国", "United States": "美国", "America": "美国",
    "HK": "香港", "Hong Kong": "香港", "Hongkong": "香港",
    "TW": "台湾", "Taiwan": "台湾",
    "JP": "日本", "Japan": "日本",
    "KR": "韩国", "Korea": "韩国", "South Korea": "韩国",
    "SG": "新加坡", "Singapore": "新加坡",
    "UK": "英国", "GB": "英国", "United Kingdom": "英国", "England": "英国",
    "DE": "德国", "Germany": "德国",
    "FR": "法国", "France": "法国",
    "CA": "加拿大", "Canada": "加拿大",
    "AU": "澳大利亚", "Australia": "澳大利亚",
    "IN": "印度", "India": "印度",
    "RU": "俄罗斯", "Russia": "俄罗斯",
    "NL": "荷兰", "Netherlands": "荷兰",
    "BR": "巴西", "Brazil": "巴西",
    "TR": "土耳其", "Turkey": "土耳其", "Türkiye": "土耳其",
    "AR": "阿根廷", "Argentina": "阿根廷",
    "VN": "越南", "Vietnam": "越南",
    "TH": "泰国", "Thailand": "泰国",
    "MY": "马来西亚", "Malaysia": "马来西亚",
    "ID": "印尼", "Indonesia": "印尼",
    "PH": "菲律宾", "Philippines": "菲律宾",
    "IT": "意大利", "Italy": "意大利",
    "ES": "西班牙", "Spain": "西班牙",
    "CH": "瑞士", "Switzerland": "瑞士",
    "SE": "瑞典", "Sweden": "瑞典",
    "NO": "挪威", "Norway": "挪威",
    "FI": "芬兰", "Finland": "芬兰",
    "PL": "波兰", "Poland": "波兰",
    "UA": "乌克兰", "Ukraine": "乌克兰",
    "IL": "以色列", "Israel": "以色列",
    "ZA": "南非", "South Africa": "南非",
    "MX": "墨西哥", "Mexico": "墨西哥",
    "CL": "智利", "Chile": "智利",
    "CO": "哥伦比亚", "Colombia": "哥伦比亚",
    "IE": "爱尔兰", "Ireland": "爱尔兰",
    "NZ": "新西兰", "New Zealand": "新西兰",
}

# 国旗 emoji 映射
_FLAG_MAP = {
    "🇺🇸": "美国", "🇭🇰": "香港", "🇹🇼": "台湾", "🇯🇵": "日本",
    "🇰🇷": "韩国", "🇸🇬": "新加坡", "🇬🇧": "英国", "🇩🇪": "德国",
    "🇫🇷": "法国", "🇨🇦": "加拿大", "🇦🇺": "澳大利亚", "🇮🇳": "印度",
    "🇷🇺": "俄罗斯", "🇳🇱": "荷兰", "🇧🇷": "巴西", "🇹🇷": "土耳其",
    "🇦🇷": "阿根廷", "🇻🇳": "越南", "🇹🇭": "泰国", "🇲🇾": "马来西亚",
    "🇮🇩": "印尼", "🇵🇭": "菲律宾", "🇮🇹": "意大利", "🇪🇸": "西班牙",
    "🇨🇭": "瑞士", "🇸🇪": "瑞典", "🇳🇴": "挪威", "🇫🇮": "芬兰",
    "🇵🇱": "波兰", "🇺🇦": "乌克兰", "🇮🇱": "以色列", "🇿🇦": "南非",
    "🇲🇽": "墨西哥",
}

# 2字母国家代码列表（从 _COUNTRY_MAP 中提取，避免重复维护）
_TWO_LETTER_CODES = [k for k in _COUNTRY_MAP if len(k) == 2 and k.isalpha() and k.isupper()]


def _extract_country(original_name):
    """从节点名称中提取国家/地区，返回中文名或 "未知" """
    if not original_name:
        return "未知"

    # 先检查国旗 emoji
    for flag, country in _FLAG_MAP.items():
        if flag in original_name:
            return country

    # 优先匹配中文及英文长关键词
    for keyword, country in _COUNTRY_MAP.items():
        if len(keyword) >= 2 and keyword in original_name:
            return country

    # 匹配2字母国家代码（需在开头且后接分隔符）
    name_upper = original_name.upper()
    for code in _TWO_LETTER_CODES:
        if re.match(rf'^{code}(?=[\W_]|$)', name_upper):
            return _COUNTRY_MAP[code]

    return "未知"


def parse_nodes(tagged_contents):
    """
    解析所有订阅内容为节点列表。
    参数：
        tagged_contents = [(source_name, raw_text, data_date), ...]
        data_date 格式为 "YYYY-MM-DD"，用于计算与今天的天数差
    当天数据节点 name 格式为 "[国家][源名称][IP:端口]"，如 "[美国][mibei77][1.1.1.1:443]"
    非当天数据节点加前缀如 "[-1][日本][v2raynode][2.2.2.2:8080]"（表示数据是1天前的）
    返回：(nodes, per_source_node_counts)
        - nodes: 节点列表
        - per_source_node_counts: {source_name: node_count} 每源解析到的节点数
    """
    from datetime import date

    nodes = []
    per_source_node_counts = {}
    today = date.today()
    parsers = {
        "vmess://": parse_vmess,
        "vless://": parse_vless,
        "ss://": parse_ss,
        "trojan://": parse_trojan,
    }

    for source_name, content, data_date in tagged_contents:
        source_node_count = 0
        # 计算数据日期与今天的天数差
        try:
            d = date.fromisoformat(data_date)
            day_offset = (d - today).days  # 0=今天, -1=昨天, -2=前天
        except (ValueError, TypeError):
            day_offset = 0

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
                        # 从原始名称中提取国家信息
                        country = _extract_country(node.get("name", ""))
                        # 节点名称格式：[国家][源名称][IP:端口]，非当天数据加 [-N] 前缀
                        if day_offset == 0:
                            node["name"] = f"[{country}][{source_name}][{node['address']}:{node['port']}]"
                        else:
                            node["name"] = f"[{day_offset}][{country}][{source_name}][{node['address']}:{node['port']}]"
                        node["source"] = source_name
                        node["country"] = country
                        node["day_offset"] = day_offset
                        # 同步更新 raw URI 中的名称（vmess 需要特殊处理）
                        node["raw"] = _rebuild_raw_with_name(node)
                        nodes.append(node)
                        source_node_count += 1
                    break

        per_source_node_counts[source_name] = (
            per_source_node_counts.get(source_name, 0) + source_node_count
        )

    return nodes, per_source_node_counts


def _rebuild_raw_with_name(node):
    """重建 raw URI，将节点名称（含源标记）写回到 URI 中"""
    protocol = node["protocol"]
    raw = node["raw"]
    new_name = node["name"]

    try:
        if protocol == "vmess":
            decoded_json = json.loads(decode_base64(raw.replace("vmess://", "")))
            decoded_json["ps"] = new_name
            new_b64 = base64.b64encode(
                json.dumps(decoded_json, ensure_ascii=False).encode("utf-8")
            ).decode("utf-8")
            return f"vmess://{new_b64}"
        else:
            # vless/trojan/ss 的名称都在 URI fragment (#名称)
            base_part = raw.rsplit("#", 1)[0] if "#" in raw else raw
            return f"{base_part}#{urllib.parse.quote(new_name)}"
    except Exception:
        pass

    return raw



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
                "dns_time_ms": 0,
                "test_method": "dns_fail",
            }

    # 2. 多轮测速
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

    # 3. 统计分析
    successes = [r for r in all_latencies if r is not None]
    total = len(all_latencies)
    loss_rate = 1.0 - len(successes) / total if total > 0 else 1.0

    if successes:
        avg_latency = statistics.mean(successes)
        min_latency = min(successes)
        max_latency = max(successes)
        jitter = statistics.stdev(successes) if len(successes) > 1 else 0
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
                    logger.debug(
                        f"  {result['address']}:{result['port']} → "
                        f"{result['avg_latency_ms']}ms (±{result['jitter_ms']}ms) "
                        f"丢包{result['loss_rate']*100:.0f}% "
                        f"[{result['test_method']}]"
                    )
            except Exception as e:
                logger.debug(f"测试异常: {e}")

            if done_count % 100 == 0 or done_count == total:
                logger.info(f"  进度: {done_count}/{total}")

    return results


# ============================================================
# 第三步 B：xray-core 真实代理测速
# ============================================================

XRAY_VERSION = "25.10.15"
XRAY_DIR = Path(__file__).parent / ".xray"


def get_xray_download_url():
    """根据当前系统架构生成 xray-core 下载 URL（仅支持 Linux）"""
    machine = platform.machine().lower()
    if machine in ("aarch64", "arm64"):
        arch = "linux-arm64-v8a"
    else:
        arch = "linux-64"

    return (
        f"https://github.com/XTLS/Xray-core/releases/download/"
        f"v{XRAY_VERSION}/Xray-{arch}.zip"
    )


def ensure_xray_binary():
    """确保 xray 二进制文件存在，不存在则自动下载"""
    xray_bin = XRAY_DIR / "xray"

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
    os.chmod(xray_bin, 0o755)

    logger.info(f"  xray-core 就绪: {xray_bin}")
    return str(xray_bin)


def _build_stream_settings(net, params, address):
    """
    构建 xray streamSettings（传输层配置）。
    params: dict-like 对象，支持 .get(key, default) 方法。
    返回 stream dict。
    """
    stream = {"network": "websocket" if net == "ws" else net}
    if net == "ws":
        stream["wsSettings"] = {
            "path": params.get("path", "/"),
            "host": params.get("host", address),
        }
    elif net == "grpc":
        stream["grpcSettings"] = {"serviceName": params.get("serviceName", "") or params.get("path", "")}
    elif net in ("h2", "http"):
        stream["network"] = "h2"
        h2_host = params.get("host", address)
        stream["httpSettings"] = {
            "path": params.get("path", "/"),
            "host": [h2_host] if h2_host else [address],
        }
    elif net == "httpupgrade":
        stream["httpupgradeSettings"] = {
            "path": params.get("path", "/"),
            "host": params.get("host", address),
        }
    elif net in ("xhttp", "splithttp"):
        stream["network"] = "xhttp"
        stream["xhttpSettings"] = {
            "path": params.get("path", "/"),
            "host": params.get("host", address),
        }
    return stream


def _apply_tls_settings(stream, params, address):
    """为 stream 添加 TLS 配置"""
    stream["security"] = "tls"
    sni = params.get("sni", "") or params.get("host", "") or address
    tls_settings = {"serverName": sni, "allowInsecure": True}
    tls_settings["fingerprint"] = params.get("fp", "") or "chrome"
    alpn = params.get("alpn", "")
    if alpn:
        tls_settings["alpn"] = alpn.split(",")
    stream["tlsSettings"] = tls_settings


# xray 支持的传输协议白名单
_SUPPORTED_NETWORKS = {"tcp", "ws", "grpc", "h2", "http", "kcp", "quic",
                       "httpupgrade", "splithttp", "xhttp"}

# SS 合法加密方法
_VALID_SS_METHODS = {
    "aes-128-gcm", "aes-256-gcm", "chacha20-poly1305",
    "chacha20-ietf-poly1305", "xchacha20-poly1305",
    "2022-blake3-aes-128-gcm", "2022-blake3-aes-256-gcm",
    "2022-blake3-chacha20-poly1305",
    "aes-128-cfb", "aes-192-cfb", "aes-256-cfb",
    "aes-128-ctr", "aes-192-ctr", "aes-256-ctr",
    "rc4-md5", "chacha20", "chacha20-ietf",
    "none", "plain",
}


def build_xray_outbound(node, tag="proxy"):
    """根据节点信息构建单个 xray outbound 配置块，失败返回 None"""
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
        if net not in _SUPPORTED_NETWORKS:
            logger.debug(f"  跳过不支持的传输协议: {net} ({address}:{port})")
            return None

        # vmess 的参数格式兼容：用 dict wrapper 统一 get 接口
        stream = _build_stream_settings(net, decoded, address)
        if decoded.get("tls", "") == "tls":
            _apply_tls_settings(stream, decoded, address)

        outbound = {
            "tag": tag, "protocol": "vmess",
            "settings": {"vnext": [{"address": address, "port": port, "users": [{
                "id": decoded.get("id", ""),
                "alterId": int(decoded.get("aid", 0)),
                "security": decoded.get("scy", "auto"),
            }]}]},
            "streamSettings": stream,
        }

    elif protocol in ("vless", "trojan"):
        parsed = urllib.parse.urlparse(raw_uri)
        params = dict(urllib.parse.parse_qsl(parsed.query))

        net = params.get("type", "tcp")
        security = params.get("security", "tls" if protocol == "trojan" else "none")

        if net not in _SUPPORTED_NETWORKS:
            logger.debug(f"  跳过不支持的传输协议: {net} ({address}:{port})")
            return None

        stream = _build_stream_settings(net, params, address)

        if security in ("tls", ""):
            _apply_tls_settings(stream, params, address)
        elif security == "reality":
            pbk = params.get("pbk", "")
            if not pbk:
                logger.debug(f"  跳过 REALITY 节点（缺少 publicKey）: {address}:{port}")
                return None
            stream["security"] = "reality"
            reality_settings = {
                "serverName": params.get("sni", ""),
                "fingerprint": params.get("fp", "chrome"),
                "publicKey": pbk,
                "shortId": params.get("sid", ""),
            }
            spx = params.get("spx", "")
            if spx:
                reality_settings["spiderX"] = spx
            stream["realitySettings"] = reality_settings

        if protocol == "vless":
            user = {"id": parsed.username or "", "encryption": "none"}
            flow = params.get("flow", "")
            if flow:
                user["flow"] = flow
            outbound = {
                "tag": tag, "protocol": "vless",
                "settings": {"vnext": [{"address": address, "port": port, "users": [user]}]},
                "streamSettings": stream,
            }
        else:  # trojan
            outbound = {
                "tag": tag, "protocol": "trojan",
                "settings": {"servers": [{"address": address, "port": port, "password": parsed.username or ""}]},
                "streamSettings": stream,
            }

    elif protocol == "ss":
        uri_clean = raw_uri.replace("ss://", "")
        if "#" in uri_clean:
            uri_clean = uri_clean.rsplit("#", 1)[0]

        # 解析 method:password
        method = password = None
        if "@" in uri_clean:
            method_pass_b64, _ = uri_clean.split("@", 1)
            method_pass = decode_base64(method_pass_b64) or method_pass_b64
            if ":" in method_pass:
                method, password = method_pass.split(":", 1)
        else:
            decoded_ss = decode_base64(uri_clean)
            if "@" in decoded_ss:
                method_pass, _ = decoded_ss.split("@", 1)
                if ":" in method_pass:
                    method, password = method_pass.split(":", 1)

        if not method or not password:
            return None
        if method not in _VALID_SS_METHODS:
            logger.debug(f"  跳过 SS 节点（不支持的加密方法 '{method}'）: {address}:{port}")
            return None

        outbound = {
            "tag": tag, "protocol": "shadowsocks",
            "settings": {"servers": [{"address": address, "port": port, "method": method, "password": password}]},
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

    # 兜底使用 blackhole 而非 freedom（direct），
    # 防止路由匹配失败时流量直连出去，导致测试结果不真实
    outbounds.append({"tag": "block", "protocol": "blackhole"})

    config = {
        "log": {"loglevel": "warning"},
        "inbounds": inbounds,
        "outbounds": outbounds,
        "routing": {
            "domainStrategy": "AsIs",
            "rules": routing_rules,
            # 未匹配到任何规则的流量走 blackhole（丢弃），
            # 确保每个测试请求必须经过对应的代理节点
            "defaultOutboundTag": "block",
        },
    }

    return config, failed_indices


def _xray_fail(node, error, latencies=None):
    """构造 xray 测试失败结果"""
    return {**node, "xray_ok": False, "xray_avg_ms": float("inf"),
            "xray_latencies": latencies or [], "xray_error": error}


def xray_test_via_proxy(node, socks_port, test_count=3, timeout=10, local_ip=None):
    """
    通过已启动的 xray 代理端口测试单个节点的真实可用性。
    验证：出口IP ≠ 本机IP → generate_204 连通 → 内容下载验证
    """
    proxy = f"socks5h://127.0.0.1:{socks_port}"
    proxies = {"http": proxy, "https": proxy}

    # ---- 阶段 0: 出口 IP 验证 ----
    exit_ip = None
    for url in _IP_CHECK_URLS:
        try:
            resp = requests.get(url, proxies=proxies, timeout=timeout, headers=HEADERS)
            if resp.status_code == 200:
                exit_ip = resp.text.strip()
                if exit_ip and len(exit_ip) < 50:
                    break
                exit_ip = None
        except Exception:
            continue

    if not exit_ip:
        return _xray_fail(node, "exit_ip_check_failed")
    if local_ip and exit_ip == local_ip:
        return _xray_fail(node, f"proxy_bypass_detected(exit_ip={exit_ip}==local_ip)")

    # ---- 阶段 1: 快速连通性测试 ----
    quick_urls = [("http://www.gstatic.com/generate_204", 204), ("http://cp.cloudflare.com/", 200)]
    latencies = []
    for i in range(test_count):
        url, expected_code = quick_urls[i % len(quick_urls)]
        try:
            start = time.time()
            resp = requests.get(url, proxies=proxies, timeout=timeout, headers=HEADERS)
            elapsed = (time.time() - start) * 1000
            latencies.append(elapsed if resp.status_code in (expected_code, 200, 204) else None)
        except Exception:
            latencies.append(None)
        if i < test_count - 1:
            time.sleep(0.3)

    quick_successes = [l for l in latencies if l is not None]
    if not quick_successes:
        return _xray_fail(node, "quick_check_all_failed", latencies)

    # ---- 阶段 2: 内容可达性验证 ----
    content_urls = [
        ("https://www.google.com/robots.txt", 200, "User-agent"),
        ("https://www.cloudflare.com/cdn-cgi/trace", 200, "warp="),
    ]
    content_latencies = []
    for url, expected_code, expected_content in content_urls:
        try:
            start = time.time()
            resp = requests.get(url, proxies=proxies, timeout=timeout, headers=HEADERS)
            elapsed = (time.time() - start) * 1000
            if resp.status_code == expected_code and (not expected_content or expected_content in resp.text[:2000]):
                content_latencies.append(elapsed)
                break
        except Exception:
            continue

    if not content_latencies:
        return _xray_fail(node, "content_verify_failed", latencies)

    # 合并所有延迟
    all_latencies = quick_successes + content_latencies
    avg = statistics.mean(all_latencies)
    jitter = statistics.stdev(all_latencies) if len(all_latencies) > 1 else 0

    return {
        **node,
        "xray_ok": True,
        "xray_avg_ms": round(avg, 1),
        "xray_min_ms": round(min(all_latencies), 1),
        "xray_max_ms": round(max(all_latencies), 1),
        "xray_jitter_ms": round(jitter, 1),
        "xray_success": len(all_latencies),
        "xray_total": test_count + 1,
        "xray_latencies": [round(l, 1) if l else None for l in latencies] + [round(l, 1) for l in content_latencies],
        "xray_error": "",
        "content_verified": True,
        "exit_ip": exit_ip,
    }


_IP_CHECK_URLS = ("https://api.ipify.org?format=text", "https://ifconfig.me/ip", "https://icanhazip.com")


def get_local_ip():
    """获取本机公网 IP（不走代理），用于后续验证代理是否生效"""
    for url in _IP_CHECK_URLS:
        try:
            resp = requests.get(url, timeout=5, headers=HEADERS)
            if resp.status_code == 200:
                ip = resp.text.strip()
                if ip and len(ip) < 50:
                    return ip
        except Exception:
            continue
    return None


def batch_xray_test(xray_bin, candidate_nodes):
    """
    单进程多节点并发测速：
    1. 为所有候选节点分配端口，生成一个合并配置
    2. 启动 1 个 xray 进程（所有节点共享）
    3. 并发通过各端口测速（含出口 IP 验证，确保流量真正走了代理）
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

    # 获取本机公网 IP，用于后续验证代理出口是否不同
    local_ip = get_local_ip()
    if local_ip:
        logger.info(f"  本机公网 IP: {local_ip}（代理出口 IP 必须与此不同）")
    else:
        logger.warning("  无法获取本机公网 IP，跳过出口 IP 验证")

    # 1. 分配端口
    ports = find_free_ports(total)
    nodes_with_ports = list(zip(candidate_nodes, ports))

    # 2. 生成合并配置
    xray_config, failed_indices = build_xray_multi_config(nodes_with_ports)

    results = [_xray_fail(candidate_nodes[idx], "config_build_failed") for idx in failed_indices]

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
                results.extend(_xray_fail(node, f"xray_binary_invalid: {err_msg[:100]}")
                              for idx, (node, _) in enumerate(nodes_with_ports) if idx not in failed_indices)
                return results
        except Exception as ve:
            logger.error(f"  xray 二进制验证失败: {ve}")

        # 启动 xray 进程，同时捕获 stdout 和 stderr 以便调试
        xray_proc = subprocess.Popen(
            [xray_bin, "run", "-c", str(config_file)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid,
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
            stderr_out = xray_proc.stderr.read().decode(errors="ignore")
            stdout_out = xray_proc.stdout.read().decode(errors="ignore")
            exit_code = xray_proc.returncode
            logger.error(f"  xray 进程启动失败 (exit_code={exit_code})")
            if stderr_out:
                logger.error(f"  stderr: {stderr_out[:500]}")
            if stdout_out:
                logger.error(f"  stdout: {stdout_out[:500]}")

            # 保存崩溃现场到 debug/ 目录
            try:
                debug_dir = Path(__file__).parent / "debug"
                debug_dir.mkdir(exist_ok=True)
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                (debug_dir / f"xray_crash_{timestamp}_config.json").write_text(
                    config_file.read_text(encoding="utf-8"), encoding="utf-8")
                (debug_dir / f"xray_crash_{timestamp}_log.txt").write_text(
                    f"exit_code: {exit_code}\ntotal_nodes: {total}\n"
                    f"\n=== STDOUT ===\n{stdout_out}\n\n=== STDERR ===\n{stderr_out}\n",
                    encoding="utf-8")
                logger.info(f"  崩溃现场已保存到 debug/xray_crash_{timestamp}_*")
            except Exception as dump_err:
                logger.warning(f"  保存崩溃现场失败: {dump_err}")

            results.extend(_xray_fail(node, f"xray_crashed(exit={exit_code}): {stderr_out[:100]}")
                          for idx, (node, _) in enumerate(nodes_with_ports) if idx not in failed_indices)
            return results

        logger.info(f"  xray 进程已启动 (PID={xray_proc.pid})，开始并发测速...")

        # 4. 并发测所有节点
        testable = [(idx, node, port) for idx, (node, port) in enumerate(nodes_with_ports)
                     if idx not in failed_indices]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    xray_test_via_proxy, node, port, test_count, timeout, local_ip
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
                    verified = " [内容验证✓]" if result.get("content_verified") else ""
                    exit_ip = result.get("exit_ip", "")
                    ip_info = f" [出口:{exit_ip}]" if exit_ip else ""
                    logger.info(
                        f"  [{done_count}/{len(testable)}] {status} {node['address']}:{node['port']} "
                        f"→ {avg}ms {f'({err})' if err else ''}{verified}{ip_info} {name}"
                    )
                except Exception as e:
                    results.append(_xray_fail(node, str(e)))
                    logger.warning(f"  [{done_count}/{len(testable)}] 测试异常: {e}")

    except Exception as e:
        logger.error(f"  xray 测速整体异常: {e}")
    finally:
        # 清理：杀掉唯一的 xray 进程
        if xray_proc and xray_proc.poll() is None:
            try:
                os.killpg(os.getpgid(xray_proc.pid), signal.SIGTERM)
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
# 第四步：筛选可用节点 + 生成订阅
# ============================================================

def select_best_nodes(test_results, max_latency=2000, max_loss=0.4):
    """
    筛选可用节点 —— 过滤不可用的，保留所有能用的
    不限制数量，让客户端自己测速选最优
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

    return valid if valid else []



# ============================================================
# 主流程
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="V2Ray 订阅聚合 - 采集/去重/真实测速/筛选")
    parser.add_argument("--workers", type=int, default=TEST_CONFIG["max_workers"], help="并发线程数 (默认30)")
    parser.add_argument("--output", type=str, default=None, help="输出目录 (默认 ./output)")
    parser.add_argument("--ping-count", type=int, default=TEST_CONFIG["tcp_ping_count"], help="每轮ping次数 (默认5)")
    parser.add_argument("--rounds", type=int, default=TEST_CONFIG.get("test_rounds", 3), help="测速轮次 (默认3)")
    parser.add_argument("--no-tls", action="store_true", help="禁用 TLS 握手测试（只用 TCP ping）")

    parser.add_argument("--timeout", type=int, default=TEST_CONFIG["tcp_ping_timeout"], help="单次超时秒数 (默认5)")
    parser.add_argument("--verbose", "-v", action="store_true", help="显示详细测速日志")
    args = parser.parse_args()

    TEST_CONFIG["max_workers"] = args.workers
    TEST_CONFIG["tcp_ping_count"] = args.ping_count
    TEST_CONFIG["test_rounds"] = args.rounds
    TEST_CONFIG["tcp_ping_timeout"] = args.timeout
    if args.no_tls:
        TEST_CONFIG["tls_test_enabled"] = False

    if args.verbose:
        logging.getLogger(__name__).setLevel(logging.DEBUG)

    output_dir = Path(args.output) if args.output else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("V2Ray 订阅聚合平台 启动")
    logger.info("=" * 60)

    # Step 1: 采集
    logger.info("\n[1/4] 采集订阅源...")
    tagged_contents, source_stats = source_module.collect_all()
    if not tagged_contents:
        logger.error("所有订阅源采集失败，退出")
        sys.exit(1)

    # Step 2: 解析去重
    logger.info("\n[2/4] 解析节点并去重...")
    nodes, per_source_node_counts = parse_nodes(tagged_contents)
    logger.info(f"  解析得到 {len(nodes)} 个节点")

    # 输出每个源解析到的节点数
    logger.info("  各源节点数：")
    for src_name, count in per_source_node_counts.items():
        logger.info(f"    [{src_name}] {count} 个节点")
    # 标记采集成功但解析出 0 节点的源
    for stat in source_stats:
        if stat["success"] and stat["name"] not in per_source_node_counts:
            logger.warning(f"    [{stat['name']}] 采集成功但未解析出任何节点")

    unique_nodes = deduplicate_nodes(nodes)
    logger.info(f"  去重后剩余 {len(unique_nodes)} 个节点")

    if not unique_nodes:
        logger.error("无可用节点，退出")
        sys.exit(1)

    # Step 3: 阶段一 —— TCP/TLS 快速初筛
    logger.info("\n[3/5] 阶段一：TCP/TLS 快速初筛...")
    test_results = batch_test_nodes(unique_nodes)

    # 初筛过滤：只排除完全不可达的节点，其余全部进 xray 验证
    logger.info("\n[4/5] 初筛过滤不可达节点...")
    preliminary_best = [
        r for r in test_results
        if r["avg_latency_ms"] < float("inf") and r["loss_rate"] < 1.0
    ]

    if not preliminary_best:
        logger.warning("初筛无可用节点")

    logger.info(f"  初筛通过 {len(preliminary_best)} 个候选节点（TCP/TLS 可达）")

    # Step 4: 阶段二 —— xray-core 真实代理验证
    if preliminary_best:
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
                    node["real_latency_ms"] = node["xray_avg_ms"]
                    node["avg_latency_ms"] = node["xray_avg_ms"]
                    node["jitter_ms"] = node.get("xray_jitter_ms", 0)

                # 所有 xray 验证通过的节点全部保留
                best_nodes = select_best_nodes(
                    xray_ok_nodes,
                    max_latency=TEST_CONFIG["max_latency_ms"],
                    max_loss=1.0,  # xray 已经验证过可用，放宽丢包限制
                )
                logger.info(f"  ✓ 经 xray 真实代理验证可用: {len(best_nodes)} 个节点（全部输出）")
            else:
                logger.warning("=" * 60)
                logger.warning("xray 真实代理测试全部失败！")
                logger.warning("这意味着所有候选节点虽然 tcping 可通，但实际无法代理上网。")
                logger.warning("可能原因：节点已过期/被封/免费节点不可用")
                logger.warning("=" * 60)
                # 不再回退到初筛结果，输出空列表比输出不可用节点更诚实
                best_nodes = []

        except Exception as e:
            logger.error(f"xray-core 测速失败: {e}")
            logger.warning("xray 测试异常，回退使用初筛结果（仅供参考，可能不可用）")
            best_nodes = preliminary_best
    else:
        best_nodes = []

    if not best_nodes:
        logger.warning("未筛选到任何可用节点！")
        logger.warning("所有节点均无法通过真实代理验证，本次不输出无效节点。")
        # 仍然生成空的订阅文件和报告，但不含无效节点

    # 输出结果
    logger.info(f"\n{'='*80}")
    logger.info(f"可用 {len(best_nodes)} 个节点（xray 真实代理验证通过）：")
    if best_nodes:
        logger.info(f"{'序号':<4} {'协议':<7} {'地址':<30} {'延迟(ms)':<10} {'出口IP':<18} {'名称'}")
        logger.info(f"{'-'*90}")
        for i, node in enumerate(best_nodes, 1):
            xray_avg = node.get("xray_avg_ms", "-")
            exit_ip = node.get("exit_ip", "-")
            logger.info(
                f"{i:<4} {node['protocol']:<7} {node['address']}:{node['port']:<20} "
                f"{xray_avg:<10} {exit_ip:<18} {node.get('name', '')[:30]}"
            )

    # 生成订阅文件（[-N] 标记已在 parse_nodes 中根据源数据日期完成）
    sub_file = output_dir / "best_nodes.txt"
    sub_content = base64.b64encode(
        "\n".join(node["raw"] for node in best_nodes).encode("utf-8")
    ).decode("utf-8")
    sub_file.write_text(sub_content, encoding="utf-8")

    logger.info(f"\n输出文件: {sub_file}")
    logger.info(f"完成！共 {len(best_nodes)} 个可用节点。")


if __name__ == "__main__":
    main()
