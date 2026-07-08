"""
xray-core 配置构建与二进制管理
"""

import os
import json
import socket
import logging
import zipfile
import platform
import urllib.parse
from pathlib import Path

import requests

from parsers import decode_base64

logger = logging.getLogger(__name__)

XRAY_VERSION = "25.10.15"
XRAY_DIR = Path(__file__).parent / ".xray"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


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


# xray 支持的传输协议白名单
_SUPPORTED_NETWORKS = {"tcp", "ws", "grpc", "kcp", "quic",
                       "httpupgrade", "splithttp", "xhttp"}

# SS 合法加密方法（xray 25.x 仅支持 AEAD 系列，已移除不安全的旧算法）
_VALID_SS_METHODS = {
    "aes-128-gcm", "aes-256-gcm", "chacha20-poly1305",
    "chacha20-ietf-poly1305", "xchacha20-poly1305",
    "2022-blake3-aes-128-gcm", "2022-blake3-aes-256-gcm",
    "2022-blake3-chacha20-poly1305",
    "none", "plain",
}


def _build_stream_settings(net, params, address):
    """构建 xray streamSettings（传输层配置）"""
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
    """一次性分配 count 个可用的本地端口"""
    sockets = []
    ports = []
    for _ in range(count):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        ports.append(s.getsockname()[1])
        sockets.append(s)
    for s in sockets:
        s.close()
    return ports


def build_xray_multi_config(nodes_with_ports):
    """
    为多个节点生成一个合并的 xray 配置。
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

    outbounds.append({"tag": "block", "protocol": "blackhole"})

    config = {
        "log": {"loglevel": "warning"},
        "inbounds": inbounds,
        "outbounds": outbounds,
        "routing": {
            "domainStrategy": "AsIs",
            "rules": routing_rules,
            "defaultOutboundTag": "block",
        },
    }

    return config, failed_indices
