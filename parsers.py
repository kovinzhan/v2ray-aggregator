"""
节点解析模块：支持 vmess/vless/ss/trojan 协议的 URI 解析和去重
"""

import json
import base64
import logging
import urllib.parse
from datetime import date

from country_map import extract_country


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
        name = ""
        if "#" in uri_clean:
            uri_clean, name = uri_clean.rsplit("#", 1)
            name = urllib.parse.unquote(name)

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


def rebuild_raw_with_name(node):
    """重建 raw URI，将节点名称写回到 URI 中（公开接口）"""
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


# 协议前缀 → 解析器映射
_PARSERS = {
    "vmess://": parse_vmess,
    "vless://": parse_vless,
    "ss://": parse_ss,
    "trojan://": parse_trojan,
}


def parse_nodes(tagged_contents):
    """
    解析所有订阅内容为节点列表。
    参数：
        tagged_contents = [(source_name, raw_text, data_date), ...]
    返回：(nodes, per_source_node_counts)
    """
    nodes = []
    per_source_node_counts = {}
    today = date.today()

    for source_name, content, data_date in tagged_contents:
        source_node_count = 0
        try:
            d = date.fromisoformat(data_date)
            day_offset = (d - today).days
        except (ValueError, TypeError):
            day_offset = 0

        decoded = decode_base64(content)
        if not decoded:
            decoded = content

        for line in decoded.splitlines():
            line = line.strip()
            if not line:
                continue
            for prefix, parser in _PARSERS.items():
                if line.startswith(prefix):
                    node = parser(line)
                    if node and node["address"] and node["port"]:
                        original_name = node.get("name", "")
                        country = extract_country(original_name)
                        if country == "未知":
                            logging.getLogger(__name__).warning(
                                f"无法识别地区，原节点名: {original_name}"
                            )
                        if day_offset == 0:
                            node["name"] = f"[{country}][{source_name}]"
                        else:
                            node["name"] = f"[{day_offset}][{country}][{source_name}]"
                        node["source"] = source_name
                        node["country"] = country
                        node["day_offset"] = day_offset
                        node["raw"] = rebuild_raw_with_name(node)
                        nodes.append(node)
                        source_node_count += 1
                    break

        per_source_node_counts[source_name] = (
            per_source_node_counts.get(source_name, 0) + source_node_count
        )

    return nodes, per_source_node_counts


def deduplicate_nodes(nodes):
    """按 地址+端口+协议 去重"""
    seen = set()
    unique = []
    for node in nodes:
        if node["uid"] not in seen:
            seen.add(node["uid"])
            unique.append(node)
    return unique
