"""
PassWall - 每日更新免费节点
https://passwall.wiki/free-node/

页面包含 Clash 和 V2ray 订阅链接，需要动态解析。
"""

import re
from . import BaseSource, register


@register
class PasswallSource(BaseSource):
    name = "passwall"

    PAGE_URL = "https://passwall.wiki/free-node/"

    def fetch(self) -> list[str]:
        html = self.http_get_text(self.PAGE_URL)

        # 查找订阅链接（通常是 txt 文件或 sub 链接）
        sub_links = re.findall(
            r'(https?://[^\s<"\']+?(?:\.txt|/sub\?[^\s<"\']*|v2ray[^\s<"\']*\.txt))',
            html
        )

        # 也查找常见订阅平台链接
        sub_links += re.findall(
            r'(https?://[^\s<"\']*(?:subscribe|api|sub)[^\s<"\']*)',
            html
        )

        # 去重
        sub_links = list(dict.fromkeys(sub_links))

        if not sub_links:
            raise Exception("页面中未找到订阅链接")

        results = []
        for url in sub_links:
            try:
                content = self.http_get_text(url, timeout=15)
                # 简单验证内容像是节点数据（base64 或协议链接）
                if any(proto in content for proto in ["vmess://", "vless://", "trojan://", "ss://"]) \
                        or len(content) > 100:
                    results.append(content)
            except Exception:
                continue

        if not results:
            raise Exception(f"所有 {len(sub_links)} 个链接均获取失败或内容无效")

        return results
