"""
V2Nodes - 自动测试更新
https://www.v2nodes.com/

提供公共 V2Ray 服务器列表，自动测试并更新。
注意：该站可能有 CloudFlare 拦截，采集时需要容忍失败。
"""

import re
from . import BaseSource, register


@register
class V2NodesSource(BaseSource):
    name = "v2nodes"

    BASE_URL = "https://www.v2nodes.com"

    def fetch(self) -> list[str]:
        # 尝试主页面，找订阅链接
        html = self.http_get_text(self.BASE_URL, timeout=20)

        # 找所有可能的订阅链接
        sub_links = re.findall(
            r'(https?://[^\s<"\']+?(?:\.txt|/sub[^\s<"\']*|/raw[^\s<"\']*|/conf[^\s<"\']*))',
            html
        )
        sub_links = list(dict.fromkeys(sub_links))

        # 也尝试从子页面找
        if not sub_links:
            # 找 v2ray 相关页面链接
            page_links = re.findall(
                rf'href="({re.escape(self.BASE_URL)}/[^\s<"\']*v2ray[^\s<"\']*|{re.escape(self.BASE_URL)}/country/[^\s<"\']+)"',
                html
            )
            for page_url in page_links[:5]:
                try:
                    page_html = self.http_get_text(page_url, timeout=15)
                    sub_links = re.findall(
                        r'(https?://[^\s<"\']+?(?:\.txt|/sub[^\s<"\']*|/raw[^\s<"\']*|/conf[^\s<"\']*))',
                        page_html
                    )
                    if sub_links:
                        break
                except Exception:
                    continue

        if not sub_links:
            raise Exception("未找到订阅链接")

        results = []
        for url in sub_links:
            try:
                content = self.http_get_text(url, timeout=15)
                if any(proto in content for proto in ["vmess://", "vless://", "trojan://", "ss://"]):
                    results.append(content)
            except Exception:
                continue

        if not results:
            raise Exception(f"所有 {len(sub_links)} 个链接均获取失败")

        return results
