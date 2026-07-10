"""
ClashNodeFree - 每日自动更新
https://clashnodefree.com/free-node/

提供 Clash/V2Ray/Trojan/Sing-Box 节点订阅。
"""

import re
from . import BaseSource, register


@register
class ClashNodeFreeSource(BaseSource):
    name = "clashnodefree"
    enabled = False  # 暂时禁用，实际不可用

    PAGE_URL = "https://clashnodefree.com/free-node/"

    def fetch(self) -> list[str]:
        html = self.http_get_text(self.PAGE_URL)

        # 查找订阅链接
        sub_links = re.findall(
            r'(https?://[^\s<"\']+?(?:\.txt|\.yaml|/sub\?[^\s<"\']*|subscribe[^\s<"\']*))',
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
                if any(proto in content for proto in ["vmess://", "vless://", "trojan://", "ss://"]) \
                        or len(content) > 100:
                    results.append(content)
            except Exception:
                continue

        if not results:
            raise Exception(f"所有 {len(sub_links)} 个链接均获取失败或内容无效")

        return results
