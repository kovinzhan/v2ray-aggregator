"""
V2RayNode - 每日更新
https://v2raynode.top/

提供 V2Ray/Clash 订阅资源。
"""

import re
from . import BaseSource, register


@register
class V2RayNodeSource(BaseSource):
    name = "v2raynode"

    PAGE_URL = "https://v2raynode.top/"

    def fetch(self) -> list[str]:
        html = self.http_get_text(self.PAGE_URL)

        # 查找最新文章链接
        article_links = re.findall(
            r'href="(https?://v2raynode\.top/\d+[^\s<"\']*)"',
            html
        )

        if not article_links:
            # 直接在首页找订阅链接
            article_links = [self.PAGE_URL]

        # 去重，只取最新几篇
        article_links = list(dict.fromkeys(article_links))[:3]

        results = []
        for article_url in article_links:
            try:
                page = self.http_get_text(article_url, timeout=15)
                # 查找订阅链接
                sub_links = re.findall(
                    r'(https?://[^\s<"\']+?(?:\.txt|\.yaml|/sub[^\s<"\']*))',
                    page
                )
                for url in sub_links:
                    try:
                        content = self.http_get_text(url, timeout=15)
                        if any(proto in content for proto in
                               ["vmess://", "vless://", "trojan://", "ss://"]) \
                                or len(content) > 100:
                            results.append(content)
                    except Exception:
                        continue
            except Exception:
                continue

        if not results:
            raise Exception("未能获取到有效订阅内容")

        return results
