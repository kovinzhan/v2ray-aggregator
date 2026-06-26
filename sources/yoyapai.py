"""
悠雅派 (yoyapai) - 每日更新，90+ 节点
https://yoyapai.com/category/mianfeijiedian
"""

import re
from . import BaseSource, register


@register
class YoyapaiSource(BaseSource):
    name = "yoyapai"

    CATEGORY_URL = "https://yoyapai.com/category/mianfeijiedian"

    def fetch(self) -> list[str]:
        html = self.http_get_text(self.CATEGORY_URL)

        # 提取最新文章链接
        article_links = re.findall(
            r'href="(https?://yoyapai\.com/\d+[^\s<"\']*)"',
            html
        )
        if not article_links:
            raise Exception("未找到文章链接")

        # 去重，取最新一篇
        article_links = list(dict.fromkeys(article_links))
        post_url = article_links[0]

        page = self.http_get_text(post_url)

        # 查找订阅链接
        sub_links = re.findall(
            r'(https?://[^\s<"\']+?(?:\.txt|\.yaml|/sub\?[^\s<"\']*|subscribe[^\s<"\']*))',
            page
        )

        # 去重
        sub_links = list(dict.fromkeys(sub_links))

        if not sub_links:
            raise Exception("文章中未找到订阅链接")

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
