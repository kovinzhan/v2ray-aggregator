"""
悠雅派 (yoyapai) - 每日更新，90+ 节点
https://yoyapai.com/category/mianfeijiedian

文章链接格式为纯数字 ID（如 https://yoyapai.com/564），
通过 WordPress sitemap 获取最新文章。
详情页中包含 freenode.yoyapai.com 的订阅链接。
"""

import re
from html import unescape
from . import BaseSource, register


@register
class YoyapaiSource(BaseSource):
    name = "yoyapai"

    SITEMAP_URL = "https://yoyapai.com/wp-sitemap-posts-post-1.xml"

    def fetch(self) -> list[str]:
        # 从 WordPress sitemap 获取文章列表
        sitemap = self.http_get_text(self.SITEMAP_URL)

        # 提取文章链接（纯数字ID格式）
        article_urls = re.findall(
            r'<loc>(https://yoyapai\.com/\d+)</loc>',
            sitemap
        )

        if not article_urls:
            raise Exception("sitemap 中未找到文章链接")

        # WordPress sitemap 中最新的排在最后，取最后一篇
        # 实际测试发现可能顺序不固定，取 ID 最大的
        article_urls.sort(key=lambda u: int(u.split('/')[-1]), reverse=True)
        target_url = article_urls[0]

        # 访问文章详情页（HTML 实体解码，因为链接中的 :// 可能被编码为 &#47;&#47;）
        page = unescape(self.http_get_text(target_url, timeout=15))

        # 提取 freenode.yoyapai.com 的订阅链接（.txt 和 .yaml）
        sub_links = re.findall(
            r'(https?://freenode\.yoyapai\.com/[^\s<>"\']+?\.(?:txt|yaml))',
            page
        )

        # 如果没有匹配到，尝试更宽泛的格式
        if not sub_links:
            sub_links = re.findall(
                r'(https?://freenode\.yoyapai\.com/[^\s<>"\']+)',
                page
            )

        sub_links = list(dict.fromkeys(sub_links))

        if not sub_links:
            raise Exception(f"文章页 {target_url} 中未找到订阅链接")

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
            raise Exception(f"所有 {len(sub_links)} 个订阅链接均获取失败或内容无效")

        return results
