"""
V2Ray中文教程网 (v2raynews) - 每日更新
https://v2raynews.org/index.php/category/免费节点/

WordPress 站点，通过分类页找最新文章，文章页内含 dlconf.clashapps.cc 的订阅链接。
订阅链接返回 base64 编码的节点列表。
"""

import re
from html import unescape
from . import BaseSource, register


@register
class V2RayNewsSource(BaseSource):
    name = "v2raynews"

    CATEGORY_URL = "https://v2raynews.org/index.php/category/%E5%85%8D%E8%B4%B9%E8%8A%82%E7%82%B9/"

    def fetch(self) -> list[str]:
        # 从分类页获取最新文章链接
        html = self.http_get_text(self.CATEGORY_URL, timeout=15)

        article_urls = re.findall(
            r'href="(https://v2raynews\.org/index\.php/\d{4}/\d{2}/\d{2}/[^"]+)"',
            html
        )
        # 去重
        article_urls = list(dict.fromkeys(article_urls))

        if not article_urls:
            raise Exception("分类页中未找到文章链接")

        # 尝试最新 3 篇文章
        sub_links = []
        for url in article_urls[:3]:
            try:
                page = unescape(self.http_get_text(url, timeout=15))
            except Exception:
                continue

            # 提取 dlconf.clashapps.cc 的 .conf 订阅链接（base64编码的节点列表）
            # 忽略 .yaml（Clash配置格式，parsers不支持解析）
            sub_links = re.findall(
                r'(https?://dlconf\.clashapps\.cc/[^\s<>"\']+?\.conf)',
                page
            )
            # 也尝试更宽泛的匹配
            if not sub_links:
                sub_links = re.findall(
                    r'(https?://[^\s<>"\']+?\.conf)',
                    page
                )

            sub_links = list(dict.fromkeys(sub_links))
            if sub_links:
                break

        if not sub_links:
            raise Exception(f"最新 {min(3, len(article_urls))} 篇文章中均未找到订阅链接")

        results = []
        for url in sub_links:
            try:
                content = self.http_get_text(url, timeout=15)
                # 订阅内容可能是 base64 编码的节点列表
                if any(proto in content for proto in ["vmess://", "vless://", "trojan://", "ss://"]):
                    results.append(content)
                elif len(content) > 100:
                    # 尝试 base64 解码验证
                    results.append(content)
            except Exception:
                continue

        if not results:
            raise Exception(f"所有 {len(sub_links)} 个订阅链接均获取失败")

        return results
