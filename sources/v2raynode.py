"""
V2RayNode - 每日更新
https://v2raynode.top/

通过 sitemap 获取最新文章 URL，文章详情页中包含
node.v2raynode.top 的 .txt/.yaml/.json 订阅链接。
"""

import re
from datetime import datetime, date
from . import BaseSource, register


@register
class V2RayNodeSource(BaseSource):
    name = "v2raynode"

    SITEMAP_URL = "https://v2raynode.top/sitemap.xml"

    def _extract_date_from_url(self, url):
        """从文章 URL 中提取日期，如 /free-node/2026-6-27-xxx.htm → '2026-06-27'"""
        match = re.search(r'/free-node/(\d{4})-(\d{1,2})-(\d{1,2})-', url)
        if match:
            y, m, d = int(match.group(1)), int(match.group(2)), int(match.group(3))
            return date(y, m, d).strftime("%Y-%m-%d")
        return date.today().strftime("%Y-%m-%d")

    def fetch(self) -> list[str]:
        _, results = self._fetch_internal()
        return results

    def fetch_with_date(self) -> list[tuple[str, str]]:
        data_date, results = self._fetch_internal()
        return [(content, data_date) for content in results]

    def _fetch_internal(self) -> tuple[str, list[str]]:
        """内部实现，返回 (data_date, [content, ...])"""
        # 从 sitemap 获取最新的免费节点文章链接
        sitemap = self.http_get_text(self.SITEMAP_URL)

        # 提取 /free-node/ 下的文章链接
        article_urls = re.findall(
            r'<loc>(https://v2raynode\.top/free-node/\d{4}-\d{1,2}-\d{1,2}-[^<]+\.htm)</loc>',
            sitemap
        )

        if not article_urls:
            raise Exception("sitemap 中未找到免费节点文章")

        # 取最新一篇
        today = datetime.now().strftime("%Y-%-m-%-d")
        target_url = None
        for url in article_urls:
            if today in url:
                target_url = url
                break
        if not target_url:
            target_url = article_urls[0]

        # 从 URL 中提取数据日期
        data_date = self._extract_date_from_url(target_url)

        # 访问文章详情页
        page = self.http_get_text(target_url, timeout=15)

        # 提取 node.v2raynode.top 的订阅链接（.txt 为 V2Ray 格式）
        sub_links = re.findall(
            r'(https?://node\.v2raynode\.top/[^\s<"\']+?\.txt)',
            page
        )

        sub_links = list(dict.fromkeys(sub_links))

        if not sub_links:
            raise Exception(f"文章页 {target_url} 中未找到 V2Ray 订阅链接")

        results = []
        for url in sub_links:
            try:
                content = self.http_get_text(url, timeout=15)
                # 排除 HTML 页面
                if content.strip().startswith(('<', '<!')) and '<html' in content[:500].lower():
                    continue
                if any(proto in content for proto in ["vmess://", "vless://", "trojan://", "ss://"]) \
                        or len(content) > 100:
                    results.append(content)
            except Exception:
                continue

        if not results:
            raise Exception(f"所有 {len(sub_links)} 个订阅链接均获取失败或内容无效")

        return data_date, results
