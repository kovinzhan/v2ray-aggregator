"""
米贝分享 (mibeifenxiang.com) - 每日更新
https://mibeifenxiang.com/free-nodes/

每天发布一篇文章，内含订阅链接，格式为：
https://node.mibeifenxiang.com/uploads/{YYYY}/{MM}/{YYYYMMDD}.txt

通过文章列表页提取最新文章链接，进入详情页获取 .txt 订阅链接。
"""

import re
from datetime import date
from . import BaseSource, register


@register
class MibeiFenxiangSource(BaseSource):
    name = "mibeifenxiang"

    LIST_URL = "https://mibeifenxiang.com/free-nodes/"

    def _extract_date_from_url(self, url):
        """从文章 URL 中提取日期，如 /2026-6-27-xxx.htm → '2026-06-27'"""
        match = re.search(r'/(\d{4})-(\d{1,2})-(\d{1,2})-', url)
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
        # 获取文章列表页
        html = self.http_get_text(self.LIST_URL, timeout=15)

        # 提取文章链接（相对路径格式如 /free-nodes/2026-6-27-xxx.htm）
        article_paths = re.findall(
            r'href="(/free-nodes/\d{4}-\d{1,2}-\d{1,2}-[^"]+\.htm)"',
            html
        )

        if not article_paths:
            raise Exception("列表页未找到文章链接")

        # 去重，取第一个（最新）
        article_paths = list(dict.fromkeys(article_paths))
        target_url = "https://mibeifenxiang.com" + article_paths[0]

        # 从 URL 中提取数据日期
        data_date = self._extract_date_from_url(target_url)

        # 访问文章详情页
        page = self.http_get_text(target_url, timeout=15)

        # 提取 node.mibeifenxiang.com 的 .txt 订阅链接
        sub_links = re.findall(
            r'(https?://node\.mibeifenxiang\.com/uploads/[^\s<"\']+?\.txt)',
            page
        )

        sub_links = list(dict.fromkeys(sub_links))

        if not sub_links:
            raise Exception(f"文章页 {target_url} 中未找到订阅链接")

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
