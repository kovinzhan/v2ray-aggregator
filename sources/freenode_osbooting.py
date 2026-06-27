"""
FreeNode (freenode.osbooting.com) - 每日更新 V2Ray/Clash 节点
https://freenode.osbooting.com/

每天发布一篇文章，标题格式：
"2026年6月27日丨免费节点分享291个丨每日更新Clash/V2Ray最新订阅"

文章详情页中包含 V2Ray 和 Clash 订阅链接。
"""

import re
from datetime import date
from . import BaseSource, register


@register
class FreeNodeOsbootingSource(BaseSource):
    name = "freenode_osbooting"

    BASE_URL = "https://freenode.osbooting.com/"

    def _extract_date_from_title(self, title):
        """从文章标题中提取日期，如 '2026年6月27日丨...' → '2026-06-27'"""
        match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', title)
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
        # 获取首页文章列表
        html = self.http_get_text(self.BASE_URL, timeout=15)

        # 提取文章链接（相对路径格式如 /freenodes/20260627）
        article_paths = re.findall(
            r'href="(/freenodes/\d{8})"',
            html
        )

        if not article_paths:
            raise Exception("首页未找到文章链接")

        # 去重，取第一个（最新）
        article_paths = list(dict.fromkeys(article_paths))
        target_url = "https://freenode.osbooting.com" + article_paths[0]

        # 从 URL 中提取日期（/freenodes/20260627 → 2026-06-27）
        url_date_match = re.search(r'/freenodes/(\d{4})(\d{2})(\d{2})', target_url)
        if url_date_match:
            data_date = f"{url_date_match.group(1)}-{url_date_match.group(2)}-{url_date_match.group(3)}"
        else:
            data_date = date.today().strftime("%Y-%m-%d")

        # 访问文章详情页
        page = self.http_get_text(target_url, timeout=15)

        # 提取订阅链接（在 <code> 标签中，格式如 /nodefiles/20260627XXXX.txt 或 .yaml）
        sub_links = re.findall(
            r'(https?://freenode\.osbooting\.com/nodefiles/[^\s<"\']+\.(?:txt|yaml))',
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
