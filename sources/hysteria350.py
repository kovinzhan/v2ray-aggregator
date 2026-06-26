"""
hysteria350/free-jichang - GitHub 仓库，每日更新
https://github.com/hysteria350/free-jichang

README.md 中包含多条外部订阅链接（fn01.fn0618.xyz），
需要解析 README 提取链接后逐个拉取。
"""

import re
from . import BaseSource, register


@register
class Hysteria350Source(BaseSource):
    name = "hysteria350"

    README_URL = "https://raw.githubusercontent.com/hysteria350/free-jichang/main/README.md"

    def fetch(self) -> list[str]:
        readme = self.http_get_text(self.README_URL)

        # 提取所有订阅链接
        links = re.findall(r'(https://fn\d+\.[^\s\)]+/nodes/[a-f0-9]+)', readme)
        if not links:
            raise Exception("README 中未找到订阅链接")

        results = []
        for url in links:
            try:
                content = self.http_get_text(url, timeout=15)
                if content:
                    results.append(content)
            except Exception:
                continue

        if not results:
            raise Exception(f"所有 {len(links)} 条订阅链接均获取失败")

        return results
