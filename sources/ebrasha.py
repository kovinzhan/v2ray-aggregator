"""
ebrasha/free-v2ray-public-list - GitHub 仓库，每15分钟自动更新
https://github.com/ebrasha/free-v2ray-public-list

按协议分文件提供：vmess、vless、trojan、ss
"""

from . import BaseSource, register


@register
class EbrashaSource(BaseSource):
    name = "ebrasha"
    enabled = False  # GitHub 静态节点库，历史累积26万+节点大部分已失效

    BASE_URL = "https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list/refs/heads/main"

    # 按协议分的文件（不用 All-Type 避免重复）
    FILES = [
        "vmess_configs.txt",
        "vless_configs.txt",
        "trojan_configs.txt",
        "ss_configs.txt",
    ]

    def fetch(self) -> list[str]:
        results = []
        for filename in self.FILES:
            url = f"{self.BASE_URL}/{filename}"
            try:
                content = self.http_get_text(url)
                if content:
                    results.append(content)
            except Exception:
                continue

        if not results:
            raise Exception("所有协议文件均获取失败")

        return results
