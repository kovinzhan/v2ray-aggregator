"""
xyfqzy/free-nodes - GitHub 仓库，每6小时更新
https://github.com/xyfqzy/free-nodes

订阅文件在 nodes/ 目录下：v2ray.txt, shadowsocks.txt, trojan.txt
"""

from . import BaseSource, register


@register
class XyfqzySource(BaseSource):
    name = "xyfqzy"
    enabled = False  # GitHub 静态节点库，大量历史节点已失效

    BASE_URL = "https://raw.githubusercontent.com/xyfqzy/free-nodes/main/nodes"

    FILES = [
        "v2ray.txt",
        "shadowsocks.txt",
        "trojan.txt",
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
            raise Exception("所有节点文件均获取失败")

        return results
