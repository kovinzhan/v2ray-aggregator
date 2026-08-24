"""
yy1588133/proxy-pool - 每 6 小时由 GitHub Actions 自动更新
https://github.com/yy1588133/proxy-pool

自动聚合多个公开免费节点源，去重后生成 Clash 和 v2rayN 订阅。
订阅文件：v2ray.txt（base64 编码，约 1000 节点）
"""

from . import BaseSource, register


@register
class ProxyPoolSource(BaseSource):
    name = "proxypool"

    SUB_URL = (
        "https://raw.githubusercontent.com/yy1588133/proxy-pool/"
        "main/v2ray.txt"
    )

    def fetch(self) -> list[str]:
        content = self.http_get_text(self.SUB_URL, timeout=60)
        if len(content) < 100:
            raise Exception(f"订阅内容过短: {len(content)} 字节")
        return [content]
