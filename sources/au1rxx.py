"""
Au1rxx free-vpn-subscriptions - 每小时更新，sing-box 真实流量验证
https://github.com/Au1rxx/free-vpn-subscriptions

发布前每个节点都经过 sing-box 真实 HTTP 流量转发验证，
是目前质量最高的免费节点聚合源（与米贝系地址重叠仅 8%）。
订阅文件：output/v2ray-base64.txt（base64 编码，约 2000 节点）
"""

from . import BaseSource, register


@register
class Au1rxxSource(BaseSource):
    name = "au1rxx"

    SUB_URL = (
        "https://raw.githubusercontent.com/Au1rxx/free-vpn-subscriptions/"
        "main/output/v2ray-base64.txt"
    )

    def fetch(self) -> list[str]:
        content = self.http_get_text(self.SUB_URL, timeout=60)
        # base64 内容无法直接判断协议，用长度做基本校验
        if len(content) < 100:
            raise Exception(f"订阅内容过短: {len(content)} 字节")
        return [content]
