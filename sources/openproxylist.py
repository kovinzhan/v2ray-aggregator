"""
OpenProxyList - 分钟级自动更新
https://openproxylist.com/v2ray/

提供 V2Ray 原始节点列表（明文），直接返回 vmess/vless/trojan/ss 链接。
每几分钟自动测试并更新所有节点。
"""

from . import BaseSource, register


@register
class OpenProxyListSource(BaseSource):
    name = "openproxylist"

    RAW_LIST_URL = "https://openproxylist.com/v2ray/rawlist/text"

    def fetch(self) -> list[str]:
        content = self.http_get_text(self.RAW_LIST_URL, timeout=20)
        # 原始列表是明文节点链接，每行一个
        if not any(proto in content for proto in ["vmess://", "vless://", "trojan://", "ss://"]):
            raise Exception("返回内容不包含有效节点链接")

        return [content]
