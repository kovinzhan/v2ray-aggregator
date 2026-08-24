"""
Epodonios/v2ray-configs - 每 5 分钟自动更新
https://github.com/Epodonios/v2ray-configs

多渠道大规模聚合（数千节点），量大但无可用性验证，
依赖本项目自身的 TCP 初筛 + xray 真实代理验证过滤。
订阅文件：All_Configs_Sub.txt（明文节点链接，含少量无效行会被解析器跳过）
"""

from . import BaseSource, register


@register
class EpodoniosSource(BaseSource):
    name = "epodonios"

    SUB_URL = (
        "https://raw.githubusercontent.com/Epodonios/v2ray-configs/"
        "main/All_Configs_Sub.txt"
    )

    def fetch(self) -> list[str]:
        content = self.http_get_text(self.SUB_URL, timeout=90)
        if not any(proto in content for proto in ["vmess://", "vless://", "trojan://", "ss://"]):
            raise Exception("订阅内容中未找到任何节点链接")
        return [content]
