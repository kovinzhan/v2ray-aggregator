"""
hysteria350/free-jichang - GitHub 仓库，每日更新
https://github.com/hysteria350/free-jichang

README.md 中包含多条外部订阅链接（fn01.fn0618.xyz/nodes/...），
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

        # 提取所有订阅链接（更宽泛的正则以适应域名变化）
        links = re.findall(r'(https://fn\d+\.[^\s\)\]"\']+/nodes/[a-f0-9]+)', readme)
        if not links:
            # 备用正则：匹配更通用的格式
            links = re.findall(r'(https?://[^\s\)\]"\']+/nodes/[a-f0-9]{32})', readme)

        if not links:
            raise Exception("README 中未找到订阅链接")

        # 去重
        links = list(dict.fromkeys(links))

        results = []
        errors = []
        for url in links:
            try:
                content = self.http_get_text(url, timeout=20)
                if content and (
                    any(proto in content for proto in ["vmess://", "vless://", "trojan://", "ss://", "hysteria2://"])
                    or len(content) > 50
                ):
                    results.append(content)
            except Exception as e:
                errors.append(f"{url}: {e}")
                continue

        if not results:
            # 输出更详细的错误信息便于排查
            sample_errors = errors[:3]
            raise Exception(
                f"所有 {len(links)} 条订阅链接均获取失败，"
                f"示例错误: {'; '.join(sample_errors)}"
            )

        return results
