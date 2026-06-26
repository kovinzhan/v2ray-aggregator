"""
free-nodes/v2rayfree - GitHub 仓库，每日更新两次
https://github.com/free-nodes/v2rayfree

文件命名规则：v + 年月日 + 序号(1或2)
例如：v202606261, v202606262
"""

from datetime import datetime
from . import BaseSource, register


@register
class V2rayFreeSource(BaseSource):
    name = "v2rayfree"
    enabled = False  # GitHub 静态节点库，大量历史节点已失效

    BASE_URL = "https://raw.githubusercontent.com/free-nodes/v2rayfree/main"

    def fetch(self) -> list[str]:
        today = datetime.utcnow().strftime("%Y%m%d")
        results = []

        # 尝试今天的两个文件（序号2是晚间更新，优先）
        for seq in ["2", "1"]:
            filename = f"v{today}{seq}"
            url = f"{self.BASE_URL}/{filename}"
            try:
                content = self.http_get_text(url)
                if content:
                    results.append(content)
            except Exception:
                continue

        # 如果今天的都没有，尝试昨天的
        if not results:
            from datetime import timedelta
            yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y%m%d")
            for seq in ["2", "1"]:
                filename = f"v{yesterday}{seq}"
                url = f"{self.BASE_URL}/{filename}"
                try:
                    content = self.http_get_text(url)
                    if content:
                        results.append(content)
                        break  # 昨天的拿一份就够了
                except Exception:
                    continue

        if not results:
            raise Exception("未找到任何可用的节点文件")

        return results
