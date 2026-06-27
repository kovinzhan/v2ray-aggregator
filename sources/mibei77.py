"""
米贝分享 - 动态解析每日节点
https://www.mibei77.com/category/jiedian
"""

import re
from . import BaseSource, register


@register
class Mibei77Source(BaseSource):
    name = "mibei77"


    def fetch(self) -> list[str]:
        category_url = "https://www.mibei77.com/category/jiedian"
        resp = self.http_get(category_url)

        # 提取最新帖子链接
        matches = re.findall(r'href="(https://www\.mibei77\.com/\d+\.html)"', resp.text)
        if not matches:
            raise Exception("未找到帖子链接")

        post_url = matches[0]
        resp2 = self.http_get(post_url)

        # 提取 v2ray 订阅链接
        v2ray_links = re.findall(r'(https://mm\.mibei77\.com/[^\s<"\']+\.txt)', resp2.text)
        if not v2ray_links:
            raise Exception("未找到订阅链接")

        # 下载订阅内容
        content = self.http_get_text(v2ray_links[0])
        return [content]
