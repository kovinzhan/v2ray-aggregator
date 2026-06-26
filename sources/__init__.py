"""
订阅源采集模块

每个源一个文件，继承 BaseSource 基类，放在 sources/ 目录下。
新增源只需创建一个新文件并定义 Source 类即可自动注册。
"""

import os
import logging
import importlib
import pkgutil
from abc import ABC, abstractmethod

import requests

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


class BaseSource(ABC):
    """订阅源基类，所有源必须继承此类"""

    # 子类必须设置
    name: str = ""           # 源名称（唯一标识）
    enabled: bool = True     # 是否启用

    @abstractmethod
    def fetch(self) -> list[str]:
        """
        采集节点内容。
        返回：原始文本列表，每个元素是一段订阅内容（base64 或逐行节点链接）。
        抛出异常表示采集失败。
        """
        ...

    def safe_fetch(self) -> tuple[list[str], dict]:
        """
        安全包装，捕获异常并记录日志。
        返回：(原始文本列表, 采集状态字典)
        状态字典包含：name, success, content_count, error
        """
        status = {"name": self.name, "success": False, "content_count": 0, "error": ""}
        if not self.enabled:
            logger.debug(f"[{self.name}] 已禁用，跳过")
            status["error"] = "disabled"
            return [], status
        try:
            results = self.fetch()
            count = len(results)
            status["success"] = True
            status["content_count"] = count
            logger.info(f"✓ [{self.name}] 采集成功，获取 {count} 段内容")
            return results, status
        except Exception as e:
            status["error"] = str(e)
            logger.warning(f"✗ [{self.name}] 采集失败: {e}")
            return [], status

    # ---- 工具方法，子类可直接使用 ----

    def http_get(self, url, timeout=30, **kwargs):
        """带默认 headers 和超时的 GET 请求"""
        headers = kwargs.pop("headers", HEADERS)
        resp = requests.get(url, headers=headers, timeout=timeout, **kwargs)
        resp.raise_for_status()
        return resp

    def http_get_text(self, url, timeout=30, **kwargs) -> str:
        """GET 请求并返回文本"""
        return self.http_get(url, timeout=timeout, **kwargs).text.strip()


# ============================================================
# 源注册表
# ============================================================

_registry: dict[str, BaseSource] = {}


def register(source_class):
    """注册一个源类（装饰器）"""
    instance = source_class()
    if not instance.name:
        raise ValueError(f"{source_class.__name__} 必须设置 name 属性")
    _registry[instance.name] = instance
    return source_class


def get_all_sources() -> list[BaseSource]:
    """获取所有已注册的源"""
    return list(_registry.values())


def get_enabled_sources() -> list[BaseSource]:
    """获取所有已启用的源"""
    return [s for s in _registry.values() if s.enabled]


def collect_all() -> tuple[list[tuple[str, str]], list[dict]]:
    """
    采集所有已启用源的内容。
    返回：
        - tagged_contents: [(source_name, raw_text), ...] 每段内容带源名称标记
        - source_stats: [{"name":..., "success":..., "content_count":..., "error":...}, ...]
    """
    tagged_contents = []  # (source_name, raw_text)
    source_stats = []
    sources = get_enabled_sources()
    logger.info(f"开始采集，共 {len(sources)} 个启用源")

    for source in sources:
        results, status = source.safe_fetch()
        source_stats.append(status)
        for content in results:
            tagged_contents.append((source.name, content))

    success_count = sum(1 for s in source_stats if s["success"])
    fail_count = sum(1 for s in source_stats if not s["success"])
    logger.info(f"采集完成，{success_count} 个成功 / {fail_count} 个失败，共获取 {len(tagged_contents)} 段内容")

    # 输出每个源的采集状态汇总
    logger.info("各源采集状态：")
    for stat in source_stats:
        if stat["success"]:
            logger.info(f"  ✓ {stat['name']}: 获取 {stat['content_count']} 段内容")
        else:
            logger.info(f"  ✗ {stat['name']}: 失败 ({stat['error']})")

    return tagged_contents, source_stats


# ============================================================
# 自动加载 sources/ 目录下所有模块
# ============================================================

def _auto_discover():
    """自动导入 sources/ 下所有 .py 模块，触发 @register 装饰器"""
    package_dir = os.path.dirname(__file__)
    for _, module_name, _ in pkgutil.iter_modules([package_dir]):
        if module_name.startswith("_"):
            continue
        importlib.import_module(f".{module_name}", package=__name__)


_auto_discover()
