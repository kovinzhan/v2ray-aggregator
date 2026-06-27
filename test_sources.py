#!/usr/bin/env python3
"""
订阅源测试脚本

用途：验证所有已注册的源能否正常拉取数据。
每次新增源后跑一下即可快速确认。

用法：
    python3 test_sources.py              # 测试所有已启用的源
    python3 test_sources.py vpnnode      # 只测试指定源
    python3 test_sources.py vpnnode mibei77  # 测试多个指定源
    python3 test_sources.py --all        # 包括已禁用的源
    python3 test_sources.py --list       # 列出所有已注册的源
"""

import sys
import os
import time
import hashlib
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sources import get_all_sources, get_enabled_sources


def format_size(n_bytes):
    """格式化字节大小"""
    if n_bytes < 1024:
        return f"{n_bytes} B"
    elif n_bytes < 1024 * 1024:
        return f"{n_bytes / 1024:.1f} KB"
    else:
        return f"{n_bytes / (1024 * 1024):.1f} MB"


def test_source(source):
    """测试单个源，返回测试结果字典"""
    result = {
        "name": source.name,
        "enabled": source.enabled,
        "success": False,
        "content_count": 0,
        "total_size": 0,
        "data_date": "",
        "duration": 0,
        "error": "",
        "details": [],
    }

    start = time.time()
    try:
        contents = source.fetch_with_date()
        result["success"] = True
        result["content_count"] = len(contents)

        for i, (content, dt) in enumerate(contents):
            lines = content.strip().split("\n")
            size = len(content.encode("utf-8"))
            md5 = hashlib.md5(content.strip().encode()).hexdigest()[:12]
            result["total_size"] += size
            result["details"].append({
                "index": i + 1,
                "date": dt,
                "size": size,
                "lines": len(lines),
                "md5": md5,
                "preview": lines[0][:70] if lines else "",
            })

        if contents:
            result["data_date"] = contents[0][1]

    except Exception as e:
        result["error"] = str(e)

    result["duration"] = time.time() - start
    return result


def print_result(result, verbose=True):
    """打印单个源的测试结果"""
    name = result["name"]
    enabled_tag = "" if result["enabled"] else " [已禁用]"

    if result["success"]:
        print(f"  ✅ {name}{enabled_tag}")
        print(f"     数据日期: {result['data_date']} | "
              f"内容段数: {result['content_count']} | "
              f"总大小: {format_size(result['total_size'])} | "
              f"耗时: {result['duration']:.1f}s")
        if verbose:
            for d in result["details"]:
                print(f"     [{d['index']}] {format_size(d['size']):>8} | "
                      f"{d['lines']:>4} 行 | "
                      f"MD5: {d['md5']} | "
                      f"{d['preview']}")
    else:
        print(f"  ❌ {name}{enabled_tag}")
        print(f"     错误: {result['error']} | 耗时: {result['duration']:.1f}s")


def main():
    parser = argparse.ArgumentParser(description="订阅源测试工具")
    parser.add_argument("sources", nargs="*", help="指定要测试的源名称（留空则测试所有已启用源）")
    parser.add_argument("--all", "-a", action="store_true", help="包含已禁用的源")
    parser.add_argument("--list", "-l", action="store_true", help="仅列出所有已注册的源")
    parser.add_argument("--quiet", "-q", action="store_true", help="简洁输出，不显示详情")
    args = parser.parse_args()

    all_sources = get_all_sources()

    # 列出模式
    if args.list:
        print(f"已注册源共 {len(all_sources)} 个：")
        for s in all_sources:
            status = "✓ 启用" if s.enabled else "✗ 禁用"
            print(f"  [{status}] {s.name}")
        return

    # 确定要测试的源
    if args.sources:
        # 按名称筛选
        name_set = set(args.sources)
        targets = [s for s in all_sources if s.name in name_set]
        not_found = name_set - {s.name for s in targets}
        if not_found:
            print(f"⚠️  未找到源: {', '.join(not_found)}")
            print(f"   可用源: {', '.join(s.name for s in all_sources)}")
            if not targets:
                sys.exit(1)
    elif args.all:
        targets = all_sources
    else:
        targets = get_enabled_sources()

    # 开始测试
    print("=" * 65)
    print(f"📡 订阅源测试  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  |  共 {len(targets)} 个源")
    print("=" * 65)

    results = []
    total_start = time.time()

    for source in targets:
        result = test_source(source)
        results.append(result)
        print_result(result, verbose=not args.quiet)
        print()

    total_duration = time.time() - total_start

    # 汇总
    success = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]

    print("=" * 65)
    print(f"📊 测试汇总  |  总耗时: {total_duration:.1f}s")
    print(f"   成功: {len(success)}/{len(results)}")

    if success:
        total_size = sum(r["total_size"] for r in success)
        total_contents = sum(r["content_count"] for r in success)
        print(f"   总内容段数: {total_contents}  |  总数据量: {format_size(total_size)}")

    if failed:
        print(f"   失败: {', '.join(r['name'] for r in failed)}")

    print("=" * 65)

    # 如果有失败的，返回非零退出码
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
