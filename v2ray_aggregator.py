#!/usr/bin/env python3
"""
V2Ray 订阅聚合平台
功能：多源采集 → 解析去重 → TCP初筛 → xray真实代理验证 → 带宽测速 → 输出可用节点订阅
策略：不限数量，只筛可用性，让客户端自行测速选最优
部署：GitHub Actions 定时执行 / 云服务器 cron
"""

import sys
import base64
import logging
import argparse
from pathlib import Path

import sources as source_module

from parsers import parse_nodes, deduplicate_nodes, rebuild_raw_with_name
from xray_config import ensure_xray_binary
from speed_test import batch_test_nodes, batch_xray_test, batch_bandwidth_test

# ============================================================
# 配置
# ============================================================

TEST_CONFIG = {
    "tcp_ping_count": 2,        # 每个节点 TCP ping 次数
    "tcp_ping_timeout": 3,      # 单次超时（秒）
    "max_workers": 500,         # 并发测试线程数
    "max_latency_ms": 2000,     # 最大可接受延迟（ms）
    # xray-core 真实代理测速配置
    "xray_test_count": 2,       # 每个节点通过代理请求次数
    "xray_test_timeout": 8,     # 代理请求超时（秒）
    "xray_startup_wait": 2,     # xray 进程启动等待（秒）
    "xray_max_workers": 100,    # xray 测试并发数
    # 带宽测速配置
    "bandwidth_test_enabled": True,
    "bandwidth_download_bytes": 2 * 1024 * 1024,  # 2MB
    "bandwidth_timeout": 15,
    "bandwidth_max_workers": 100,
    "bandwidth_top_n": 0,       # 0=全部测试
}

OUTPUT_DIR = Path(__file__).parent / "output"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ============================================================
# 主流程
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="V2Ray 订阅聚合 - 采集/去重/真实测速/筛选")
    parser.add_argument("--workers", type=int, default=TEST_CONFIG["max_workers"], help="并发线程数")
    parser.add_argument("--output", type=str, default=None, help="输出目录")
    parser.add_argument("--ping-count", type=int, default=TEST_CONFIG["tcp_ping_count"], help="每节点ping次数")
    parser.add_argument("--timeout", type=int, default=TEST_CONFIG["tcp_ping_timeout"], help="单次超时秒数")
    parser.add_argument("--verbose", "-v", action="store_true", help="显示详细日志")
    args = parser.parse_args()

    TEST_CONFIG["max_workers"] = args.workers
    TEST_CONFIG["tcp_ping_count"] = args.ping_count
    TEST_CONFIG["tcp_ping_timeout"] = args.timeout

    if args.verbose:
        logging.getLogger(__name__).setLevel(logging.DEBUG)

    output_dir = Path(args.output) if args.output else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("V2Ray 订阅聚合平台 启动")
    logger.info("=" * 60)

    # Step 1: 采集
    logger.info("\n[1/4] 采集订阅源...")
    tagged_contents, source_stats = source_module.collect_all()
    if not tagged_contents:
        logger.error("所有订阅源采集失败，退出")
        sys.exit(1)

    # Step 2: 解析去重
    logger.info("\n[2/4] 解析节点并去重...")
    nodes, per_source_node_counts = parse_nodes(tagged_contents)
    logger.info(f"  解析得到 {len(nodes)} 个节点")

    logger.info("  各源节点数：")
    for src_name, count in per_source_node_counts.items():
        logger.info(f"    [{src_name}] {count} 个节点")
    for stat in source_stats:
        if stat["success"] and stat["name"] not in per_source_node_counts:
            logger.warning(f"    [{stat['name']}] 采集成功但未解析出任何节点")

    unique_nodes = deduplicate_nodes(nodes)
    logger.info(f"  去重后剩余 {len(unique_nodes)} 个节点")

    if not unique_nodes:
        logger.error("无可用节点，退出")
        sys.exit(1)

    # Step 3: TCP 快速初筛
    logger.info("\n[3/5] TCP 端口可达检测...")
    test_results = batch_test_nodes(
        unique_nodes,
        max_workers=TEST_CONFIG["max_workers"],
        ping_count=TEST_CONFIG["tcp_ping_count"],
        timeout=TEST_CONFIG["tcp_ping_timeout"],
    )

    # 过滤完全不可达的节点
    logger.info("\n[4/5] 过滤不可达节点...")
    preliminary_best = [
        r for r in test_results
        if r["avg_latency_ms"] < float("inf") and r["loss_rate"] < 1.0
    ]

    if not preliminary_best:
        logger.warning("初筛无可用节点")

    logger.info(f"  初筛通过 {len(preliminary_best)} 个候选节点（TCP 可达）")

    # Step 4: xray-core 真实代理验证
    best_nodes = []
    if preliminary_best:
        logger.info(f"\n[5/5] xray-core 真实代理测速...")
        try:
            xray_bin = ensure_xray_binary()
            xray_results = batch_xray_test(xray_bin, preliminary_best, TEST_CONFIG)

            xray_ok_nodes = [r for r in xray_results if r.get("xray_ok")]
            xray_fail_count = len(xray_results) - len(xray_ok_nodes)

            logger.info(f"\n  xray 测试完成: {len(xray_ok_nodes)} 可用 / {xray_fail_count} 不可用")

            if xray_ok_nodes:
                for node in xray_ok_nodes:
                    node["real_latency_ms"] = node["xray_avg_ms"]
                    node["avg_latency_ms"] = node["xray_avg_ms"]
                    node["jitter_ms"] = node.get("xray_jitter_ms", 0)

                best_nodes = xray_ok_nodes
                logger.info(f"  ✓ 经 xray 真实代理验证可用: {len(best_nodes)} 个节点")
            else:
                logger.warning("=" * 60)
                logger.warning("xray 真实代理测试全部失败！")
                logger.warning("所有候选节点虽然 TCP 可通，但实际无法代理上网。")
                logger.warning("=" * 60)

        except Exception as e:
            logger.error(f"xray-core 测速失败: {e}")
            logger.warning("xray 测试异常，回退使用初筛结果（仅供参考）")
            best_nodes = preliminary_best

    if not best_nodes:
        logger.warning("未筛选到任何可用节点！将写入失败提示假节点。")

    # ---- 带宽测速 ----
    if best_nodes and TEST_CONFIG.get("bandwidth_test_enabled"):
        logger.info(f"\n{'='*80}")
        logger.info("[带宽测速] 测试所有可用节点的下载带宽...")
        try:
            xray_bin = ensure_xray_binary()
            best_nodes = batch_bandwidth_test(xray_bin, best_nodes, TEST_CONFIG)

            # 按带宽降序排序（带宽相同则按延迟升序）
            best_nodes.sort(
                key=lambda n: (
                    0 if n.get("download_mbps", 0) > 0 else 1,
                    -n.get("download_mbps", 0),
                    n.get("xray_avg_ms", float("inf")),
                )
            )

            bw_nodes = [n for n in best_nodes if n.get("download_mbps", 0) > 0]
            logger.info(f"\n  带宽测速完成: {len(bw_nodes)}/{len(best_nodes)} 个节点测出带宽")
            if bw_nodes:
                logger.info(f"{'序号':<4} {'协议':<7} {'地址':<30} {'带宽(Mbps)':<12} {'延迟(ms)':<10} {'名称'}")
                logger.info(f"{'-'*100}")
                for i, node in enumerate(bw_nodes[:30], 1):
                    logger.info(
                        f"{i:<4} {node['protocol']:<7} {node['address']}:{node['port']:<20} "
                        f"{node.get('download_mbps', 0):<12} {node.get('xray_avg_ms', '-'):<10} "
                        f"{node.get('name', '')[:30]}"
                    )

        except Exception as e:
            logger.error(f"带宽测速失败: {e}")
            logger.info("带宽测速异常，将使用延迟排序结果输出。")
    else:
        if best_nodes:
            logger.info("\n  带宽测速已禁用，跳过。")
            best_nodes.sort(key=lambda n: n.get("xray_avg_ms", float("inf")))

    # ---- 最终输出 ----
    # 将带宽信息追加到节点名称中，方便手机端直接看到带宽
    for node in best_nodes:
        bw = node.get("download_mbps", 0)
        if bw > 0:
            node["name"] = f"{node['name']} | {bw:.1f}Mbps"
        else:
            node["name"] = f"{node['name']} | 带宽未测"
        node["raw"] = rebuild_raw_with_name(node)

    sub_file = output_dir / "best_nodes.txt"

    # 如果没有可用节点，生成假节点提示任务失败，方便手机端更新时感知异常
    if not best_nodes:
        import time
        fail_time = time.strftime("%m-%d %H:%M")
        fake_node_name = f"⚠️采集失败-{fail_time}-请稍后重试"
        fake_raw = f"trojan://fake@127.0.0.1:1#{fake_node_name}"
        sub_content = base64.b64encode(fake_raw.encode("utf-8")).decode("utf-8")
    else:
        sub_content = base64.b64encode(
            "\n".join(node["raw"] for node in best_nodes).encode("utf-8")
        ).decode("utf-8")

    sub_file.write_text(sub_content, encoding="utf-8")

    logger.info(f"\n{'='*80}")
    logger.info(f"输出文件: {sub_file}")
    logger.info(f"完成！共 {len(best_nodes)} 个可用节点（带宽排序，名称含带宽信息）。")


if __name__ == "__main__":
    main()
