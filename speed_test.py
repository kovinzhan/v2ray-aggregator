"""
测速模块：
- TCP 端口可达检测（简化后的初筛）
- xray 真实代理验证
- 带宽测速
- 公共的 xray 进程管理上下文管理器
"""

import os
import json
import time
import shutil
import socket
import signal
import logging
import tempfile
import subprocess
import statistics
from pathlib import Path
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from xray_config import (
    find_free_ports, build_xray_multi_config, HEADERS,
)

logger = logging.getLogger(__name__)


# ============================================================
# 公共：xray 进程上下文管理器
# ============================================================

@contextmanager
def xray_process_context(xray_bin, xray_config, nodes_with_ports, failed_indices,
                         startup_wait=2, label="xray"):
    """
    上下文管理器：启动 xray 进程，等待就绪，yield，最终清理。

    用法:
        with xray_process_context(...) as proc_info:
            if proc_info is None:
                # 启动失败
            else:
                # proc_info = {"proc": xray_proc, "tmp_dir": tmp_dir}
                # 在此做测速...

    自动处理：写配置、启动进程、端口就绪检测、杀进程、清理临时目录。
    """
    tmp_dir = tempfile.mkdtemp(prefix=f"xray_{label}_")
    config_file = Path(tmp_dir) / "config.json"
    config_file.write_text(json.dumps(xray_config, indent=2), encoding="utf-8")

    xray_proc = None
    try:
        # 启动 xray
        xray_proc = subprocess.Popen(
            [xray_bin, "run", "-c", str(config_file)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid,
        )

        # 等待就绪
        max_wait = startup_wait * 2
        waited = 0
        check_interval = 0.5
        while waited < max_wait:
            time.sleep(check_interval)
            waited += check_interval
            if xray_proc.poll() is not None:
                break
            if waited >= startup_wait:
                first_testable = next(
                    ((idx, port) for idx, (_, port) in enumerate(nodes_with_ports)
                     if idx not in failed_indices),
                    None
                )
                if first_testable:
                    try:
                        test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        test_sock.settimeout(1)
                        test_sock.connect(("127.0.0.1", first_testable[1]))
                        test_sock.close()
                        logger.info(f"  [{label}] xray 端口就绪（等待 {waited:.1f}s）")
                        break
                    except Exception:
                        pass

        # 检查是否已退出
        if xray_proc.poll() is not None:
            stderr_out = xray_proc.stderr.read().decode(errors="ignore")
            stdout_out = xray_proc.stdout.read().decode(errors="ignore")
            exit_code = xray_proc.returncode
            logger.error(f"  [{label}] xray 进程启动失败 (exit_code={exit_code})")
            if stderr_out:
                logger.error(f"  stderr: {stderr_out[:500]}")
            if stdout_out:
                logger.error(f"  stdout: {stdout_out[:500]}")

            # 保存崩溃现场
            try:
                debug_dir = Path(__file__).parent / "debug"
                debug_dir.mkdir(exist_ok=True)
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                (debug_dir / f"xray_crash_{timestamp}_config.json").write_text(
                    config_file.read_text(encoding="utf-8"), encoding="utf-8")
                (debug_dir / f"xray_crash_{timestamp}_log.txt").write_text(
                    f"exit_code: {exit_code}\nlabel: {label}\n"
                    f"\n=== STDOUT ===\n{stdout_out}\n\n=== STDERR ===\n{stderr_out}\n",
                    encoding="utf-8")
                logger.info(f"  崩溃现场已保存到 debug/xray_crash_{timestamp}_*")
            except Exception as dump_err:
                logger.warning(f"  保存崩溃现场失败: {dump_err}")

            yield None
            return

        logger.info(f"  [{label}] xray 进程已启动 (PID={xray_proc.pid})")
        yield {"proc": xray_proc, "tmp_dir": tmp_dir}

    except Exception as e:
        logger.error(f"  [{label}] xray 启动异常: {e}")
        yield None
    finally:
        # 清理进程
        if xray_proc and xray_proc.poll() is None:
            try:
                os.killpg(os.getpgid(xray_proc.pid), signal.SIGTERM)
                xray_proc.wait(timeout=5)
            except Exception:
                try:
                    xray_proc.kill()
                except Exception:
                    pass
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ============================================================
# TCP 端口可达检测（简化后的初筛）
# ============================================================

def tcp_ping(host, port, timeout=3):
    """单次 TCP 连接检测，返回延迟(ms)，失败返回 None"""
    try:
        start = time.time()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, port))
        sock.close()
        return (time.time() - start) * 1000
    except Exception:
        return None


def test_node_reachable(node, ping_count=2, timeout=3):
    """
    简化的端口可达检测：多次 TCP ping 取均值。
    不再做 DNS 预解析、TLS 握手、多轮、抖动计算等复杂逻辑。
    返回带测速结果的 node dict。
    """
    address = node["address"]
    port = node["port"]

    latencies = []
    for _ in range(ping_count):
        lat = tcp_ping(address, port, timeout)
        latencies.append(lat)

    successes = [r for r in latencies if r is not None]
    total = len(latencies)
    loss_rate = 1.0 - len(successes) / total if total > 0 else 1.0

    if successes:
        avg_latency = statistics.mean(successes)
    else:
        avg_latency = float("inf")

    return {
        **node,
        "avg_latency_ms": round(avg_latency, 1) if avg_latency != float("inf") else float("inf"),
        "loss_rate": round(loss_rate, 3),
        "success_count": len(successes),
        "total_count": total,
    }


def batch_test_nodes(nodes, max_workers=500, ping_count=2, timeout=3):
    """并发 TCP 端口可达检测"""
    results = []
    total = len(nodes)

    logger.info(f"开始 TCP 可达检测，共 {total} 个节点，{ping_count} 次/节点，并发 {max_workers}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(test_node_reachable, node, ping_count, timeout): node
            for node in nodes
        }

        done_count = 0
        for future in as_completed(futures):
            done_count += 1
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.debug(f"测试异常: {e}")

            if done_count % 100 == 0 or done_count == total:
                logger.info(f"  进度: {done_count}/{total}")

    return results


# ============================================================
# xray 真实代理验证
# ============================================================

_IP_CHECK_URLS = (
    "https://api.ipify.org?format=text",
    "https://ifconfig.me/ip",
    "https://icanhazip.com",
)


def get_local_ip():
    """获取本机公网 IP"""
    for url in _IP_CHECK_URLS:
        try:
            resp = requests.get(url, timeout=5, headers=HEADERS)
            if resp.status_code == 200:
                ip = resp.text.strip()
                if ip and len(ip) < 50:
                    return ip
        except Exception:
            continue
    return None


def _xray_fail(node, error, latencies=None):
    """构造 xray 测试失败结果"""
    return {**node, "xray_ok": False, "xray_avg_ms": float("inf"),
            "xray_latencies": latencies or [], "xray_error": error}


def xray_test_via_proxy(node, socks_port, test_count=3, timeout=10, local_ip=None):
    """
    通过已启动的 xray 代理端口测试单个节点的真实可用性。
    验证：出口IP ≠ 本机IP → generate_204 连通 → 内容下载验证
    """
    proxy = f"socks5h://127.0.0.1:{socks_port}"
    proxies = {"http": proxy, "https": proxy}

    # 阶段 0: 出口 IP 验证
    exit_ip = None
    for url in _IP_CHECK_URLS:
        try:
            resp = requests.get(url, proxies=proxies, timeout=timeout, headers=HEADERS)
            if resp.status_code == 200:
                exit_ip = resp.text.strip()
                if exit_ip and len(exit_ip) < 50:
                    break
                exit_ip = None
        except Exception:
            continue

    if not exit_ip:
        return _xray_fail(node, "exit_ip_check_failed")
    if local_ip and exit_ip == local_ip:
        return _xray_fail(node, f"proxy_bypass_detected(exit_ip={exit_ip}==local_ip)")

    # 阶段 1: 快速连通性测试
    quick_urls = [("http://www.gstatic.com/generate_204", 204), ("http://cp.cloudflare.com/", 200)]
    latencies = []
    for i in range(test_count):
        url, expected_code = quick_urls[i % len(quick_urls)]
        try:
            start = time.time()
            resp = requests.get(url, proxies=proxies, timeout=timeout, headers=HEADERS)
            elapsed = (time.time() - start) * 1000
            latencies.append(elapsed if resp.status_code in (expected_code, 200, 204) else None)
        except Exception:
            latencies.append(None)
        if i < test_count - 1:
            time.sleep(0.3)

    quick_successes = [l for l in latencies if l is not None]
    if not quick_successes:
        return _xray_fail(node, "quick_check_all_failed", latencies)

    # 阶段 2: 内容可达性验证
    content_urls = [
        ("https://www.google.com/robots.txt", 200, "User-agent"),
        ("https://www.cloudflare.com/cdn-cgi/trace", 200, "warp="),
    ]
    content_latencies = []
    for url, expected_code, expected_content in content_urls:
        try:
            start = time.time()
            resp = requests.get(url, proxies=proxies, timeout=timeout, headers=HEADERS)
            elapsed = (time.time() - start) * 1000
            if resp.status_code == expected_code and (not expected_content or expected_content in resp.text[:2000]):
                content_latencies.append(elapsed)
                break
        except Exception:
            continue

    if not content_latencies:
        return _xray_fail(node, "content_verify_failed", latencies)

    # 合并所有延迟
    all_latencies = quick_successes + content_latencies
    avg = statistics.mean(all_latencies)
    jitter = statistics.stdev(all_latencies) if len(all_latencies) > 1 else 0

    return {
        **node,
        "xray_ok": True,
        "xray_avg_ms": round(avg, 1),
        "xray_min_ms": round(min(all_latencies), 1),
        "xray_max_ms": round(max(all_latencies), 1),
        "xray_jitter_ms": round(jitter, 1),
        "xray_success": len(all_latencies),
        "xray_total": test_count + 1,
        "xray_latencies": [round(l, 1) if l else None for l in latencies] + [round(l, 1) for l in content_latencies],
        "xray_error": "",
        "content_verified": True,
        "exit_ip": exit_ip,
    }


def batch_xray_test(xray_bin, candidate_nodes, config):
    """
    单进程多节点并发测速：使用公共上下文管理器统一管理 xray 进程。
    """
    total = len(candidate_nodes)
    max_workers = config.get("xray_max_workers", 100)
    test_count = config.get("xray_test_count", 2)
    timeout = config.get("xray_test_timeout", 8)
    startup_wait = config.get("xray_startup_wait", 2)

    logger.info(f"  xray 真实代理测速（单进程模式）：{total} 个候选，并发 {max_workers}")
    logger.info(f"  每节点 {test_count} 次请求，超时 {timeout}s")

    # 获取本机公网 IP
    local_ip = get_local_ip()
    if local_ip:
        logger.info(f"  本机公网 IP: {local_ip}（代理出口 IP 必须与此不同）")
    else:
        logger.warning("  无法获取本机公网 IP，跳过出口 IP 验证")

    # 分配端口 & 构建配置
    ports = find_free_ports(total)
    nodes_with_ports = list(zip(candidate_nodes, ports))
    xray_config, failed_indices = build_xray_multi_config(nodes_with_ports)

    results = [_xray_fail(candidate_nodes[idx], "config_build_failed") for idx in failed_indices]

    if xray_config is None:
        logger.warning("  所有节点配置构建失败，跳过 xray 测速")
        return results

    # 验证 xray 二进制
    try:
        version_result = subprocess.run([xray_bin, "version"], capture_output=True, timeout=10)
        logger.info(f"  xray 版本: {version_result.stdout.decode(errors='ignore').splitlines()[0] if version_result.stdout else '未知'}")
        if version_result.returncode != 0:
            err_msg = version_result.stderr.decode(errors="ignore")[:300]
            logger.error(f"  xray 二进制不可用: {err_msg}")
            results.extend(_xray_fail(node, f"xray_binary_invalid: {err_msg[:100]}")
                          for idx, (node, _) in enumerate(nodes_with_ports) if idx not in failed_indices)
            return results
    except Exception as ve:
        logger.error(f"  xray 二进制验证失败: {ve}")

    # 使用上下文管理器启动 xray
    with xray_process_context(xray_bin, xray_config, nodes_with_ports, failed_indices,
                              startup_wait=startup_wait, label="proxy_test") as proc_info:
        if proc_info is None:
            results.extend(_xray_fail(node, "xray_start_failed")
                          for idx, (node, _) in enumerate(nodes_with_ports) if idx not in failed_indices)
            return results

        logger.info(f"  开始并发测速...")

        testable = [(idx, node, port) for idx, (node, port) in enumerate(nodes_with_ports)
                    if idx not in failed_indices]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(xray_test_via_proxy, node, port, test_count, timeout, local_ip): (idx, node)
                for idx, node, port in testable
            }

            done_count = 0
            for future in as_completed(futures):
                done_count += 1
                idx, node = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    status = "✓" if result.get("xray_ok") else "✗"
                    avg = result.get("xray_avg_ms", "∞")
                    err = result.get("xray_error", "")
                    name = result.get("name", "")[:20]
                    verified = " [内容验证✓]" if result.get("content_verified") else ""
                    exit_ip = result.get("exit_ip", "")
                    ip_info = f" [出口:{exit_ip}]" if exit_ip else ""
                    logger.info(
                        f"  [{done_count}/{len(testable)}] {status} {node['address']}:{node['port']} "
                        f"→ {avg}ms {f'({err})' if err else ''}{verified}{ip_info} {name}"
                    )
                except Exception as e:
                    results.append(_xray_fail(node, str(e)))
                    logger.warning(f"  [{done_count}/{len(testable)}] 测试异常: {e}")

    logger.info(f"  xray 测速完成，共 {len(results)} 个结果")
    return results


# ============================================================
# 带宽测速
# ============================================================

def bandwidth_test_single(socks_port, download_bytes=2*1024*1024, timeout=15):
    """通过代理下载文件，计算带宽（Mbps）"""
    proxy = f"socks5h://127.0.0.1:{socks_port}"
    proxies = {"http": proxy, "https": proxy}
    url = f"https://speed.cloudflare.com/__down?bytes={download_bytes}"

    try:
        start = time.time()
        total_bytes = 0
        resp = requests.get(url, proxies=proxies, timeout=timeout,
                           headers=HEADERS, stream=True)
        if resp.status_code != 200:
            return None

        for chunk in resp.iter_content(chunk_size=65536):
            total_bytes += len(chunk)
            if time.time() - start > timeout:
                break

        elapsed = time.time() - start
        if elapsed <= 0 or total_bytes < download_bytes * 0.5:
            return None

        speed_bps = total_bytes * 8 / elapsed
        speed_mbps = speed_bps / 1_000_000
        return round(speed_mbps, 2)

    except Exception:
        return None


def batch_bandwidth_test(xray_bin, nodes_with_results, config):
    """
    批量带宽测速：使用公共上下文管理器统一管理 xray 进程。
    """
    download_bytes = config.get("bandwidth_download_bytes", 2 * 1024 * 1024)
    timeout = config.get("bandwidth_timeout", 15)
    max_workers = config.get("bandwidth_max_workers", 100)
    top_n = config.get("bandwidth_top_n", 0)
    startup_wait = config.get("xray_startup_wait", 2)

    # 按延迟排序，取 Top N
    sorted_nodes = sorted(nodes_with_results, key=lambda n: n.get("xray_avg_ms", float("inf")))
    if top_n > 0:
        sorted_nodes = sorted_nodes[:top_n]

    total = len(sorted_nodes)
    logger.info(f"\n  带宽测速：{total} 个节点，并发 {max_workers}，下载 {download_bytes // 1024}KB/节点")

    # 分配端口 & 构建配置
    ports = find_free_ports(total)
    nodes_with_ports = list(zip(sorted_nodes, ports))
    xray_config, failed_indices = build_xray_multi_config(nodes_with_ports)

    if xray_config is None:
        logger.warning("  带宽测速：所有节点配置构建失败")
        return sorted_nodes

    # 使用上下文管理器启动 xray
    with xray_process_context(xray_bin, xray_config, nodes_with_ports, failed_indices,
                              startup_wait=startup_wait, label="bandwidth") as proc_info:
        if proc_info is None:
            logger.error("  带宽测速：xray 进程启动失败")
            return sorted_nodes

        logger.info(f"  带宽测速：开始并发下载测速...")

        testable = [(idx, node, port) for idx, (node, port) in enumerate(nodes_with_ports)
                    if idx not in failed_indices]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(bandwidth_test_single, port, download_bytes, timeout): (idx, node)
                for idx, node, port in testable
            }

            done_count = 0
            success_count = 0
            for future in as_completed(futures):
                done_count += 1
                idx, node = futures[future]
                try:
                    mbps = future.result()
                    if mbps is not None:
                        node["download_mbps"] = mbps
                        success_count += 1
                        logger.info(
                            f"  [{done_count}/{len(testable)}] ✓ {node['address']}:{node['port']} "
                            f"→ {mbps} Mbps"
                        )
                    else:
                        node["download_mbps"] = 0
                        logger.debug(
                            f"  [{done_count}/{len(testable)}] ✗ {node['address']}:{node['port']} "
                            f"→ 下载失败"
                        )
                except Exception as e:
                    node["download_mbps"] = 0
                    logger.debug(f"  [{done_count}/{len(testable)}] 异常: {e}")

                if done_count % 20 == 0 or done_count == len(testable):
                    logger.info(f"  带宽测速进度: {done_count}/{len(testable)}，成功 {success_count}")

        logger.info(f"  带宽测速完成：{success_count}/{len(testable)} 个节点测出带宽")

    return sorted_nodes
