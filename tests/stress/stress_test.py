#!/usr/bin/env python3
"""
ClickGraph Server Stress Test

Runs concurrent Cypher queries against the ClickGraph server in sql_only mode
to detect memory leaks, crashes, and performance regressions.

Usage:
    python3 tests/stress/stress_test.py [options]

    # Quick 5-minute smoke test
    python3 tests/stress/stress_test.py --duration 300 --concurrency 10

    # 24-hour endurance test
    python3 tests/stress/stress_test.py --duration 86400 --concurrency 50
"""

import argparse
import asyncio
import json
import os
import random
import signal
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Query patterns
# ---------------------------------------------------------------------------

@dataclass
class QueryPattern:
    query: str
    category: str           # simple, medium, complex, error
    schema: str = "social_integration"
    expect_error: bool = False


SIMPLE_QUERIES = [
    QueryPattern("MATCH (u:User) RETURN u.name LIMIT 1", "simple"),
    QueryPattern("MATCH (u:User) RETURN COUNT(*) AS cnt", "simple"),
    QueryPattern("MATCH (u:User)-[:FOLLOWS]->(v:User) RETURN u.name, v.name LIMIT 10", "simple"),
    QueryPattern("MATCH (u:User) WHERE u.age > 25 RETURN u.name, u.age", "simple"),
    QueryPattern("MATCH (u:User)-[:FOLLOWS]->(v:User) WHERE u.name = 'Alice' RETURN v.name", "simple"),
    QueryPattern("MATCH (u:User)-[:AUTHORED]->(p:Post) RETURN u.name, p.content LIMIT 5", "simple"),
    QueryPattern("MATCH (u:User) RETURN u.name ORDER BY u.name LIMIT 3", "simple"),
    QueryPattern("MATCH (u:User)-[:FOLLOWS]->(v:User) RETURN u.name, COUNT(v) AS cnt", "simple"),
]

MEDIUM_QUERIES = [
    QueryPattern(
        "MATCH (u:User)-[:FOLLOWS]->(v:User) WITH u, COUNT(v) AS follows "
        "RETURN u.name, follows ORDER BY follows DESC",
        "medium",
    ),
    QueryPattern(
        "MATCH (u:User)-[:FOLLOWS*1..2]->(v:User) WHERE u.name = 'Alice' "
        "RETURN DISTINCT v.name",
        "medium",
    ),
    QueryPattern(
        "MATCH (u:User)-[:FOLLOWS*2]->(v:User) RETURN u.name, v.name",
        "medium",
    ),
    QueryPattern(
        "MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(v:User) "
        "RETURN u.name, COUNT(v) AS follows",
        "medium",
    ),
    QueryPattern(
        "MATCH (u:User)-[:FOLLOWS]->(v:User) WITH u, v "
        "MATCH (v)-[:AUTHORED]->(p:Post) RETURN u.name, p.content LIMIT 10",
        "medium",
    ),
    QueryPattern(
        "MATCH (u:User)-[:AUTHORED]->(p:Post) "
        "RETURN u.name, COUNT(p) AS posts ORDER BY posts DESC",
        "medium",
    ),
    QueryPattern(
        "MATCH (u:User)-[:FOLLOWS*1..3]->(v:User) "
        "RETURN u.name, COUNT(DISTINCT v) AS reach",
        "medium",
    ),
    QueryPattern(
        "MATCH (u:User) WITH u ORDER BY u.age DESC LIMIT 5 "
        "RETURN u.name, u.age",
        "medium",
    ),
    QueryPattern(
        "MATCH (u:User)-[:FOLLOWS]->(v:User) "
        "RETURN u.name, COLLECT(v.name) AS friends",
        "medium",
    ),
    QueryPattern(
        "MATCH (u:User)-[:FOLLOWS]->(v:User)-[:FOLLOWS]->(w:User) "
        "WHERE u.name = 'Alice' RETURN DISTINCT w.name",
        "medium",
    ),
]

COMPLEX_QUERIES = [
    QueryPattern(
        "MATCH (u:User)-[:FOLLOWS*1..4]->(v:User) WHERE u.name = 'Alice' "
        "OPTIONAL MATCH (v)-[:AUTHORED]->(p:Post) "
        "RETURN u.name, v.name, p.content LIMIT 20",
        "complex",
    ),
    QueryPattern(
        "MATCH (u:User)-[:FOLLOWS]->(v:User) WITH DISTINCT u, v "
        "MATCH (v)-[:FOLLOWS]->(w:User) "
        "RETURN u.name, v.name, w.name LIMIT 10",
        "complex",
    ),
    QueryPattern(
        "MATCH path = (u:User)-[:FOLLOWS*1..5]->(v:User) "
        "WHERE u.name = 'Alice' "
        "RETURN u.name, v.name, length(path) AS hops LIMIT 20",
        "complex",
    ),
    QueryPattern(
        "MATCH (u:User)-[:FOLLOWS]->(v:User) "
        "WITH u, COLLECT(v.name) AS friends "
        "RETURN u.name, friends, size(friends) AS cnt ORDER BY cnt DESC",
        "complex",
    ),
    QueryPattern(
        "MATCH (u:User)-[:FOLLOWS*0..3]->(v:User) WHERE u.name = 'Alice' "
        "RETURN DISTINCT v.name",
        "complex",
    ),
    QueryPattern(
        "MATCH (u:User)-[:FOLLOWS]->(v:User) WITH u, COUNT(v) AS fc "
        "WHERE fc > 0 "
        "MATCH (u)-[:AUTHORED]->(p:Post) RETURN u.name, fc, COUNT(p) AS posts",
        "complex",
    ),
]

ERROR_QUERIES = [
    QueryPattern("MATCH (n:NonExistentLabel) RETURN n", "error", expect_error=True),
    QueryPattern("MATCH (u:User)-[:NONEXISTENT_REL]->(v) RETURN u", "error", expect_error=True),
    QueryPattern("MATCH", "error", expect_error=True),
    QueryPattern("MATCH (u:User) RETURN !!invalid!!", "error", expect_error=True),
    QueryPattern("", "error", expect_error=True),
    QueryPattern("RETURN 1 + 'text'", "error", expect_error=True),
]

# TestUser/TestProduct queries from test_fixtures schema
TEST_FIXTURE_QUERIES = [
    QueryPattern(
        "MATCH (u:TestUser)-[:TEST_FOLLOWS]->(v:TestUser) RETURN u.name, v.name LIMIT 5",
        "simple", schema="test_fixtures",
    ),
    QueryPattern(
        "MATCH (u:TestUser)-[:TEST_PURCHASED]->(p:TestProduct) RETURN u.name, p.name",
        "medium", schema="test_fixtures",
    ),
    QueryPattern(
        "MATCH (u:TestUser)-[:TEST_FOLLOWS*1..2]->(v:TestUser) "
        "RETURN u.name, COUNT(DISTINCT v) AS reach",
        "medium", schema="test_fixtures",
    ),
]


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

@dataclass
class IntervalMetrics:
    """Metrics for a single reporting interval."""
    start_time: float = 0.0
    requests: int = 0
    successes: int = 0
    errors_400: int = 0
    errors_500: int = 0
    errors_timeout: int = 0
    errors_connection: int = 0
    latencies_ms: list = field(default_factory=list)
    latencies_by_cat: dict = field(default_factory=lambda: {
        "simple": [], "medium": [], "complex": [], "error": []
    })
    error_messages: dict = field(default_factory=dict)  # message -> count


class MetricsCollector:
    """Thread-safe metrics collection."""

    def __init__(self):
        self.current = IntervalMetrics(start_time=time.time())
        self.cumulative = IntervalMetrics(start_time=time.time())
        self.memory_samples = []  # (timestamp, vmrss_mb)
        self._lock = asyncio.Lock()

    async def record(self, latency_ms: float, category: str, status: int,
                     error_msg: Optional[str] = None):
        async with self._lock:
            self.current.requests += 1
            self.cumulative.requests += 1
            self.current.latencies_ms.append(latency_ms)
            self.cumulative.latencies_ms.append(latency_ms)

            if category in self.current.latencies_by_cat:
                self.current.latencies_by_cat[category].append(latency_ms)
            if category in self.cumulative.latencies_by_cat:
                self.cumulative.latencies_by_cat[category].append(latency_ms)

            if 200 <= status < 300:
                self.current.successes += 1
                self.cumulative.successes += 1
            elif status == 400:
                self.current.errors_400 += 1
                self.cumulative.errors_400 += 1
            elif status >= 500:
                self.current.errors_500 += 1
                self.cumulative.errors_500 += 1
            elif status == -1:  # timeout
                self.current.errors_timeout += 1
                self.cumulative.errors_timeout += 1
            elif status == -2:  # connection error
                self.current.errors_connection += 1
                self.cumulative.errors_connection += 1

            if error_msg:
                # Truncate long messages
                key = error_msg[:120]
                self.current.error_messages[key] = self.current.error_messages.get(key, 0) + 1
                self.cumulative.error_messages[key] = self.cumulative.error_messages.get(key, 0) + 1

    async def rotate_interval(self) -> IntervalMetrics:
        """Return current interval metrics and start a new interval."""
        async with self._lock:
            snapshot = self.current
            self.current = IntervalMetrics(start_time=time.time())
            return snapshot

    def record_memory(self, vmrss_mb: float):
        self.memory_samples.append((time.time(), vmrss_mb))


# ---------------------------------------------------------------------------
# Memory monitor
# ---------------------------------------------------------------------------

def get_vmrss_mb(pid: int) -> Optional[float]:
    """Read VmRSS from /proc/PID/status (Linux only)."""
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    # VmRSS:   123456 kB
                    parts = line.split()
                    return int(parts[1]) / 1024.0  # kB -> MB
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return None
    return None


def find_server_pid(port: int) -> Optional[int]:
    """Find PID of process listening on given port."""
    try:
        import subprocess
        result = subprocess.run(
            ["lsof", "-ti", f":{port}"],
            capture_output=True, text=True, timeout=5
        )
        if result.stdout.strip():
            return int(result.stdout.strip().split('\n')[0])
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------

def percentile(data: list, p: float) -> float:
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * p / 100.0
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[f]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


def format_latency(data: list) -> str:
    if not data:
        return "n/a"
    return (f"P50={percentile(data, 50):.0f}ms "
            f"P95={percentile(data, 95):.0f}ms "
            f"P99={percentile(data, 99):.0f}ms "
            f"max={max(data):.0f}ms")


def print_interval_report(interval_num: int, elapsed: float, interval: IntervalMetrics,
                          cumulative: IntervalMetrics, vmrss_mb: Optional[float],
                          initial_vmrss: Optional[float]):
    duration = time.time() - interval.start_time
    if duration <= 0:
        duration = 1
    rps = interval.requests / duration

    print(f"\n[{time.strftime('%H:%M:%S')}] INTERVAL {interval_num} "
          f"({elapsed:.0f}s elapsed)")
    print(f"  Requests: {interval.requests} ({rps:.1f} req/s) | "
          f"Success: {interval.successes} | "
          f"Err400: {interval.errors_400} | "
          f"Err500: {interval.errors_500} | "
          f"Timeout: {interval.errors_timeout} | "
          f"ConnErr: {interval.errors_connection}")
    print(f"  Latency: {format_latency(interval.latencies_ms)}")

    for cat in ["simple", "medium", "complex", "error"]:
        cat_data = interval.latencies_by_cat.get(cat, [])
        if cat_data:
            print(f"    {cat:8s}: {len(cat_data):5d} queries | {format_latency(cat_data)}")

    if vmrss_mb is not None:
        growth = ""
        if initial_vmrss and initial_vmrss > 0:
            pct = (vmrss_mb - initial_vmrss) / initial_vmrss * 100
            growth = f" ({pct:+.1f}% from start)"
        print(f"  Memory: VmRSS={vmrss_mb:.1f} MB{growth}")

    cum_duration = time.time() - cumulative.start_time
    cum_rps = cumulative.requests / max(cum_duration, 1)
    print(f"  Cumulative: {cumulative.requests} total | {cum_rps:.1f} avg req/s | "
          f"{cumulative.successes} ok | "
          f"{cumulative.errors_500} server errors")

    # Top errors this interval
    if interval.error_messages:
        top = sorted(interval.error_messages.items(), key=lambda x: -x[1])[:3]
        print("  Top errors:")
        for msg, count in top:
            print(f"    ({count}x) {msg}")

    sys.stdout.flush()


def print_final_report(cumulative: IntervalMetrics, memory_samples: list,
                       initial_vmrss: Optional[float], duration: float):
    print("\n" + "=" * 70)
    print("FINAL STRESS TEST REPORT")
    print("=" * 70)

    total = cumulative.requests
    success = cumulative.successes
    err_rate = (total - success) / max(total, 1) * 100
    rps = total / max(duration, 1)

    print(f"\nDuration: {duration:.0f}s ({duration/3600:.1f} hours)")
    print(f"Total queries: {total:,}")
    print(f"Success rate: {success:,}/{total:,} ({100-err_rate:.1f}%)")
    print(f"Average throughput: {rps:.1f} req/s")
    print(f"Errors: 400={cumulative.errors_400} | 500={cumulative.errors_500} | "
          f"timeout={cumulative.errors_timeout} | conn={cumulative.errors_connection}")

    print(f"\nLatency (overall): {format_latency(cumulative.latencies_ms)}")
    for cat in ["simple", "medium", "complex", "error"]:
        cat_data = cumulative.latencies_by_cat.get(cat, [])
        if cat_data:
            print(f"  {cat:8s}: {format_latency(cat_data)} ({len(cat_data)} queries)")

    if memory_samples:
        vmrss_values = [m for _, m in memory_samples]
        peak = max(vmrss_values)
        final = vmrss_values[-1]
        start = vmrss_values[0]
        growth_pct = (final - start) / max(start, 1) * 100
        print(f"\nMemory: start={start:.1f}MB | peak={peak:.1f}MB | "
              f"final={final:.1f}MB | growth={growth_pct:+.1f}%")

        # Simple leak detection
        if len(vmrss_values) > 10:
            first_half = statistics.mean(vmrss_values[:len(vmrss_values)//2])
            second_half = statistics.mean(vmrss_values[len(vmrss_values)//2:])
            if second_half > first_half * 1.1:
                print(f"  WARNING: Memory grew {((second_half/first_half)-1)*100:.1f}% "
                      f"between first and second half")

    if cumulative.error_messages:
        print("\nTop error messages:")
        top = sorted(cumulative.error_messages.items(), key=lambda x: -x[1])[:10]
        for msg, count in top:
            print(f"  ({count:,}x) {msg}")

    # Verdict
    print("\nVERDICT:")
    passed = True
    if cumulative.errors_500 > 0:
        # Unexpected 500s on non-error queries are concerning
        non_error_500_ratio = cumulative.errors_500 / max(total, 1)
        if non_error_500_ratio > 0.01:
            print(f"  WARN: {cumulative.errors_500} server errors (500) "
                  f"({non_error_500_ratio*100:.2f}%)")
    if cumulative.errors_connection > 0:
        print(f"  FAIL: {cumulative.errors_connection} connection errors "
              "(server may have crashed)")
        passed = False
    if cumulative.errors_timeout > total * 0.05:
        print(f"  WARN: {cumulative.errors_timeout} timeouts "
              f"({cumulative.errors_timeout/max(total,1)*100:.1f}%)")

    if memory_samples and len(vmrss_values) > 10:
        if vmrss_values[-1] > vmrss_values[0] * 2:
            print(f"  FAIL: Memory doubled ({vmrss_values[0]:.0f}MB -> {vmrss_values[-1]:.0f}MB)")
            passed = False

    if passed:
        print("  PASS: No critical issues detected")
    print("=" * 70)
    sys.stdout.flush()


# ---------------------------------------------------------------------------
# Main stress test loop
# ---------------------------------------------------------------------------

async def send_query(session, base_url: str, pattern: QueryPattern,
                     client_timeout) -> tuple:
    """Send a single query, return (latency_ms, status_code, error_msg)."""
    payload = {
        "query": f"USE {pattern.schema}\n{pattern.query}",
        "sql_only": True,
    }

    start = time.monotonic()
    try:
        async with session.post(
            f"{base_url}/query",
            json=payload,
            timeout=client_timeout,
        ) as resp:
            latency_ms = (time.monotonic() - start) * 1000
            status = resp.status
            error_msg = None
            if status >= 400:
                try:
                    body = await resp.json()
                    error_msg = body.get("error") or body.get("message") or str(body)
                except Exception:
                    error_msg = await resp.text()
            return latency_ms, status, error_msg
    except asyncio.TimeoutError:
        latency_ms = (time.monotonic() - start) * 1000
        return latency_ms, -1, "timeout"
    except Exception as e:
        latency_ms = (time.monotonic() - start) * 1000
        return latency_ms, -2, str(e)


async def worker(worker_id: int, queries: list, session, base_url: str,
                 metrics: MetricsCollector, stop_event: asyncio.Event,
                 client_timeout, rng: random.Random):
    """Worker coroutine that continuously sends queries until stopped."""
    n = len(queries)
    idx = rng.randint(0, n - 1)

    while not stop_event.is_set():
        pattern = queries[idx % n]
        idx += 1

        latency_ms, status, error_msg = await send_query(
            session, base_url, pattern, client_timeout
        )

        # For error-category queries, 400 is expected success
        if pattern.expect_error and status == 400:
            await metrics.record(latency_ms, pattern.category, 200)
        else:
            await metrics.record(latency_ms, pattern.category, status, error_msg)

        # Small random delay to avoid pure spin (0-5ms)
        await asyncio.sleep(rng.uniform(0, 0.005))


async def memory_monitor_loop(metrics: MetricsCollector, server_pid: int,
                               interval: float, stop_event: asyncio.Event):
    """Periodically sample server memory."""
    while not stop_event.is_set():
        vmrss = get_vmrss_mb(server_pid)
        if vmrss is not None:
            metrics.record_memory(vmrss)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


async def run_stress_test(args):
    import aiohttp
    from urllib.parse import urlparse

    base_url = args.server
    duration = args.duration
    concurrency = args.concurrency
    report_interval = args.report_interval
    query_timeout = args.query_timeout
    seed = args.seed if args.seed is not None else int(time.time())
    client_timeout = aiohttp.ClientTimeout(total=query_timeout)

    print(f"ClickGraph Stress Test")
    print(f"  Server: {base_url}")
    print(f"  Duration: {duration}s ({duration/3600:.1f}h)")
    print(f"  Concurrency: {concurrency}")
    print(f"  Report interval: {report_interval}s")
    print(f"  Query timeout: {query_timeout}s")
    print(f"  Random seed: {seed}")
    print()

    # Build query pool with distribution
    rng = random.Random(seed)
    queries = []
    queries.extend(SIMPLE_QUERIES)
    queries.extend(TEST_FIXTURE_QUERIES)
    # Weight medium higher
    queries.extend(MEDIUM_QUERIES * 2)
    queries.extend(COMPLEX_QUERIES)
    queries.extend(ERROR_QUERIES)
    rng.shuffle(queries)

    print(f"Query pool: {len(queries)} patterns "
          f"(simple={len(SIMPLE_QUERIES)+len([q for q in TEST_FIXTURE_QUERIES if q.category=='simple'])}, "
          f"medium={len(MEDIUM_QUERIES)+len([q for q in TEST_FIXTURE_QUERIES if q.category=='medium'])}, "
          f"complex={len(COMPLEX_QUERIES)}, "
          f"error={len(ERROR_QUERIES)})")

    # Find server PID for memory monitoring
    parsed = urlparse(base_url)
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    server_pid = find_server_pid(port)
    initial_vmrss = None
    if server_pid:
        initial_vmrss = get_vmrss_mb(server_pid)
        if initial_vmrss is not None:
            print(f"Server PID: {server_pid} | Initial VmRSS: {initial_vmrss:.1f} MB")
        else:
            print(f"Server PID: {server_pid} | Initial VmRSS: unknown (unable to read /proc)")
    else:
        print("WARNING: Could not find server PID for memory monitoring")

    # Verify server is up
    health_timeout = aiohttp.ClientTimeout(total=10.0)
    async with aiohttp.ClientSession() as test_session:
        try:
            resp_lat, resp_status, _ = await send_query(
                test_session, base_url,
                QueryPattern("MATCH (u:User) RETURN u.name LIMIT 1", "simple"),
                health_timeout,
            )
            if resp_status >= 500:
                print(f"ERROR: Server returned {resp_status} on health check")
                return
            print(f"Server health check: status={resp_status} latency={resp_lat:.0f}ms")
        except Exception as e:
            print(f"ERROR: Cannot reach server at {base_url}: {e}")
            return

    metrics = MetricsCollector()
    stop_event = asyncio.Event()

    # Graceful shutdown on SIGINT/SIGTERM
    def handle_signal(sig, frame):
        print(f"\nReceived signal {sig}, shutting down gracefully...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    start_time = time.time()

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=concurrency + 10),
    ) as session:
        # Start workers
        workers = []
        for i in range(concurrency):
            worker_rng = random.Random(seed + i + 1)
            task = asyncio.create_task(
                worker(i, queries, session, base_url, metrics,
                       stop_event, client_timeout, worker_rng)
            )
            workers.append(task)

        # Start memory monitor
        mem_task = None
        if server_pid:
            mem_task = asyncio.create_task(
                memory_monitor_loop(metrics, server_pid, 30.0, stop_event)
            )

        # Reporting loop
        interval_num = 0
        try:
            while not stop_event.is_set():
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=report_interval)
                except asyncio.TimeoutError:
                    pass

                elapsed = time.time() - start_time
                if elapsed >= duration:
                    print(f"\nDuration {duration}s reached, stopping...")
                    stop_event.set()
                    break

                interval_num += 1
                interval = await metrics.rotate_interval()

                vmrss = get_vmrss_mb(server_pid) if server_pid else None
                print_interval_report(
                    interval_num, elapsed, interval,
                    metrics.cumulative, vmrss, initial_vmrss,
                )

                # Check for server crash
                if server_pid and get_vmrss_mb(server_pid) is None:
                    print("\nFATAL: Server process appears to have died!")
                    stop_event.set()
                    break

        except Exception as e:
            print(f"\nUnexpected error in main loop: {e}")
            stop_event.set()

        # Wait for workers to finish
        stop_event.set()
        for task in workers:
            task.cancel()
        if mem_task:
            mem_task.cancel()

        await asyncio.gather(*workers, return_exceptions=True)
        if mem_task:
            await asyncio.gather(mem_task, return_exceptions=True)

    # Final report
    total_duration = time.time() - start_time
    print_final_report(
        metrics.cumulative, metrics.memory_samples,
        initial_vmrss, total_duration,
    )


def main():
    parser = argparse.ArgumentParser(description="ClickGraph Server Stress Test")
    parser.add_argument("--server", default="http://localhost:8080",
                        help="Server base URL (default: http://localhost:8080)")
    parser.add_argument("--duration", type=int, default=3600,
                        help="Test duration in seconds (default: 3600)")
    parser.add_argument("--concurrency", type=int, default=20,
                        help="Number of concurrent workers (default: 20)")
    parser.add_argument("--report-interval", type=int, default=30,
                        help="Stats report interval in seconds (default: 30)")
    parser.add_argument("--query-timeout", type=float, default=30.0,
                        help="Per-query timeout in seconds (default: 30)")
    parser.add_argument("--seed", type=int, default=None,
                        help="Random seed for reproducibility")
    args = parser.parse_args()

    try:
        import aiohttp  # noqa: F401
    except ImportError:
        print("ERROR: aiohttp required. Install with: pip install aiohttp")
        sys.exit(1)

    asyncio.run(run_stress_test(args))


if __name__ == "__main__":
    main()
