"""
CloudOS-RL Load Test
=====================
Sends N concurrent schedule requests and reports latency statistics.

Usage:
  python scripts/load_test.py
  python scripts/load_test.py --requests 200 --concurrency 10 --url http://localhost:8001
  python scripts/load_test.py --requests 500 --concurrency 20

Output:
  - total requests
  - success rate
  - avg / p50 / p95 / p99 / max latency
  - requests per second
  - error breakdown
"""

import argparse
import json
import statistics
import threading
import time
from typing import List
from urllib import request, error

BASE_PAYLOADS = [
    {"workload_type": "training",  "cpu_request_vcpu": 4,  "memory_request_gb": 8,  "is_spot_tolerant": True},
    {"workload_type": "inference", "cpu_request_vcpu": 2,  "memory_request_gb": 4,  "is_spot_tolerant": False},
    {"workload_type": "batch",     "cpu_request_vcpu": 8,  "memory_request_gb": 16, "is_spot_tolerant": True},
    {"workload_type": "streaming", "cpu_request_vcpu": 1,  "memory_request_gb": 2,  "is_spot_tolerant": False},
]


def _send_one(url: str, payload: dict) -> dict:
    """Sends a single request. Returns result dict."""
    t0  = time.perf_counter()
    try:
        body = json.dumps(payload).encode()
        req  = request.Request(
            f"{url}/api/v1/schedule",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(req, timeout=15) as resp:
            data     = json.loads(resp.read())
            elapsed  = (time.perf_counter() - t0) * 1000
            return {
                "ok":      True,
                "latency": elapsed,
                "cloud":   data.get("cloud", "?"),
                "status":  200,
            }
    except error.HTTPError as e:
        return {"ok": False, "latency": (time.perf_counter() - t0) * 1000,
                "error": f"HTTP {e.code}", "status": e.code}
    except Exception as exc:
        return {"ok": False, "latency": (time.perf_counter() - t0) * 1000,
                "error": str(exc)[:60], "status": 0}


def run_load_test(url: str, n_requests: int, concurrency: int) -> None:
    results: List[dict] = []
    lock     = threading.Lock()
    counter  = [0]

    def worker(idx: int):
        payload = BASE_PAYLOADS[idx % len(BASE_PAYLOADS)]
        result  = _send_one(url, payload)
        with lock:
            results.append(result)
            counter[0] += 1
            done = counter[0]
        if done % 50 == 0 or done == n_requests:
            print(f"  Progress: {done}/{n_requests}", end="\r", flush=True)

    print(f"\n{'='*60}")
    print(f"  CloudOS-RL Load Test")
    print(f"{'='*60}")
    print(f"  Target      : {url}")
    print(f"  Requests    : {n_requests}")
    print(f"  Concurrency : {concurrency}")
    print(f"{'='*60}\n")

    # Warm up — single request to ensure agent is loaded
    print("  Warming up...")
    warm = _send_one(url, BASE_PAYLOADS[0])
    if not warm["ok"]:
        print(f"  ❌ Warm-up failed: {warm.get('error', '?')}. Is the API running?")
        return
    print(f"  Warm-up OK — latency: {warm['latency']:.0f}ms\n")
    print(f"  Running load test...")

    t_start = time.perf_counter()

    threads = []
    for i in range(n_requests):
        while threading.active_count() - 1 >= concurrency:
            time.sleep(0.005)
        t = threading.Thread(target=worker, args=(i,), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    elapsed_total = (time.perf_counter() - t_start)

    # ── Report ────────────────────────────────────────────────────────────────
    ok_results     = [r for r in results if r["ok"]]
    fail_results   = [r for r in results if not r["ok"]]
    latencies      = sorted(r["latency"] for r in ok_results)

    def pct(lst, p):
        if not lst: return 0
        idx = max(0, int(len(lst) * p / 100) - 1)
        return lst[idx]

    avg_lat  = statistics.mean(latencies) if latencies else 0
    errors   = {}
    for r in fail_results:
        k = r.get("error", "unknown")
        errors[k] = errors.get(k, 0) + 1

    print(f"\n\n{'='*60}")
    print(f"  Results")
    print(f"{'='*60}")
    print(f"  Total requests    : {n_requests}")
    print(f"  Successful        : {len(ok_results)}  ({100*len(ok_results)/n_requests:.1f}%)")
    print(f"  Failed            : {len(fail_results)}  ({100*len(fail_results)/n_requests:.1f}%)")
    print(f"  Total time        : {elapsed_total:.1f}s")
    print(f"  Throughput        : {len(ok_results)/elapsed_total:.1f} req/s")
    print()
    if latencies:
        print(f"  Latency (ms)")
        print(f"    avg            : {avg_lat:.0f}ms")
        print(f"    p50            : {pct(latencies,50):.0f}ms")
        print(f"    p95            : {pct(latencies,95):.0f}ms")
        print(f"    p99            : {pct(latencies,99):.0f}ms")
        print(f"    max            : {max(latencies):.0f}ms")
        print(f"    min            : {min(latencies):.0f}ms")
    if errors:
        print(f"\n  Errors:")
        for err, cnt in sorted(errors.items(), key=lambda x: -x[1]):
            print(f"    {err}: {cnt}")
    print(f"{'='*60}\n")

    # Final verdict
    success_rate = len(ok_results) / n_requests * 100
    target_latency = 200
    p95 = pct(latencies, 95)
    if success_rate >= 99 and p95 <= target_latency:
        print(f"  ✅ PASS — {success_rate:.1f}% success, p95={p95:.0f}ms ≤ {target_latency}ms\n")
    elif success_rate >= 95:
        print(f"  ⚠️  WARN — {success_rate:.1f}% success, p95={p95:.0f}ms\n")
    else:
        print(f"  ❌ FAIL — {success_rate:.1f}% success rate below 95% threshold\n")


def main():
    parser = argparse.ArgumentParser(description="CloudOS-RL Load Test")
    parser.add_argument("--url",         default="http://localhost:8001")
    parser.add_argument("--requests",    default=100, type=int)
    parser.add_argument("--concurrency", default=10,  type=int)
    args = parser.parse_args()
    run_load_test(args.url, args.requests, args.concurrency)


if __name__ == "__main__":
    main()