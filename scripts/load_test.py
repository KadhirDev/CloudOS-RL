"""
CloudOS-RL Load Test
=====================
Sends N concurrent authenticated schedule requests and reports latency statistics.

Usage:
  python scripts/load_test.py
  python scripts/load_test.py --requests 200 --concurrency 10 --url http://localhost:8001
  python scripts/load_test.py --requests 500 --concurrency 20
  python scripts/load_test.py --username engineer --password eng123

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
from typing import Dict, List, Optional
from urllib import error, request

BASE_PAYLOADS = [
    {"workload_type": "training",  "cpu_request_vcpu": 4, "memory_request_gb": 8,  "is_spot_tolerant": True},
    {"workload_type": "inference", "cpu_request_vcpu": 2, "memory_request_gb": 4,  "is_spot_tolerant": False},
    {"workload_type": "batch",     "cpu_request_vcpu": 8, "memory_request_gb": 16, "is_spot_tolerant": True},
    {"workload_type": "streaming", "cpu_request_vcpu": 1, "memory_request_gb": 2,  "is_spot_tolerant": False},
]


def _post_json(url: str, payload: dict, headers: Optional[Dict[str, str]] = None, timeout: int = 15) -> dict:
    body = json.dumps(payload).encode("utf-8")
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)

    req = request.Request(
        url,
        data=body,
        headers=req_headers,
        method="POST",
    )

    with request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))


def _login(base_url: str, username: str, password: str) -> str:
    """
    Authenticates once and returns JWT access token.
    Raises RuntimeError with a clean message on failure.
    """
    try:
        data = _post_json(
            f"{base_url}/auth/login",
            {"username": username, "password": password},
            timeout=15,
        )
    except error.HTTPError as exc:
        try:
            detail = exc.read().decode("utf-8", errors="replace")
        except Exception:
            detail = ""
        raise RuntimeError(f"login failed: HTTP {exc.code} {detail}".strip()) from exc
    except Exception as exc:
        raise RuntimeError(f"login failed: {exc}") from exc

    token = data.get("access_token")
    if not token:
        raise RuntimeError("login failed: access_token missing in response")
    return token


def _send_one(url: str, payload: dict, headers: Dict[str, str]) -> dict:
    """Sends a single authenticated schedule request. Returns result dict."""
    t0 = time.perf_counter()
    try:
        data = _post_json(
            f"{url}/api/v1/schedule",
            payload,
            headers=headers,
            timeout=30,
        )
        elapsed = (time.perf_counter() - t0) * 1000
        return {
            "ok": True,
            "latency": elapsed,
            "cloud": data.get("cloud", "?"),
            "status": 200,
        }
    except error.HTTPError as exc:
        elapsed = (time.perf_counter() - t0) * 1000
        return {
            "ok": False,
            "latency": elapsed,
            "error": f"HTTP {exc.code}",
            "status": exc.code,
        }
    except Exception as exc:
        elapsed = (time.perf_counter() - t0) * 1000
        return {
            "ok": False,
            "latency": elapsed,
            "error": str(exc)[:80],
            "status": 0,
        }


def run_load_test(url: str, n_requests: int, concurrency: int, username: str, password: str) -> None:
    results: List[dict] = []
    lock = threading.Lock()
    counter = [0]

    print(f"\n{'=' * 60}")
    print("  CloudOS-RL Load Test")
    print(f"{'=' * 60}")
    print(f"  Target      : {url}")
    print(f"  Requests    : {n_requests}")
    print(f"  Concurrency : {concurrency}")
    print(f"  User        : {username}")
    print(f"{'=' * 60}\n")

    # Authenticate once
    print("  Authenticating...")
    try:
        token = _login(url, username, password)
    except RuntimeError as exc:
        print(f"  ❌ Authentication failed: {exc}")
        return

    auth_headers = {
        "Authorization": f"Bearer {token}",
    }
    print("  Authentication OK\n")

    def worker(idx: int):
        payload = BASE_PAYLOADS[idx % len(BASE_PAYLOADS)]
        result = _send_one(url, payload, auth_headers)
        with lock:
            results.append(result)
            counter[0] += 1
            done = counter[0]
        if done % 50 == 0 or done == n_requests:
            print(f"  Progress: {done}/{n_requests}", end="\r", flush=True)

    # Warm up
    print("  Warming up...")
    warm = _send_one(url, BASE_PAYLOADS[0], auth_headers)
    if not warm["ok"]:
        print(f"  ❌ Warm-up failed: {warm.get('error', '?')}. Is the API running and auth valid?")
        return
    print(f"  Warm-up OK — latency: {warm['latency']:.0f}ms\n")
    print("  Running load test...")

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

    elapsed_total = time.perf_counter() - t_start

    ok_results = [r for r in results if r["ok"]]
    fail_results = [r for r in results if not r["ok"]]
    latencies = sorted(r["latency"] for r in ok_results)

    def pct(lst: List[float], p: int) -> float:
        if not lst:
            return 0.0
        idx = max(0, int(len(lst) * p / 100) - 1)
        return lst[idx]

    avg_lat = statistics.mean(latencies) if latencies else 0.0
    errors: Dict[str, int] = {}
    for r in fail_results:
        key = r.get("error", "unknown")
        errors[key] = errors.get(key, 0) + 1

    print(f"\n\n{'=' * 60}")
    print("  Results")
    print(f"{'=' * 60}")
    print(f"  Total requests    : {n_requests}")
    print(f"  Successful        : {len(ok_results)}  ({100 * len(ok_results) / n_requests:.1f}%)")
    print(f"  Failed            : {len(fail_results)}  ({100 * len(fail_results) / n_requests:.1f}%)")
    print(f"  Total time        : {elapsed_total:.1f}s")
    print(f"  Throughput        : {len(ok_results) / elapsed_total:.1f} req/s")
    print()

    if latencies:
        print("  Latency (ms)")
        print(f"    avg            : {avg_lat:.0f}ms")
        print(f"    p50            : {pct(latencies, 50):.0f}ms")
        print(f"    p95            : {pct(latencies, 95):.0f}ms")
        print(f"    p99            : {pct(latencies, 99):.0f}ms")
        print(f"    max            : {max(latencies):.0f}ms")
        print(f"    min            : {min(latencies):.0f}ms")

    if errors:
        print("\n  Errors:")
        for err_msg, cnt in sorted(errors.items(), key=lambda x: -x[1]):
            print(f"    {err_msg}: {cnt}")

    print(f"{'=' * 60}\n")

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
    parser.add_argument("--url", default="http://localhost:8001")
    parser.add_argument("--requests", default=100, type=int)
    parser.add_argument("--concurrency", default=10, type=int)
    parser.add_argument("--username", default="engineer")
    parser.add_argument("--password", default="eng123")
    args = parser.parse_args()

    run_load_test(
        url=args.url,
        n_requests=args.requests,
        concurrency=args.concurrency,
        username=args.username,
        password=args.password,
    )


if __name__ == "__main__":
    main()