#!/usr/bin/env python3
"""Orchestrate all 16 consistency model experiments (4 models × 4 configs).

Enforces clean cluster state before each test and waits for stability after.
Parses verdicts and outputs structured results.

Usage:
    uv run run_experiments.py              # Run all 16 tests
    uv run run_experiments.py --model mr   # Run one model (substring match)
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
from pathlib import Path

from helpers import ClusterNotReady, pre_flight_check, stabilize_after_test

MODELS = [
    "read_your_writes",
    "monotonic_reads",
    "monotonic_writes",
    "writes_follow_reads",
]
CONFIGS = [
    "majority/majority",
    "majority/w:1",
    "local/w:1",
    "local/majority",
]


def run_test(model: str, config: str, timeout_seconds: int = 300) -> dict:
    """Run one test, parse verdict from output.

    Returns dict with config, verdict, output, and error (if any).
    """
    try:
        pre_flight_check()
    except ClusterNotReady as e:
        return {"config": config, "verdict": "SKIP", "error": str(e), "output": ""}

    try:
        result = subprocess.run(
            ["uv", "run", f"models/{model}.py", "--config", config],
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )

        # Parse verdict from output (look for "verdict: VIOLATED" etc)
        match = re.search(r"verdict:\s+(\w+)", result.stdout)
        verdict = match.group(1) if match else "ERROR"

        if result.returncode != 0 and verdict == "ERROR":
            return {
                "config": config,
                "verdict": "ERROR",
                "error": result.stderr[:200],
                "output": result.stdout,
            }

        stabilize_after_test()

        return {
            "config": config,
            "verdict": verdict,
            "output": result.stdout,
            "error": None,
        }

    except subprocess.TimeoutExpired:
        return {"config": config, "verdict": "TIMEOUT", "error": "Test exceeded timeout", "output": ""}
    except Exception as e:
        return {
            "config": config,
            "verdict": "ERROR",
            "error": str(e),
            "output": "",
        }


def run_model(model: str) -> dict:
    """Run all 4 configs for one model, return results."""
    print(f"\n{'='*70}")
    print(f"  {model.upper()}")
    print(f"{'='*70}\n")

    results = []
    for config in CONFIGS:
        print(f"  {config:<20} ", end="", flush=True)
        start = time.time()
        r = run_test(model, config)
        elapsed = time.time() - start
        verdict = r["verdict"]
        print(f"{verdict:<15} ({elapsed:.1f}s)")

        if r.get("error"):
            print(f"    Error: {r['error'][:100]}")

        results.append(r)

    return {"model": model, "results": results}


def print_table(model_name: str, results: list) -> None:
    """Print results table for one model."""
    print(f"\n  {'Config':<20} {'Verdict':<15}")
    print(f"  {'-'*35}")
    for r in results:
        print(f"  {r['config']:<20} {r['verdict']:<15}")


def run_all(model_filter: str = None) -> None:
    """Run all or filtered models, print results tables."""
    models = [m for m in MODELS if not model_filter or model_filter.lower() in m.lower()]

    if not models:
        print(f"Error: no models match '{model_filter}'", file=sys.stderr)
        sys.exit(1)

    all_results = []
    for model in models:
        model_result = run_model(model)
        all_results.append(model_result)
        print_table(model, model_result["results"])

    print(f"\n{'='*70}")
    print("  SUMMARY (all 16 tests)")
    print(f"{'='*70}\n")
    print(f"  {'Model':<25} {'Config':<20} {'Verdict':<15}")
    print(f"  {'-'*60}")

    total = 0
    passed = 0
    for model_result in all_results:
        model = model_result["model"]
        for r in model_result["results"]:
            total += 1
            if r["verdict"] in ("VIOLATED", "NOT_VIOLATED"):
                passed += 1
            print(f"  {model:<25} {r['config']:<20} {r['verdict']:<15}")

    print(f"  {'-'*60}")
    print(f"  Total: {passed}/{total} completed\n")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Orchestrate all consistency model experiments"
    )
    ap.add_argument(
        "--model",
        help="Run one model only (substring match: 'read_your', 'monotonic_reads', 'monotonic_writes', 'writes_follow')",
    )
    ap.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Timeout per test in seconds (default 300)",
    )
    args = ap.parse_args()

    run_all(args.model)


if __name__ == "__main__":
    main()
