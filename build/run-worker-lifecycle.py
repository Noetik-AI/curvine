#!/usr/bin/env python3
"""Build and run isolated local worker-lifecycle and report regressions."""

import argparse
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="new directory for logs/configuration")
    parser.add_argument("--case", help="run only one process integration test")
    parser.add_argument("--timeout", type=int, default=180, help="seconds per test process")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = (args.output or root / "testing" / f"worker-lifecycle-{time.time_ns()}").resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    env["CURVINE_LIFECYCLE_ARTIFACTS"] = str(output)
    results = []
    print(f"Evidence: {output}", flush=True)

    def run(name, command, timeout):
        started = time.monotonic()
        log = output / f"{name}.log"
        print(f"{name}: starting", flush=True)
        with log.open("w") as handle:
            process = subprocess.Popen(command, cwd=root, env=env, stdout=handle,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            try:
                code = process.wait(timeout=timeout)
            except BaseException:
                # Every server child inherits this test's process group. Stop the
                # group even if the test itself hangs before its RAII cleanup.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
                results.append({"name": name, "exit": "timeout/interrupted"})
                (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
                raise
        result = {"name": name, "exit": code, "seconds": round(time.monotonic() - started, 2)}
        if not name.startswith(("build-", "list-")):
            passed = re.search(r"test result: ok\. (\d+) passed", log.read_text())
            result["passed"] = int(passed[1]) if passed else 0
            if code == 0 and not result["passed"]:
                code = result["exit"] = "no tests passed"
        results.append(result)
        (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        print(json.dumps(result), flush=True)
        if code:
            diagnostics = []
            for line in log.read_text().splitlines():
                try:
                    item = json.loads(line)
                except ValueError:
                    diagnostics.append(line)
                    continue
                if isinstance(item, dict) and item.get("reason") == "compiler-message":
                    diagnostics.append(item["message"]["rendered"])
            print("\n".join(diagnostics)[-12000:], file=sys.stderr)
            raise RuntimeError(f"{name} failed; retained logs: {output}")
        return log.read_text()

    artifacts = {}
    for package, targets in [
        ("curvine-server", ["--test", "worker_lifecycle_test", "--test", "block_report_membership_test"]),
        ("curvine-master", ["--lib"]),
        ("curvine-config", ["--lib"]),
    ]:
        body = run(f"build-{package}", [
            "cargo", "test", "--locked", "--offline", "--release", "-p", package,
            *(["--features", "fault-injection"] if package != "curvine-config" else []),
            *targets, "--no-run", "--message-format=json",
        ], 1800)
        for line in body.splitlines():
            try:
                item = json.loads(line)
            except ValueError:
                continue
            if item.get("reason") == "compiler-artifact" and item.get("executable"):
                artifacts[item["target"]["name"]] = item["executable"]

    binary = artifacts["worker_lifecycle_test"]
    listing = run("list-lifecycle", [binary, "--ignored", "--list"], 10)
    cases = [line.removesuffix(": test") for line in listing.splitlines() if line.endswith(": test")]
    if not cases:
        raise RuntimeError("no lifecycle tests discovered")
    if args.case:
        if args.case not in cases:
            raise ValueError(f"unknown test: {args.case}; choices: {cases}")
        cases = [args.case]
    for case in cases:
        run(case, [binary, "--ignored", "--exact", case, "--nocapture", "--test-threads=1"], args.timeout)
    if not args.case:
        run("membership", [artifacts["block_report_membership_test"], "--nocapture", "--test-threads=1"], args.timeout)
        for label, test_filter in [
            ("report-state", "report_state_tests::"),
            ("metadata", "master::meta::fs_dir::"),
            ("scan", "reconcile_scan_tests::"),
            ("retention", "worker_retention::tests::"),
            ("worker-manager", "worker_manager::tests::"),
        ]:
            run(label, [artifacts["curvine_master"], test_filter, "--nocapture", "--test-threads=1"], args.timeout)
        run("retention-config", [artifacts["curvine_config"], "departure_retention_tests::", "--nocapture"], args.timeout)
    print(f"All selected checks passed. Evidence: {output}", flush=True)


if __name__ == "__main__":
    main()
