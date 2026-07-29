#!/usr/bin/env python3
"""Run a read-only real-provider canary through the dispatcher."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from custom_provider_mcp import Dispatcher


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cwd", required=True)
    parser.add_argument("--profile", default="minimax")
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()

    dispatcher = Dispatcher()
    started = dispatcher.start(
        {
            "delegation_id": "custom-provider-orchestrator-real-canary",
            "need": "Return the single token CANARY_OK after the required receipt line.",
            "boundaries": "Read-only. Do not call tools, modify files, or access credentials.",
            "deliverable": "The required receipt line followed by CANARY_OK.",
            "cwd": str(Path(args.cwd).resolve()),
            "profile": args.profile,
            "sandbox": "read-only",
            "timeout_seconds": args.timeout,
        }
    )
    while True:
        snapshot = dispatcher.wait(
            {"job_id": started["job_id"], "timeout_seconds": 20}
        )
        if snapshot["status"] not in {"starting", "running", "cancelling"}:
            break

    report = {
        "job_id": snapshot["job_id"],
        "status": snapshot["status"],
        "profile": snapshot["profile"],
        "exit_code": snapshot["exit_code"],
        "thread_id": snapshot["thread_id"],
        "receipt_verified": snapshot["receipt_verified"],
        "canary_ok": "CANARY_OK" in (snapshot["result"] or ""),
        "stderr_excerpt": snapshot["stderr_excerpt"],
    }
    initial_ok = (
        report["status"] == "completed"
        and report["receipt_verified"]
        and report["canary_ok"]
    )
    if initial_ok:
        followup_started = dispatcher.followup(
            {
                "job_id": snapshot["job_id"],
                "delegation_id": "custom-provider-orchestrator-followup-canary",
                "need": "Return the single token FOLLOWUP_OK after the required receipt line.",
                "boundaries": "Read-only. Do not call tools, modify files, or access credentials.",
                "deliverable": "The required receipt line followed by FOLLOWUP_OK.",
                "timeout_seconds": args.timeout,
            }
        )
        while True:
            followup = dispatcher.wait(
                {"job_id": followup_started["job_id"], "timeout_seconds": 20}
            )
            if followup["status"] not in {"starting", "running", "cancelling"}:
                break
        report["followup"] = {
            "job_id": followup["job_id"],
            "status": followup["status"],
            "exit_code": followup["exit_code"],
            "thread_id": followup["thread_id"],
            "receipt_verified": followup["receipt_verified"],
            "canary_ok": "FOLLOWUP_OK" in (followup["result"] or ""),
            "stderr_excerpt": followup["stderr_excerpt"],
        }
    else:
        report["followup"] = None
    print(json.dumps(report, ensure_ascii=False, indent=2))
    followup_report = report["followup"] or {}
    return 0 if (
        initial_ok
        and followup_report.get("status") == "completed"
        and followup_report.get("receipt_verified")
        and followup_report.get("canary_ok")
    ) else 1


if __name__ == "__main__":
    sys.exit(main())
