from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dcheck_enterprise_runner.io import ensure_dir, write_json
from dcheck_enterprise_runner.serializer import report_to_dict
from dcheck_enterprise_runner.spec import load_spec, EnterpriseSpec

_SCHEMA = "dcheck-enterprise-runner/v1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _should_fail(status_counts: dict[str, int], fail_on: list[str]) -> bool:
    return any(status_counts.get(s, 0) > 0 for s in fail_on)


def run_from_yaml(spec_path: str | Path, check_fn=None) -> int:
    """
    Baseline enterprise runner v1.

    Contract:
    - Reads YAML spec
    - Executes DCheck core check(...) sequentially
    - Forces render=False
    - Writes <output_dir>/run.json
    - Returns exit code 0 or 1
    """
    if check_fn is None:
        from dcheck import check as check_fn  

    spec: EnterpriseSpec = load_spec(spec_path)
    out_dir = ensure_dir(spec.run.output_dir)

    started = _utc_now()

    checks_out: list[dict[str, Any]] = []
    failed_total = 0

    for idx, job in enumerate(spec.checks, start=1):
        t0 = time.time()
        exception_str: str | None = None
        report_dict: dict[str, Any] | None = None

        try:
            report = check_fn(
                source=job.source,
                table_name=job.table_name,
                render=False,          # LOCKED
                cache=job.cache,
                modules=job.modules,
                config=job.config,
            )
            report_dict = report_to_dict(report)
            failed = _should_fail(
                report_dict["summary"]["status_counts"],
                spec.run.fail_on,
            )
        except Exception as e:
            failed = True
            exception_str = f"{type(e).__name__}: {e}"

        duration_ms = int((time.time() - t0) * 1000)
        if failed:
            failed_total += 1

        checks_out.append(
            {
                "check_id": str(idx),
                "input": {
                    "source": job.source,
                    "table_name": job.table_name,
                    "render": False,
                    "cache": job.cache,
                    "modules": job.modules,
                    "config": job.config,
                },
                "duration_ms": duration_ms,
                "failed": failed,
                "report": report_dict,
                "exception": exception_str,
            }
        )

        if failed and spec.run.stop_on_failure:
            break

    finished = _utc_now()

    payload = {
        "schema": _SCHEMA,
        "run": {
            "id": spec.run.id,
            "started_utc": started,
            "finished_utc": finished,
            "fail_on": spec.run.fail_on,
            "stop_on_failure": spec.run.stop_on_failure,
            "output_dir": str(out_dir),
        },
        "checks": checks_out,
        "summary": {
            "checks_total": len(spec.checks),
            "checks_executed": len(checks_out),
            "checks_failed": failed_total,
        },
    }

    write_json(Path(out_dir) / "run.json", payload)
    return 1 if failed_total > 0 else 0
