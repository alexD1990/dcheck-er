from __future__ import annotations

from typing import Any


_ALLOWED = ("ok", "warning", "error", "fail")


def _status_counts(results: list[dict[str, Any]]) -> dict[str, int]:
    counts = {k: 0 for k in _ALLOWED}
    for r in results:
        s = r.get("status")
        if s in counts:
            counts[s] += 1
    return counts


def report_to_dict(report: Any) -> dict[str, Any]:
    """
    Serializes DCheck core ValidationReport (shape-stable contract) into JSON-safe dict.
    We only rely on attributes described in the core contract.
    """
    results_out: list[dict[str, Any]] = []
    for rr in getattr(report, "results", []) or []:
        results_out.append(
            {
                "name": getattr(rr, "name", None),
                "status": getattr(rr, "status", None),
                "metrics": getattr(rr, "metrics", {}) or {},
                "message": getattr(rr, "message", None),
            }
        )

    return {
        "rows": getattr(report, "rows", None),
        "columns": getattr(report, "columns", None),
        "column_names": getattr(report, "column_names", None),
        "results": results_out,
        "summary": {"status_counts": _status_counts(results_out)},
    }
