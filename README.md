## DCheck Enterprise Runner (Baseline v1)

A minimal batch runner for DCheck Core.

### Purpose:
Execute one or more DCheck check(...) calls from a YAML specification and collect the results into a single, machine-readable JSON file.

This runner is intentionally minimal.

## Scope (What this runner does)

* Reads one YAML file with a fixed schema
* Translates each entry into a DCheck Core check(...) call
* Forces render=False for all checks
* Collects all results into one JSON output
* Returns a single exit code (0 or 1)

## Out of scope (By design)

* No CLI flags
* No resume / state files
* No per-table output files
* No SparkSession management
* No audit metadata
* No redaction
* No parallel execution

## Core Dependency

This runner is a thin wrapper around DCheck Core and relies on the frozen core contract:
```python
check(
  source,
  table_name=None,
  render=True,
  cache=False,
  modules=None,
  config=None
)
```
* render is always forced to False by the runner
* Core guarantees output shape, not semantics

## Input Contract (YAML v1)
### File format
```yml
version: 1

run:
  id: "run-001"          # optional, default: "run"
  output_dir: "./out"    # optional, default: "./out"
  fail_on: ["error"]     # optional, default: ["error"]
  stop_on_failure: false # optional, default: false

checks:
  - source: "sales.orders"      # REQUIRED
    table_name: "sales.orders"  # optional
    cache: false                # optional, default false
    modules: ["core_quality"]   # optional
    config: {}                  # optional

  - source: "finance.transactions"
```
## Validation rules

* version must be 1
* checks must be a list with at least one entry
* Each check must have source (string)
* modules must be list[str] or comma-separated string
* config must be an object/dict if present
* render is not allowed in the spec
* Invalid specs fail fast.

## Transform (What the runner does)

For each entry in checks, the runner calls:
```python
check(
  source=source,
  table_name=table_name,
  render=False,     # forced
  cache=cache,
  modules=modules,
  config=config
)
```
Each call is executed sequentially.

Exceptions are caught per check and recorded in output.

## Output Contract
### Output location
```
<output_dir>/run.json
```
### Output schema (v1)
```json
{
  "schema": "dcheck-enterprise-runner/v1",
  "run": {
    "id": "run-001",
    "started_utc": "2026-01-30T08:12:03Z",
    "finished_utc": "2026-01-30T08:13:10Z",
    "fail_on": ["error"],
    "stop_on_failure": false,
    "output_dir": "./out"
  },
  "checks": [
    {
      "check_id": "1",
      "input": {
        "source": "sales.orders",
        "table_name": "sales.orders",
        "render": false,
        "cache": false,
        "modules": ["core_quality"],
        "config": {}
      },
      "duration_ms": 1234,
      "failed": false,
      "report": {
        "rows": 1234567,
        "columns": 12,
        "column_names": ["order_id", "..."],
        "results": [
          {
            "name": "duplicate_rows",
            "status": "ok",
            "metrics": {},
            "message": null
          }
        ],
        "summary": {
          "status_counts": {
            "ok": 1,
            "warning": 0,
            "error": 0,
            "fail": 0
          }
        }
      },
      "exception": null
    }
  ],
  "summary": {
    "checks_total": 2,
    "checks_executed": 2,
    "checks_failed": 0
  }
}
```

## Exit Code

* 0 — no check produced a status listed in fail_on
* 1 — at least one check failed or threw an exception

## Public API
```python
from dcheck_enterprise_runner import run_from_yaml

exit_code = run_from_yaml("spec.yml")
```
This is the only supported entry point.

## Design Principle

Enterprise Runner is a batch adapter, not a compute engine.
All data quality logic lives in DCheck Core.