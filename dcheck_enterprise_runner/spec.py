from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class RunSpec:
    id: str = "run"
    output_dir: str = "./out"
    fail_on: list[str] = None  # default set in normalize
    stop_on_failure: bool = False


@dataclass(frozen=True)
class CheckSpec:
    source: str
    table_name: str | None = None
    cache: bool = False
    modules: list[str] | None = None
    config: dict[str, Any] | None = None


@dataclass(frozen=True)
class EnterpriseSpec:
    version: int
    run: RunSpec
    checks: list[CheckSpec]


class SpecError(ValueError):
    pass


_ALLOWED_STATUSES = {"ok", "warning", "error", "fail"}


def load_spec(path: str | Path) -> EnterpriseSpec:
    p = Path(path)
    if not p.exists() or not p.is_file():
        raise SpecError(f"Spec file not found: {p}")

    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SpecError("YAML root must be a mapping/object")

    version = data.get("version")
    if version != 1:
        raise SpecError("version must be 1")

    run_raw = data.get("run", {}) or {}
    if not isinstance(run_raw, dict):
        raise SpecError("run must be an object")

    fail_on_raw = run_raw.get("fail_on", ["error"])
    if isinstance(fail_on_raw, str):
        fail_on = [s.strip() for s in fail_on_raw.split(",") if s.strip()]
    elif isinstance(fail_on_raw, list) and all(isinstance(x, str) for x in fail_on_raw):
        fail_on = fail_on_raw
    else:
        raise SpecError("run.fail_on must be list[str] or comma-separated string")

    unknown = [s for s in fail_on if s not in _ALLOWED_STATUSES]
    if unknown:
        raise SpecError(f"run.fail_on contains unknown statuses: {unknown} (allowed={sorted(_ALLOWED_STATUSES)})")

    run = RunSpec(
        id=str(run_raw.get("id", "run")),
        output_dir=str(run_raw.get("output_dir", "./out")),
        fail_on=fail_on,
        stop_on_failure=bool(run_raw.get("stop_on_failure", False)),
    )

    checks_raw = data.get("checks")
    if not isinstance(checks_raw, list) or len(checks_raw) < 1:
        raise SpecError("checks must be a list with at least 1 item")

    checks: list[CheckSpec] = []
    for i, item in enumerate(checks_raw):
        if not isinstance(item, dict):
            raise SpecError(f"checks[{i}] must be an object")

        source = item.get("source")
        if not isinstance(source, str) or not source.strip():
            raise SpecError(f"checks[{i}].source must be a non-empty string")

        table_name = item.get("table_name", None)
        if table_name is not None and not isinstance(table_name, str):
            raise SpecError(f"checks[{i}].table_name must be string or null")

        cache = bool(item.get("cache", False))

        modules_raw = item.get("modules", None)
        modules: list[str] | None
        if modules_raw is None:
            modules = None
        elif isinstance(modules_raw, str):
            modules = [s.strip() for s in modules_raw.split(",") if s.strip()]
        elif isinstance(modules_raw, list) and all(isinstance(x, str) for x in modules_raw):
            modules = modules_raw
        else:
            raise SpecError(f"checks[{i}].modules must be list[str], string, or null")

        config_raw = item.get("config", None)
        if config_raw is None:
            config = None
        elif isinstance(config_raw, dict):
            config = config_raw
        else:
            raise SpecError(f"checks[{i}].config must be object/dict or null")

        # IMPORTANT: render is NOT allowed in spec (locked False in runner)
        if "render" in item:
            raise SpecError(f"checks[{i}] must not specify render (runner locks render=False)")

        checks.append(
            CheckSpec(
                source=source.strip(),
                table_name=table_name.strip() if isinstance(table_name, str) else None,
                cache=cache,
                modules=modules,
                config=config,
            )
        )

    return EnterpriseSpec(version=version, run=run, checks=checks)