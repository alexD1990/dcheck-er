import json
import textwrap
from types import SimpleNamespace

import dcheck_enterprise_runner.runner as runner_mod


def write(tmp_path, name, content):
    p = tmp_path / name
    p.write_text(textwrap.dedent(content), encoding="utf-8")
    return str(p)


def test_run_writes_run_json_and_locks_render(tmp_path):
    # Stub core ValidationReport shape
    fake_report = SimpleNamespace(
        rows=10,
        columns=2,
        column_names=["a", "b"],
        results=[
            SimpleNamespace(name="r1", status="ok", metrics={}, message=None),
        ],
    )

    def fake_check_fn(**kwargs):
        # Contract: runner MUST force render=False
        assert kwargs["render"] is False
        return fake_report

    out_dir = tmp_path / "out"
    spec_path = write(
        tmp_path,
        "spec.yml",
        f"""
        version: 1
        run:
          id: "t1"
          output_dir: "{out_dir.as_posix()}"
          fail_on: ["error"]
        checks:
          - source: "sales.orders"
            modules: ["core_quality"]
            config: {{}}
        """,
    )

    rc = runner_mod.run_from_yaml(spec_path, check_fn=fake_check_fn)
    assert rc == 0

    run_json_path = out_dir / "run.json"
    assert run_json_path.exists()

    data = json.loads(run_json_path.read_text(encoding="utf-8"))
    assert data["schema"] == "dcheck-enterprise-runner/v1"
    assert "run" in data and "checks" in data and "summary" in data
    assert data["run"]["id"] == "t1"

    c0 = data["checks"][0]
    assert c0["input"]["render"] is False
    assert c0["failed"] is False
    assert c0["exception"] is None

    report = c0["report"]
    assert report["rows"] == 10
    assert report["columns"] == 2
    assert report["column_names"] == ["a", "b"]
    assert isinstance(report["results"], list)
    assert "summary" in report and "status_counts" in report["summary"]


def test_fail_on_triggers_exit_1(tmp_path):
    fake_report = SimpleNamespace(
        rows=1,
        columns=1,
        column_names=["a"],
        results=[SimpleNamespace(name="r1", status="error", metrics={}, message="boom")],
    )

    def fake_check_fn(**kwargs):
        return fake_report

    out_dir = tmp_path / "out"
    spec_path = write(
        tmp_path,
        "spec.yml",
        f"""
        version: 1
        run:
          output_dir: "{out_dir.as_posix()}"
          fail_on: ["error"]
        checks:
          - source: "x"
        """,
    )

    rc = runner_mod.run_from_yaml(spec_path, check_fn=fake_check_fn)
    assert rc == 1


def test_exception_is_recorded_and_exit_1(tmp_path):
    def boom(**kwargs):
        raise RuntimeError("no spark")

    out_dir = tmp_path / "out"
    spec_path = write(
        tmp_path,
        "spec.yml",
        f"""
        version: 1
        run:
          output_dir: "{out_dir.as_posix()}"
        checks:
          - source: "x"
        """,
    )

    rc = runner_mod.run_from_yaml(spec_path, check_fn=boom)
    assert rc == 1

    data = json.loads((out_dir / "run.json").read_text(encoding="utf-8"))
    c0 = data["checks"][0]
    assert c0["failed"] is True
    assert c0["report"] is None
    assert isinstance(c0["exception"], str) and "RuntimeError" in c0["exception"]

def test_runner_sends_core_input_contract(tmp_path):
    from types import SimpleNamespace
    import textwrap

    def write(tmp_path, name, content):
        p = tmp_path / name
        p.write_text(textwrap.dedent(content), encoding="utf-8")
        return str(p)

    out_dir = tmp_path / "out"
    spec_path = write(
        tmp_path,
        "spec.yml",
        f"""
        version: 1
        run:
          output_dir: "{out_dir.as_posix()}"
        checks:
          - source: "sales.orders"
            table_name: "sales.orders"
            cache: true
            modules: ["core_quality"]
            config: {{"threshold": 0.1}}
        """,
    )

    def fake_check_fn(**kwargs):
        assert kwargs["source"] == "sales.orders"
        assert kwargs["table_name"] == "sales.orders"
        assert kwargs["render"] is False
        assert kwargs["cache"] is True
        assert kwargs["modules"] == ["core_quality"]
        assert kwargs["config"] == {"threshold": 0.1}

        return SimpleNamespace(rows=0, columns=0, column_names=[], results=[])

    rc = runner_mod.run_from_yaml(spec_path, check_fn=fake_check_fn)
    assert rc == 0
