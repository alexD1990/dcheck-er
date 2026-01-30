import textwrap
import pytest
from dcheck_enterprise_runner.spec import load_spec, SpecError

def write(tmp_path, name, content):
    p = tmp_path / name
    p.write_text(textwrap.dedent(content), encoding="utf-8")
    return str(p)

def test_valid_spec_defaults(tmp_path):
    path = write(tmp_path, "spec.yml", """
    version: 1
    checks:
      - source: "sales.orders"
    """)
    spec = load_spec(path)

    assert spec.version == 1
    assert spec.run.id == "run"
    assert spec.run.output_dir == "./out"
    assert spec.run.fail_on == ["error"]
    assert spec.run.stop_on_failure is False

    assert len(spec.checks) == 1
    c0 = spec.checks[0]
    assert c0.source == "sales.orders"
    assert c0.table_name is None
    assert c0.cache is False
    assert c0.modules is None
    assert c0.config is None

def test_spec_rejects_wrong_version(tmp_path):
    path = write(tmp_path, "spec.yml", """
    version: 2
    checks:
      - source: "x"
    """)
    with pytest.raises(SpecError):
        load_spec(path)

def test_spec_requires_checks_list(tmp_path):
    path = write(tmp_path, "spec.yml", """
    version: 1
    """)
    with pytest.raises(SpecError):
        load_spec(path)

def test_spec_rejects_empty_checks(tmp_path):
    path = write(tmp_path, "spec.yml", """
    version: 1
    checks: []
    """)
    with pytest.raises(SpecError):
        load_spec(path)

def test_spec_requires_source(tmp_path):
    path = write(tmp_path, "spec.yml", """
    version: 1
    checks:
      - table_name: "x"
    """)
    with pytest.raises(SpecError):
        load_spec(path)

def test_spec_rejects_render_in_check(tmp_path):
    path = write(tmp_path, "spec.yml", """
    version: 1
    checks:
      - source: "x"
        render: true
    """)
    with pytest.raises(SpecError):
        load_spec(path)
