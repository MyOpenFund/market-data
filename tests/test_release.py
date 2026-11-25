"""Release workflow guard and package version (PyPI refuses direct references)."""

from __future__ import annotations

import re
import shutil
import subprocess
import tomllib
import zipfile
from pathlib import Path

import pytest

import market_data

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"
GUARD = ROOT / "scripts" / "check-no-direct-references.sh"

CLEAN = """[project]
name = "x"
dependencies = ["myopenfund-open-data-store[writer,recipes]>=0.2.0", "httpx>=0.27"]
[project.urls]
Homepage = "https://github.com/MyOpenFund/market-data"
"""


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    bash = shutil.which("bash")
    assert bash is not None
    return subprocess.run([bash, str(GUARD), *args], capture_output=True, text=True, check=False)


def _guard(tmp_path: Path, text: str) -> subprocess.CompletedProcess[str]:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(text, encoding="utf-8")
    return _run(str(pyproject))


def _wheel(tmp_path: Path, requires: list[str]) -> Path:
    wheel = tmp_path / "x-1.0.0-py3-none-any.whl"
    metadata = "Metadata-Version: 2.4\nName: x\nVersion: 1.0.0\n" + "".join(
        f"Requires-Dist: {r}\n" for r in requires
    )
    with zipfile.ZipFile(wheel, "w") as z:
        z.writestr("x/__init__.py", "")
        z.writestr("x-1.0.0.dist-info/METADATA", metadata)
    return wheel


def test_release_workflow_runs_the_guard_before_building():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "scripts/check-no-direct-references.sh" in text
    assert text.index("check-no-direct-references.sh") < text.index("python -m build")
    assert text.index("python -m build") < text.index("check-no-direct-references.sh --wheel")
    assert "environment: ${{" in text and "'pypi'" in text
    assert "id-token: write" in text
    assert "twine check --strict" in text
    assert 'tags: ["v*"]' in text


def test_guard_passes_a_clean_pyproject(tmp_path):
    assert _guard(tmp_path, CLEAN).returncode == 0


@pytest.mark.parametrize(
    "line",
    [
        'dependencies = ["pkg @ git+https://github.com/o/r@abc"]',
        'dependencies = ["pkg@git+https://github.com/o/r"]',
        'dependencies = ["pkg @ https://example.org/pkg.whl"]',
        'dependencies = ["pkg @ file:///tmp/pkg"]',
        "[tool.hatch.metadata]\nallow-direct-references = true",
    ],
)
def test_guard_fails_on_a_direct_reference(tmp_path, line):
    result = _guard(tmp_path, CLEAN + line + "\n")
    assert result.returncode == 1
    assert "PyPI" in result.stdout


def test_version_is_read_from_the_package():
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert "version" in pyproject["project"]["dynamic"]
    assert "version" not in pyproject["project"]
    assert pyproject["tool"]["hatch"]["version"]["path"] == "src/market_data/__init__.py"
    assert re.fullmatch(r"\d+\.\d+\.\d+", market_data.__version__)


def test_wheel_guard_passes_plain_requirements(tmp_path):
    wheel = _wheel(tmp_path, ["httpx>=0.27", "pkg[extra]>=1; python_version >= '3.11'"])
    assert _run("--wheel", str(wheel)).returncode == 0


def test_wheel_guard_fails_on_a_direct_reference_in_requires_dist(tmp_path):
    wheel = _wheel(tmp_path, ["httpx>=0.27", "pkg[extra] @ git+https://github.com/o/r@abc"])
    result = _run("--wheel", str(wheel))
    assert result.returncode == 1
    assert "PyPI" in result.stdout


def test_wheel_guard_refuses_a_missing_wheel(tmp_path):
    assert _run("--wheel", str(tmp_path / "none.whl")).returncode == 2
