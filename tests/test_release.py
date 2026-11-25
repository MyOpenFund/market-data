"""Release workflow guard and package version (PyPI refuses direct references)."""

from __future__ import annotations

import re
import shutil
import subprocess
import tomllib
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


def _guard(tmp_path: Path, text: str) -> subprocess.CompletedProcess[str]:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(text, encoding="utf-8")
    bash = shutil.which("bash")
    assert bash is not None
    return subprocess.run(
        [bash, str(GUARD), str(pyproject)], capture_output=True, text=True, check=False
    )


def test_release_workflow_runs_the_guard_before_building():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "scripts/check-no-direct-references.sh" in text
    assert text.index("check-no-direct-references.sh") < text.index("python -m build")
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
    assert market_data.__version__ == "0.1.0"
    assert re.fullmatch(r"\d+\.\d+\.\d+", market_data.__version__)
