"""Run Bandit using config from pyproject.toml.

Bandit doesn't natively read pyproject.toml, so we centralize configuration in
[tool.bandit] and use this wrapper from CI and pre-commit.
"""

from __future__ import annotations

import os
import subprocess
import sys
from typing import Any


def _load_pyproject(path: str) -> dict[str, Any]:
    if sys.version_info >= (3, 11):
        import tomllib  # type: ignore[attr-defined]

        with open(path, "rb") as f:
            return tomllib.load(f)

    import tomli  # type: ignore[import-not-found]

    with open(path, "rb") as f:
        return tomli.load(f)


def main(argv: list[str]) -> int:
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    pyproject_path = os.path.join(repo_root, "pyproject.toml")
    config = _load_pyproject(pyproject_path)

    tool_cfg = config.get("tool", {})
    bandit_cfg = tool_cfg.get("bandit", {})

    severity_level = bandit_cfg.get("severity_level", "medium")
    confidence_level = bandit_cfg.get("confidence_level", "medium")
    exclude_list = bandit_cfg.get("exclude", [])

    if isinstance(exclude_list, list):
        exclude = ",".join(str(x) for x in exclude_list)
    else:
        exclude = str(exclude_list)

    # Allow callers to pass additional args (e.g. --format json), but keep the
    # core behavior stable and centralized.
    extra_args = argv[1:]

    cmd = [
        sys.executable,
        "-m",
        "bandit",
        "-r",
        repo_root,
        "--exclude",
        exclude,
        "--severity-level",
        str(severity_level),
        "--confidence-level",
        str(confidence_level),
        *extra_args,
    ]

    return subprocess.run(cmd, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

