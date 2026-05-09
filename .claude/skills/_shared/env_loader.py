"""Read .env file. Fallback to .env.example template.

skill 的 .env 不进 git (.gitignore 已加)。
.env.example 是模板，进 git。
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def load_env(skill_dir: Path) -> dict[str, str]:
    """Load .env from skill_dir, fallback to .env.example.

    Returns dict of vars. Variables already in os.environ override file values.
    """
    env: dict[str, str] = {}
    env_file = skill_dir / ".env"
    example_file = skill_dir / ".env.example"

    source = env_file if env_file.exists() else example_file
    if not source.exists():
        return env

    for raw in source.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        env[key] = value

    # os.environ overrides
    for key in list(env.keys()):
        if key in os.environ:
            env[key] = os.environ[key]

    return env


def require(env: dict[str, str], *keys: str) -> None:
    """Exit with code 2 if any required env var is missing or has placeholder value."""
    missing: list[str] = []
    placeholder: list[str] = []
    for key in keys:
        v = env.get(key)
        if not v:
            missing.append(key)
        elif v.startswith("YOUR_") or v == "REPLACE_ME":
            placeholder.append(key)
    if missing or placeholder:
        if missing:
            print(f"[skill] missing env: {', '.join(missing)}", file=sys.stderr)
        if placeholder:
            print(
                f"[skill] env still has placeholder: {', '.join(placeholder)}",
                file=sys.stderr,
            )
        print("[skill] copy .env.example to .env and fill in real values", file=sys.stderr)
        sys.exit(2)
