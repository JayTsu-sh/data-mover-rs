"""Harness artifact selection: one tiny Cargo build, no data-mover or remote dependencies."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / ".claude/skills/_shared/cargo_examples.sh"


class CargoExampleArtifacts(unittest.TestCase):
    def test_alternate_target_directory_wins_over_a_stale_local_binary(self) -> None:
        with tempfile.TemporaryDirectory(prefix="cargo artifact contract ") as directory:
            project = Path(directory)
            (project / "examples").mkdir()
            (project / "src").mkdir()
            (project / "src/lib.rs").write_text("", encoding="utf-8")
            (project / "Cargo.toml").write_text(
                '[package]\nname = "artifact-contract"\nversion = "0.0.0"\nedition = "2021"\n',
                encoding="utf-8",
            )
            (project / "examples/marker.rs").write_text(
                'fn main() { println!("fresh-artifact"); }\n', encoding="utf-8"
            )
            stale = project / "target/debug/examples/marker"
            stale.parent.mkdir(parents=True)
            stale.write_text('#!/bin/sh\necho stale-artifact\nexit 99\n', encoding="utf-8")
            stale.chmod(0o755)
            alternate = project / "actual target with spaces"
            # A configured build target adds a target triple below target-dir. A naive
            # CARGO_TARGET_DIR/debug reconstruction must also fail this regression.
            rustc = subprocess.run(["rustc", "-vV"], capture_output=True, text=True, check=True)
            host = next(line.removeprefix("host: ") for line in rustc.stdout.splitlines() if line.startswith("host: "))
            config = project / ".cargo/config.toml"
            config.parent.mkdir()
            config.write_text(
                '[build]\ntarget = ' + json.dumps(host) + '\ntarget-dir = "configured stale output"\n',
                encoding="utf-8",
            )
            wrong_layout = alternate / "debug/examples/marker"
            wrong_layout.parent.mkdir(parents=True)
            wrong_layout.write_text(stale.read_text(encoding="utf-8"), encoding="utf-8")
            wrong_layout.chmod(0o755)
            env = os.environ.copy()
            # This fixture uses the host toolchain, independent of production cross-build flags.
            for name in ("CARGO_BUILD_TARGET", "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS"):
                env.pop(name, None)
            env["CARGO_TARGET_DIR"] = str(alternate)
            result = subprocess.run(
                [
                    "bash", "-c",
                    'source "$1"; cargo_build_examples marker || exit $?; '
                    'binary=$(cargo_example_binary marker) || exit $?; '
                    'printf "%s\\n" "$binary"; "$binary"',
                    "artifact-contract", str(HELPER),
                ],
                cwd=project, env=env, capture_output=True, text=True, check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            selected, output = result.stdout.strip().splitlines()
            self.assertEqual(Path(selected), alternate / host / "debug/examples/marker")
            self.assertEqual(output, "fresh-artifact")
            self.assertIn("stale-artifact", stale.read_text(encoding="utf-8"))

    def test_failed_or_missing_artifacts_never_fall_back_to_a_local_binary(self) -> None:
        for cargo_body, expected in (("return 44", 44), ("return 0", 1)):
            with self.subTest(cargo_body=cargo_body):
                result = subprocess.run(
                    [
                        "bash", "-c",
                        'source "$1"; cargo() { ' + cargo_body + '; }; '
                        'cargo_build_examples marker; status=$?; '
                        'cargo_example_binary marker >/dev/null && exit 99; exit "$status"',
                        "artifact-contract", str(HELPER),
                    ],
                    capture_output=True, text=True, check=False,
                )
                self.assertEqual(result.returncode, expected, result.stderr)


if __name__ == "__main__":
    unittest.main()
