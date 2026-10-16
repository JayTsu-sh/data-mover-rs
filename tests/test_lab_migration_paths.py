"""Offline contract checks for migration-only shell harnesses (no backend access)."""

from pathlib import Path
import os
import re
import shlex
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = [
    ROOT / ".claude/skills/_shared/resume_matrix.sh",
    *sorted((ROOT / ".claude/skills/e2e-s3/scripts").glob("*.sh")),
    *sorted((ROOT / ".claude/skills/e2e-nfs/scripts").glob("*.sh")),
]


class MigrationHarnessPaths(unittest.TestCase):
    def test_directory_object_fixture_initializes_a_fresh_source_root(self):
        script = ROOT / ".claude/skills/e2e-s3/scripts/directory_objects.sh"
        source = script.read_text()
        start = source.index("# A local file cannot have a trailing slash.")
        setup = source[start:source.index("code=$(s3c", start)]
        with tempfile.TemporaryDirectory() as scratch:
            local = Path(scratch) / "local"
            subprocess.run(
                ["bash", "-euc", setup],
                env={**os.environ, "LOCAL": str(local), "BIG": "17"},
                check=True,
            )
            self.assertEqual((local / "src/big").stat().st_size, 17)

    def test_shell_syntax(self):
        for script in SCRIPTS:
            with self.subTest(script=script.name):
                subprocess.run(["bash", "-n", str(script)], check=True)

    def test_explicit_transfer_paths_match(self):
        checked = 0
        for script in SCRIPTS:
            source = script.read_text().replace("\\\n", " ")
            for line in source.splitlines():
                if "--source-path" not in line or "--destination-path" not in line:
                    continue
                # compare() forwards its two path parameters; its callers are checked below.
                if script.name == "directory_objects.sh" and '--source-path "$2"' in line:
                    continue
                paths = re.findall(r'--(?:source|destination)-path\s+("[^"]*"|\S+)', line)
                self.assertEqual(len(paths), 2, (script.name, line))
                self.assertEqual(paths[0].strip('"'), paths[1].strip('"'), (script.name, line))
                checked += 1
        self.assertGreater(checked, 40)

    def test_versioning_case_paths_match(self):
        script = ROOT / ".claude/skills/e2e-s3/scripts/versioning_matrix.sh"
        source = script.read_text()
        array_paths = {}
        for name, body in re.findall(r'^(\w+)=\((.*)\)$', source, re.MULTILINE):
            args = shlex.split(body)
            if "--source-path" in args:
                array_paths['${' + name + '[@]}'] = args[args.index("--source-path") + 1]
        checked = 0
        for line in source.splitlines():
            if not line.startswith("case_ "):
                continue
            args = shlex.split(line)
            destination_path = args[4]
            if "--source-path" in args:
                source_path = args[args.index("--source-path") + 1]
            else:
                selected = [array_paths[arg] for arg in args if arg in array_paths]
                self.assertEqual(len(selected), 1, line)
                source_path = selected[0]
            self.assertEqual(source_path, destination_path, line)
            checked += 1
        self.assertGreater(checked, 15)

    def test_directory_compare_paths_match(self):
        script = ROOT / ".claude/skills/e2e-s3/scripts/directory_objects.sh"
        checked = 0
        for line in script.read_text().splitlines():
            if line.startswith("compare "):
                args = shlex.split(line)
                self.assertEqual(args[2], args[4], line)
                checked += 1
        self.assertGreater(checked, 3)

    def test_integrity_missing_destination_accepts_typed_difference(self):
        script = ROOT / "tests/lab/run-integrity-e2e.sh"
        source = script.read_text()
        self.assertIn(
            "(Missing.*Destination|File not found|IO error:.*not found|NoSuchKey|NotFound)",
            source,
        )


if __name__ == "__main__":
    unittest.main()
