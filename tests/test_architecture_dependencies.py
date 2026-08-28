#!/usr/bin/env python3
"""Contract tests for the target deep-module tree and dependency guard."""

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VALIDATOR = ROOT / "tests" / "validate_architecture_dependencies.py"
PUBLIC_MODULES = {"model", "storage", "traversal", "metadata", "transfer", "integrity"}


class ArchitectureDependenciesTest(unittest.TestCase):
    def run_validator(self, root: Path = ROOT) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(VALIDATOR), str(root)],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_target_module_tree_is_declared_with_required_visibility(self):
        lib = (ROOT / "src" / "lib.rs").read_text()
        for module in PUBLIC_MODULES:
            self.assertIn(f"pub mod {module};", lib)
            self.assertTrue((ROOT / "src" / module / "mod.rs").is_file())
        self.assertIn("pub(crate) mod runtime;", lib)
        self.assertTrue((ROOT / "src" / "runtime" / "mod.rs").is_file())

    def test_storage_backend_facade_tree_is_complete(self):
        backends = ROOT / "src" / "storage" / "backends"
        for backend in {"local", "nfs", "cifs", "s3", "hdfs"}:
            self.assertTrue((backends / backend / "mod.rs").is_file())

    def test_repository_passes_dependency_guard(self):
        result = self.run_validator()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("architecture dependency guard: passed", result.stdout)

    def test_ci_runs_dependency_guard(self):
        workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
        self.assertIn("python3 tests/validate_architecture_dependencies.py .", workflow)
        self.assertIn("python3 -m unittest tests/test_architecture_dependencies.py", workflow)

    def test_guard_rejects_runtime_importing_public_module(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Path(temporary)
            shutil.copytree(ROOT / "src", fixture / "src")
            (fixture / "src" / "runtime" / "bad.rs").write_text("use crate::transfer::TransferRequest;\n")
            result = self.run_validator(fixture)
        self.assertEqual(result.returncode, 2)
        self.assertIn("runtime must not import transfer", result.stderr)

    def test_guard_rejects_grouped_and_aliased_imports(self):
        snippets = (
            "use crate::{transfer::TransferRequest};\n",
            "use crate::{transfer::{TransferRequest}};\n",
            "use crate as dm; use dm::transfer::TransferRequest;\n",
            "use crate as dm; use dm::{transfer::{TransferRequest}};\n",
            "extern crate self as dm; use dm::transfer::TransferRequest;\n",
        )
        for snippet in snippets:
            with self.subTest(snippet=snippet):
                with tempfile.TemporaryDirectory() as temporary:
                    fixture = Path(temporary)
                    shutil.copytree(ROOT / "src", fixture / "src")
                    (fixture / "src" / "runtime" / "bad.rs").write_text(snippet)
                    result = self.run_validator(fixture)
                self.assertEqual(result.returncode, 2)
                self.assertIn("runtime must not import transfer", result.stderr)

    def test_guard_rejects_legacy_or_protocol_dependencies(self):
        cases = (("model", "nfs"), ("traversal", "s3"))
        for module, dependency in cases:
            with self.subTest(module=module):
                with tempfile.TemporaryDirectory() as temporary:
                    fixture = Path(temporary)
                    shutil.copytree(ROOT / "src", fixture / "src")
                    (fixture / "src" / module / "bad.rs").write_text(f"use crate::{dependency}::Client;\n")
                    result = self.run_validator(fixture)
                self.assertEqual(result.returncode, 2)
                self.assertIn(f"{module} must not import {dependency}", result.stderr)

    def test_guard_rejects_cross_backend_import(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Path(temporary)
            shutil.copytree(ROOT / "src", fixture / "src")
            (fixture / "src" / "storage" / "backends" / "nfs" / "bad.rs").write_text(
                "use crate::storage::backends::s3::Client;\n"
            )
            result = self.run_validator(fixture)
        self.assertEqual(result.returncode, 2)
        self.assertIn("backend nfs must not import backend s3", result.stderr)

    def test_guard_rejects_grouped_cross_backend_and_runtime_imports(self):
        snippets = (
            ("use crate::storage::backends::{s3::Client};\n", "backend nfs must not import backend s3"),
            ("use super::super::s3::Client;\n", "backend nfs must not import backend s3"),
            ("use crate::storage as st; use st::backends::s3::Client;\n", "backend nfs must not import backend s3"),
            ("use crate as dm; use dm::storage::backends::s3::Client;\n", "backend nfs must not import backend s3"),
            ("use crate::runtime::Queue;\n", "backend nfs must not import runtime"),
        )
        for snippet, expected in snippets:
            with self.subTest(snippet=snippet):
                with tempfile.TemporaryDirectory() as temporary:
                    fixture = Path(temporary)
                    shutil.copytree(ROOT / "src", fixture / "src")
                    (fixture / "src" / "storage" / "backends" / "nfs" / "bad.rs").write_text(snippet)
                    result = self.run_validator(fixture)
                self.assertEqual(result.returncode, 2)
                self.assertIn(expected, result.stderr)

    def test_guard_ignores_dependencies_in_comments_and_strings(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Path(temporary)
            shutil.copytree(ROOT / "src", fixture / "src")
            (fixture / "src" / "runtime" / "notes.rs").write_text(
                '/* outer /* crate::transfer */ BackendKind::S3 */\n'
                '// crate::transfer must stay forbidden\nconst NOTE: &str = "BackendKind::S3";\n'
            )
            result = self.run_validator(fixture)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_guard_rejects_commented_module_declaration(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Path(temporary)
            shutil.copytree(ROOT / "src", fixture / "src")
            lib = (fixture / "src" / "lib.rs")
            lib.write_text(lib.read_text().replace("pub mod model;", "// pub mod model;"))
            result = self.run_validator(fixture)
        self.assertEqual(result.returncode, 2)
        self.assertIn("public module model", result.stderr)

    def test_guard_rejects_undeclared_backend_facade(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Path(temporary)
            shutil.copytree(ROOT / "src", fixture / "src")
            backends = fixture / "src" / "storage" / "backends" / "mod.rs"
            backends.write_text(backends.read_text().replace("pub(crate) mod nfs;", ""))
            result = self.run_validator(fixture)
        self.assertEqual(result.returncode, 2)
        self.assertIn("backend facade nfs is missing", result.stderr)

    def test_guard_rejects_backend_kind_orchestration(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Path(temporary)
            shutil.copytree(ROOT / "src", fixture / "src")
            (fixture / "src" / "transfer" / "bad.rs").write_text(
                "const KIND: BackendKind = BackendKind::S3;\n"
            )
            result = self.run_validator(fixture)
        self.assertEqual(result.returncode, 2)
        self.assertIn("transfer must not branch on BackendKind", result.stderr)

    def test_guard_rejects_product_leakage_in_target_modules(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Path(temporary)
            shutil.copytree(ROOT / "src", fixture / "src")
            (fixture / "src" / "transfer" / "bad.rs").write_text('const PART: &str = ".terrasync-part";\n')
            result = self.run_validator(fixture)
        self.assertEqual(result.returncode, 2)
        self.assertIn("terrasync product term", result.stderr)


if __name__ == "__main__":
    unittest.main()
