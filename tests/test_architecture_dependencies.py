#!/usr/bin/env python3
"""Workspace dependency direction contracts, including negative mutations."""
import importlib.util
import shutil
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("architecture", ROOT / "tests/validate_architecture_dependencies.py")
GUARD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GUARD)


class ArchitectureDependenciesTest(unittest.TestCase):
    def fixture(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        shutil.copytree(ROOT / "crates", root / "crates")
        shutil.copy2(ROOT / "Cargo.toml", root / "Cargo.toml")
        return root

    def add_dependency(self, root, owner, dependency, section="dependencies"):
        path = root / "crates" / owner / "Cargo.toml"
        path.write_text(path.read_text().replace(f"[{section}]", f'[{section}]\n{dependency} = "0.4"', 1))

    def test_repository_obeys_dependency_direction(self):
        self.assertEqual(GUARD.validate(ROOT), [])

    def test_engines_cannot_depend_on_protocol_implementations(self):
        root = self.fixture()
        self.add_dependency(root, "transfer", "data-mover-backend-s3")
        self.assertTrue(any("must not depend" in e for e in GUARD.validate(root)))

    def test_backends_cannot_depend_on_other_backends(self):
        root = self.fixture()
        self.add_dependency(root, "backend-nfs", "data-mover-backend-s3")
        self.assertTrue(any("must not depend" in e for e in GUARD.validate(root)))

    def test_core_cannot_depend_on_the_facade(self):
        root = self.fixture()
        self.add_dependency(root, "storage-core", "data-mover")
        self.assertTrue(any("facade" in e for e in GUARD.validate(root)))

    def test_protocol_sdk_cannot_leak_into_core(self):
        root = self.fixture()
        self.add_dependency(root, "storage-core", "aws-sdk-s3")
        self.assertTrue(any("protocol SDK" in e for e in GUARD.validate(root)))

    def test_dependency_alias_cannot_hide_an_sdk(self):
        root = self.fixture()
        path = root / "crates/storage-core/Cargo.toml"
        path.write_text(path.read_text().replace("[dependencies]", '[dependencies]\nprotocol = { package = "aws-sdk-s3", version = "1" }', 1))
        self.assertTrue(any("protocol SDK" in e for e in GUARD.validate(root)))

    def test_target_specific_dependency_is_checked(self):
        root = self.fixture()
        path = root / "crates/model/Cargo.toml"
        with path.open("a") as manifest:
            manifest.write('\n[target.\'cfg(windows)\'.dependencies]\ndata-mover-backend-cifs = "0.4"\n')
        self.assertTrue(any("must not depend" in e for e in GUARD.validate(root)))

    def test_dev_dependencies_can_supply_backend_fakes(self):
        root = self.fixture()
        self.add_dependency(root, "metadata", "data-mover-test-support", "dev-dependencies")
        self.assertEqual(GUARD.validate(root), [])

    def test_missing_backend_is_rejected(self):
        root = self.fixture()
        (root / "crates/backend-hdfs/Cargo.toml").unlink()
        self.assertTrue(any("missing" in e for e in GUARD.validate(root)))

    def test_members_must_inherit_lints(self):
        root = self.fixture()
        path = root / "crates/model/Cargo.toml"
        path.write_text(path.read_text().replace("[lints]\nworkspace = true", "[lints]"))
        self.assertTrue(any("lints" in e for e in GUARD.validate(root)))

    def test_backend_kind_dispatch_is_rejected_in_engines(self):
        root = self.fixture()
        (root / "crates/transfer/src/transfer/bad.rs").write_text("fn bad() { let _ = BackendKind::S3; }")
        self.assertTrue(any("must not branch" in e for e in GUARD.validate(root)))

    def test_comments_and_strings_do_not_count_as_dispatch(self):
        root = self.fixture()
        (root / "crates/transfer/src/transfer/documented.rs").write_text('// BackendKind::S3\nconst EXAMPLE: &str = "BackendKind::S3";')
        self.assertEqual(GUARD.validate(root), [])


if __name__ == "__main__":
    unittest.main()
