#!/usr/bin/env python3
"""Regression tests for the cluster-free HDFS directed-matrix contract."""

from __future__ import annotations

import os
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class HdfsDirectedMatrixTests(unittest.TestCase):
    def test_contract_only_does_not_require_lab_credentials(self) -> None:
        environment = os.environ.copy()
        for variable in (
            "LAB_HDFS_ADMIN_USER",
            "LAB_HDFS_CONFIG_DIR",
            "LAB_HDFS_KEYTAB",
            "LAB_HDFS_LOCATION",
        ):
            environment.pop(variable, None)

        result = subprocess.run(
            [
                "bash",
                "tests/lab/run-e2e.sh",
                "nightly-contract-regression",
                "--contract-only",
            ],
            cwd=ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("25 directed pairs, 9 involving HDFS", result.stdout)


if __name__ == "__main__":
    unittest.main()
