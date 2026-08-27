#!/usr/bin/env python3
"""Tests for deterministic AIMS assessment bundle construction."""

from __future__ import annotations

import gzip
import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path

from scripts.build_aims_assessment_bundle import build, readiness_report
from scripts.verify_aims_documents import VerificationError


class BuildAIMSAssessmentBundleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        for relative in (
            "README.md",
            "SECURITY.md",
            "docs/AI_MANAGEMENT_SYSTEM.md",
            "docs/APPROVAL_CONTROL.md",
            "docs/EVENT_LEDGER_INTEGRITY.md",
            "docs/GOVERNANCE_INSPECTION.md",
            "docs/INCIDENT_REPLAY.md",
            "docs/LAB.md",
            "docs/ORGANIZATIONAL_KNOWLEDGE.md",
            "docs/SHARED_COORDINATION.md",
            "docs/SQLITE_RECOVERY.md",
            "docs/THREAT_MODEL.md",
            "docs/development/BUILD_CONTRACT.md",
            "docs/development/ISO_IEC_42001_READINESS.md",
            "docs/development/V1_ACCEPTANCE_STATUS.md",
            "governance/aims/README.md",
        ):
            path = self.root.joinpath(*relative.split("/"))
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"evidence: {relative}\n", encoding="utf-8", newline="\n")
        manifest = {"schema_version": 1, "documents": []}
        (self.root / "governance" / "aims" / "manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8", newline="\n"
        )

    def test_build_is_byte_for_byte_deterministic_and_safe(self) -> None:
        first = self.root / "first.tar.gz"
        second = self.root / "second.tar.gz"
        first_result = build(
            self.root, "https://github.com/dominicnunez/agentos", "a" * 40, first,
            verify_source=False,
        )
        second_result = build(
            self.root, "https://github.com/dominicnunez/agentos", "a" * 40, second,
            verify_source=False,
        )
        self.assertEqual(first_result, second_result)
        self.assertEqual(first.read_bytes(), second.read_bytes())

        with gzip.GzipFile(fileobj=io.BytesIO(first.read_bytes()), mode="rb") as compressed:
            with tarfile.open(fileobj=io.BytesIO(compressed.read()), mode="r:") as archive:
                names = archive.getnames()
                self.assertIn("assessment/readiness.json", names)
                self.assertIn("SHA256SUMS", names)
                self.assertTrue(all(not name.startswith("/") and ".." not in name.split("/") for name in names))
                report = json.load(archive.extractfile("assessment/readiness.json"))  # type: ignore[arg-type]
                self.assertFalse(report["certification_assessment_ready"])
                self.assertFalse(report["conformity_determined"])
                self.assertFalse(report["certified"])

    def test_rejects_noncanonical_repository(self) -> None:
        with self.assertRaisesRegex(VerificationError, "canonical"):
            build(
                self.root,
                "https://example.com/agentos",
                "a" * 40,
                self.root / "bad.tar.gz",
                verify_source=False,
            )

    def test_readiness_requires_approved_governance_records(self) -> None:
        report = readiness_report(
            {"schema_version": 1, "documents": []},
            "https://github.com/dominicnunez/agentos",
            "a" * 40,
        )
        self.assertFalse(report["certification_assessment_ready"])
        self.assertIn("REQUIRED_GOVERNANCE_RECORDS_MISSING", report["blockers"])


if __name__ == "__main__":
    unittest.main()
