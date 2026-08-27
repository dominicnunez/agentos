#!/usr/bin/env python3
"""Tests for deterministic AIMS assessment bundle construction."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import subprocess
import tarfile
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from scripts.build_aims_assessment_bundle import _git, build, readiness_report
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
        first = self.root / "work" / "first.tar.gz"
        second = self.root / "work" / "second.tar.gz"
        first_result = build(
            self.root, "a" * 40, datetime(2026, 8, 27, 12), first,
            verify_source=False,
        )
        second_result = build(
            self.root, "a" * 40, datetime(2026, 8, 27, 12), second,
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
                self.assertIsNone(report["source"]["repository"])
                self.assertIn("UNAUTHENTICATED", report["source"]["binding"])

    def test_readiness_requires_approved_governance_records(self) -> None:
        report = readiness_report(
            {"schema_version": 1, "documents": []},
            "a" * 40,
            datetime(2026, 8, 27, 12),
        )
        self.assertFalse(report["certification_assessment_ready"])
        self.assertIn("REQUIRED_GOVERNANCE_RECORDS_MISSING", report["blockers"])

    def test_readiness_follows_approved_successor(self) -> None:
        report = readiness_report(
            {
                "schema_version": 1,
                "documents": [
                    {
                        "id": "aims.ai-policy",
                        "status": "RETIRED",
                        "superseded_by": "aims.ai-policy-v2",
                        "review_due": "2027-08-27",
                    },
                    {
                        "id": "aims.ai-policy-v2",
                        "status": "APPROVED",
                        "superseded_by": None,
                        "approved_at": "2026-08-27T11:00:00Z",
                        "review_due": "2027-08-27",
                    },
                ],
            },
            "a" * 40,
            datetime(2026, 8, 27, 12),
        )
        self.assertNotIn("aims.ai-policy", report["not_approved_required_document_ids"])

    def test_readiness_rejects_overdue_approved_record(self) -> None:
        report = readiness_report(
            {
                "schema_version": 1,
                "documents": [
                    {
                        "id": "aims.ai-policy",
                        "status": "APPROVED",
                        "superseded_by": None,
                        "approved_at": "2026-08-27T11:00:00Z",
                        "review_due": "2026-08-26",
                    }
                ],
            },
            "a" * 40,
            datetime(2026, 8, 27, 12),
        )
        self.assertIn("aims.ai-policy", report["overdue_required_document_ids"])
        self.assertIn("CONTROLLED_REVIEWS_OVERDUE", report["blockers"])

    def test_readiness_rejects_approval_after_assessment(self) -> None:
        report = readiness_report(
            {
                "schema_version": 1,
                "documents": [
                    {
                        "id": "aims.ai-policy",
                        "status": "APPROVED",
                        "superseded_by": None,
                        "approved_at": "2026-08-27T12:00:01Z",
                        "review_due": "2027-08-27",
                    }
                ],
            },
            "a" * 40,
            datetime(2026, 8, 27, 12),
        )
        self.assertIn("aims.ai-policy", report["postdated_approval_document_ids"])
        self.assertIn("APPROVALS_POSTDATE_ASSESSMENT", report["blockers"])

    def test_rejects_output_outside_work_without_overwriting(self) -> None:
        source = self.root / "governance" / "aims" / "manifest.json"
        original = source.read_bytes()
        with self.assertRaisesRegex(VerificationError, "work directory"):
            build(
                self.root,
                "a" * 40,
                datetime(2026, 8, 27, 12),
                source,
                verify_source=False,
            )
        self.assertEqual(source.read_bytes(), original)

    def test_rejects_existing_output(self) -> None:
        output = self.root / "work" / "existing.tar.gz"
        output.parent.mkdir()
        output.write_bytes(b"keep")
        with self.assertRaisesRegex(VerificationError, "already exists"):
            build(
                self.root,
                "a" * 40,
                datetime(2026, 8, 27, 12),
                output,
                verify_source=False,
            )
        self.assertEqual(output.read_bytes(), b"keep")

    def test_archive_supports_long_controlled_path(self) -> None:
        filename = "a" * 110 + ".md"
        document = self.root / "governance" / "aims" / "records" / filename
        document.parent.mkdir(parents=True)
        document.write_text(
            "# Long path\n\nStatus: **DRAFT**\n\nDraft.\n", encoding="utf-8", newline="\n"
        )
        manifest = {
            "schema_version": 1,
            "documents": [
                {
                    "id": "aims.long-path",
                    "path": f"governance/aims/records/{filename}",
                    "version": "0.1",
                    "status": "DRAFT",
                    "owner": "aims-manager",
                    "classification": "PUBLIC",
                    "approval_ref": None,
                    "approved_by": None,
                    "approved_at": None,
                    "review_due": None,
                    "supersedes": [],
                    "superseded_by": None,
                    "sha256": hashlib.sha256(document.read_bytes()).hexdigest(),
                }
            ],
        }
        (self.root / "governance" / "aims" / "manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8", newline="\n"
        )
        output = self.root / "work" / "long.tar.gz"
        build(
            self.root,
            "a" * 40,
            datetime(2026, 8, 27, 12),
            output,
            verify_source=False,
        )
        with tarfile.open(output, "r:gz") as archive:
            self.assertIn(f"repository/governance/aims/records/{filename}", archive.getnames())

    def test_git_reads_disable_replacement_objects(self) -> None:
        completed = subprocess.CompletedProcess([], 0, stdout=b"ok", stderr=b"")
        with patch("scripts.build_aims_assessment_bundle.subprocess.run", return_value=completed) as run:
            self.assertEqual(_git(self.root, ["rev-parse", "HEAD"]), b"ok")
        self.assertEqual(run.call_args.kwargs["env"]["GIT_NO_REPLACE_OBJECTS"], "1")

    def test_readiness_uses_captured_git_verified_snapshot(self) -> None:
        output = self.root / "work" / "snapshot.tar.gz"

        def mutate_worktree(*_args: object) -> None:
            document = self.root / "governance" / "aims" / "records" / "policy.md"
            document.parent.mkdir(parents=True)
            document.write_text(
                "# Policy\n\nStatus: **DRAFT**\n\nChanged after capture.\n",
                encoding="utf-8",
                newline="\n",
            )
            manifest = {
                "schema_version": 1,
                "documents": [
                    {
                        "id": "aims.policy",
                        "path": "governance/aims/records/policy.md",
                        "version": "0.1",
                        "status": "DRAFT",
                        "owner": "aims-manager",
                        "classification": "PUBLIC",
                        "approval_ref": None,
                        "approved_by": None,
                        "approved_at": None,
                        "review_due": None,
                        "supersedes": [],
                        "superseded_by": None,
                        "sha256": hashlib.sha256(document.read_bytes()).hexdigest(),
                    }
                ],
            }
            (self.root / "governance" / "aims" / "manifest.json").write_text(
                json.dumps(manifest, indent=2) + "\n", encoding="utf-8", newline="\n"
            )

        with (
            patch(
                "scripts.build_aims_assessment_bundle._verify_git_sources",
                side_effect=mutate_worktree,
            ),
            patch("scripts.build_aims_assessment_bundle._git", return_value=b""),
        ):
            build(self.root, "a" * 40, datetime(2026, 8, 27, 12), output)

        with tarfile.open(output, "r:gz") as archive:
            bundled_manifest = json.load(
                archive.extractfile("repository/governance/aims/manifest.json")  # type: ignore[arg-type]
            )
            readiness = json.load(
                archive.extractfile("assessment/readiness.json")  # type: ignore[arg-type]
            )
        self.assertEqual(bundled_manifest["documents"], [])
        self.assertEqual(readiness["controlled_document_counts"]["DRAFT"], 0)


if __name__ == "__main__":
    unittest.main()
