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

from scripts.build_aims_assessment_bundle import (
    REQUIRED_APPROVED_DOCUMENTS,
    _git,
    _git_aims_entries,
    _verify_governed_commit_order,
    _verified_commit_at,
    _verify_aims_history,
    _verify_captured_aims_snapshot,
    _verify_controlled_commit_snapshot,
    build,
    readiness_report,
)
from scripts.verify_aims_documents import MAX_DOCUMENT_BYTES, VerificationError


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

    def approved_readiness_manifest(self, outcomes: dict[str, object] | None) -> dict[str, object]:
        documents = [
            {
                "id": document_id,
                "status": "APPROVED",
                "superseded_by": None,
                "approved_at": "2026-08-27T11:00:00Z",
                "review_due": "2027-08-27",
            }
            for document_id in sorted(REQUIRED_APPROVED_DOCUMENTS)
        ]
        manifest: dict[str, object] = {
            "schema_version": 1 if outcomes is None else 2,
            "documents": documents,
        }
        if outcomes is not None:
            manifest["assessment_outcomes"] = outcomes
        return manifest

    @staticmethod
    def affirmative_outcomes() -> dict[str, object]:
        return {
            "audit": {
                "document_id": "aims.audit-result",
                "result": "PASS",
                "open_blocking_findings": 0,
            },
            "management_review": {
                "document_id": "aims.management-review-result",
                "disposition": "PROCEED",
            },
            "statement_of_applicability": {
                "document_id": "aims.statement-of-applicability",
                "result": "COMPLETE",
            },
            "readiness_decision": {
                "document_id": "aims.assessment-readiness-decision",
                "disposition": "READY",
            },
        }

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
                evidence_index = json.load(
                    archive.extractfile("assessment/evidence-index.json")  # type: ignore[arg-type]
                )
                assessment_readme = archive.extractfile("ASSESSMENT_README.txt").read().decode()  # type: ignore[union-attr]
                self.assertFalse(report["certification_assessment_ready"])
                self.assertFalse(report["conformity_determined"])
                self.assertFalse(report["certified"])
                self.assertIsNone(report["source"]["repository"])
                self.assertIsNone(report["source"]["history_baseline"])
                self.assertEqual(report["source"]["binding"], "SOURCE_UNVERIFIED")
                self.assertIn("SOURCE_NOT_GIT_VERIFIED", report["blockers"])
                self.assertEqual(evidence_index["source"]["binding"], "SOURCE_UNVERIFIED")
                self.assertIn("source-unverified working tree", assessment_readme)
                self.assertIn("not bound to the declared source commit", assessment_readme)

    def test_readiness_requires_approved_governance_records(self) -> None:
        report = readiness_report(
            {"schema_version": 1, "documents": []},
            "a" * 40,
            datetime(2026, 8, 27, 12),
        )
        self.assertFalse(report["certification_assessment_ready"])
        self.assertIn("REQUIRED_GOVERNANCE_RECORDS_MISSING", report["blockers"])

    def test_readiness_requires_machine_readable_affirmative_outcomes(self) -> None:
        report = readiness_report(
            self.approved_readiness_manifest(None),
            "a" * 40,
            datetime(2026, 8, 27, 12),
        )
        self.assertFalse(report["certification_assessment_ready"])
        self.assertIn("AFFIRMATIVE_ASSESSMENT_OUTCOMES_NOT_RECORDED", report["blockers"])

    def test_readiness_rejects_negative_approved_outcome(self) -> None:
        outcomes = self.affirmative_outcomes()
        outcomes["management_review"]["disposition"] = "DO_NOT_PROCEED"  # type: ignore[index]
        report = readiness_report(
            self.approved_readiness_manifest(outcomes),
            "a" * 40,
            datetime(2026, 8, 27, 12),
            commit_at=datetime(2026, 8, 27, 11),
            source_verified=True,
        )
        self.assertFalse(report["certification_assessment_ready"])
        self.assertIn("MANAGEMENT_REVIEW_BLOCKS_READINESS", report["blockers"])

    def test_readiness_requires_zero_blocking_audit_findings(self) -> None:
        outcomes = self.affirmative_outcomes()
        outcomes["audit"]["open_blocking_findings"] = 1  # type: ignore[index]
        report = readiness_report(
            self.approved_readiness_manifest(outcomes),
            "a" * 40,
            datetime(2026, 8, 27, 12),
            commit_at=datetime(2026, 8, 27, 11),
            source_verified=True,
        )
        self.assertFalse(report["certification_assessment_ready"])
        self.assertIn("INTERNAL_AUDIT_OUTCOME_BLOCKS_READINESS", report["blockers"])

    def test_readiness_accepts_only_complete_affirmative_outcomes(self) -> None:
        outcomes = self.affirmative_outcomes()
        report = readiness_report(
            self.approved_readiness_manifest(outcomes),
            "a" * 40,
            datetime(2026, 8, 27, 12),
            commit_at=datetime(2026, 8, 27, 11),
            source_verified=True,
        )
        self.assertTrue(report["certification_assessment_ready"])
        self.assertEqual(report["assessment_outcomes"], outcomes)
        self.assertEqual(report["blockers"], [])

    def test_readiness_rejects_approval_after_verified_source_commit(self) -> None:
        outcomes = self.affirmative_outcomes()
        report = readiness_report(
            self.approved_readiness_manifest(outcomes),
            "a" * 40,
            datetime(2026, 8, 27, 12),
            commit_at=datetime(2026, 8, 27, 10, 59, 59),
            source_verified=True,
        )
        self.assertFalse(report["certification_assessment_ready"])
        self.assertEqual(
            report["source_commit_postdated_approval_document_ids"],
            sorted(REQUIRED_APPROVED_DOCUMENTS),
        )
        self.assertIn("APPROVALS_POSTDATE_SOURCE_COMMIT", report["blockers"])

    def test_readiness_rejects_verified_source_without_commit_time(self) -> None:
        report = readiness_report(
            self.approved_readiness_manifest(self.affirmative_outcomes()),
            "a" * 40,
            datetime(2026, 8, 27, 12),
            source_verified=True,
        )
        self.assertFalse(report["certification_assessment_ready"])
        self.assertIn("VERIFIED_SOURCE_COMMIT_TIME_MISSING", report["blockers"])

    def test_readiness_follows_approved_successor(self) -> None:
        report = readiness_report(
            {
                "schema_version": 1,
                "documents": [
                    {
                        "id": "aims.ai-policy",
                        "status": "RETIRED",
                        "superseded_by": "aims.ai-policy-v2",
                        "approved_at": "2026-08-27T10:00:00Z",
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

    def test_readiness_rejects_future_retirement_decision(self) -> None:
        report = readiness_report(
            {
                "schema_version": 1,
                "documents": [
                    {
                        "id": "aims.ai-policy",
                        "status": "RETIRED",
                        "superseded_by": "aims.ai-policy-v2",
                        "approved_at": "2026-08-27T12:00:01Z",
                        "review_due": "2027-08-27",
                    },
                    {
                        "id": "aims.ai-policy-v2",
                        "status": "APPROVED",
                        "superseded_by": None,
                        "approved_at": "2026-08-27T10:00:00Z",
                        "review_due": "2027-08-27",
                    },
                ],
            },
            "a" * 40,
            datetime(2026, 8, 27, 12),
            commit_at=datetime(2026, 8, 27, 11),
            source_verified=True,
        )
        self.assertIn("aims.ai-policy", report["postdated_approval_document_ids"])
        self.assertIn(
            "aims.ai-policy",
            report["source_commit_postdated_approval_document_ids"],
        )
        self.assertIn("APPROVALS_POSTDATE_ASSESSMENT", report["blockers"])
        self.assertIn("APPROVALS_POSTDATE_SOURCE_COMMIT", report["blockers"])

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
            patch(
                "scripts.build_aims_assessment_bundle._verified_commit_at",
                return_value=datetime(2026, 8, 27, 11),
            ),
            patch(
                "scripts.build_aims_assessment_bundle._verify_controlled_commit_snapshot"
            ),
            patch(
                "scripts.build_aims_assessment_bundle._verify_aims_history",
                side_effect=lambda _root, _baseline, _commit, entries: (
                    _verify_captured_aims_snapshot(entries, None)
                ),
            ),
        ):
            build(
                self.root,
                "a" * 40,
                datetime(2026, 8, 27, 12),
                output,
                history_baseline="b" * 40,
            )

        with tarfile.open(output, "r:gz") as archive:
            bundled_manifest = json.load(
                archive.extractfile("repository/governance/aims/manifest.json")  # type: ignore[arg-type]
            )
            readiness = json.load(
                archive.extractfile("assessment/readiness.json")  # type: ignore[arg-type]
            )
        self.assertEqual(bundled_manifest["documents"], [])
        self.assertEqual(readiness["controlled_document_counts"]["DRAFT"], 0)
        self.assertEqual(readiness["source"]["history_baseline"], "b" * 40)
        self.assertEqual(readiness["source"]["commit_at"], "2026-08-27T11:00:00Z")

    def test_rejects_commit_after_assessment_instant(self) -> None:
        with patch(
            "scripts.build_aims_assessment_bundle._git",
            return_value=b"2026-08-27T13:00:00+00:00\n",
        ):
            with self.assertRaisesRegex(VerificationError, "postdates"):
                _verified_commit_at(
                    self.root, "a" * 40, datetime(2026, 8, 27, 12)
                )

    def test_rejects_governance_decision_after_containing_commit(self) -> None:
        manifest = self.approved_readiness_manifest(None)
        with self.assertRaisesRegex(VerificationError, "postdate their containing commit"):
            _verify_governed_commit_order(
                manifest,
                datetime(2026, 8, 27, 10, 59, 59),
                "a" * 40,
            )

    def test_history_walk_rejects_violation_hidden_before_final_commit(self) -> None:
        document = self.root / "governance" / "aims" / "records" / "policy.md"
        document.parent.mkdir(parents=True)

        def write_approved(
            body: str, version: str, approval_ref: str, approved_at: str
        ) -> None:
            document.write_text(
                f"# Policy\n\nStatus: **APPROVED**\n\n{body}\n",
                encoding="utf-8",
                newline="\n",
            )
            manifest = {
                "schema_version": 1,
                "documents": [
                    {
                        "id": "aims.policy",
                        "path": "governance/aims/records/policy.md",
                        "version": version,
                        "status": "APPROVED",
                        "owner": "aims-manager",
                        "classification": "PUBLIC",
                        "approval_ref": approval_ref,
                        "approved_by": "project-owner",
                        "approved_at": approved_at,
                        "review_due": "2027-08-27",
                        "supersedes": [],
                        "superseded_by": None,
                        "sha256": hashlib.sha256(document.read_bytes()).hexdigest(),
                    }
                ],
            }
            (self.root / "governance" / "aims" / "manifest.json").write_text(
                json.dumps(manifest, indent=2) + "\n", encoding="utf-8", newline="\n"
            )

        subprocess.run(["git", "init", "-q"], cwd=self.root, check=True)
        subprocess.run(
            ["git", "config", "core.autocrlf", "false"], cwd=self.root, check=True
        )
        subprocess.run(
            ["git", "config", "user.email", "tests@agentos.invalid"],
            cwd=self.root,
            check=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Agent OS Tests"], cwd=self.root, check=True
        )
        write_approved(
            "Original approved bytes.",
            "1.0",
            "decision-original",
            "2026-08-27T10:00:00Z",
        )
        subprocess.run(["git", "add", "."], cwd=self.root, check=True)
        subprocess.run(["git", "commit", "-q", "-m", "divergence"], cwd=self.root, check=True)
        baseline_branch = subprocess.check_output(
            ["git", "branch", "--show-current"], cwd=self.root
        ).decode().strip()
        subprocess.run(["git", "checkout", "-q", "-b", "long-lived"], cwd=self.root, check=True)

        write_approved(
            "Legitimately approved v2 bytes.",
            "1.1",
            "decision-v2",
            "2026-08-27T11:00:00Z",
        )
        subprocess.run(["git", "add", "."], cwd=self.root, check=True)
        subprocess.run(["git", "commit", "-q", "-m", "approved-v2"], cwd=self.root, check=True)
        write_approved(
            "Unapproved v3 bytes reusing v2 evidence.",
            "1.2",
            "decision-v2",
            "2026-08-27T11:00:00Z",
        )
        subprocess.run(["git", "add", "."], cwd=self.root, check=True)
        subprocess.run(["git", "commit", "-q", "-m", "violation"], cwd=self.root, check=True)
        subprocess.run(["git", "checkout", "-q", baseline_branch], cwd=self.root, check=True)
        (self.root / "README.md").write_text(
            "trusted main advanced after branch divergence\n", encoding="utf-8", newline="\n"
        )
        subprocess.run(["git", "add", "README.md"], cwd=self.root, check=True)
        subprocess.run(["git", "commit", "-q", "-m", "trusted-baseline"], cwd=self.root, check=True)
        baseline = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.root).decode().strip()
        subprocess.run(
            ["git", "merge", "-q", "--no-ff", "-m", "merge-long-lived", "long-lived"],
            cwd=self.root,
            check=True,
        )
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.root).decode().strip()
        source_entries = {
            "governance/aims/manifest.json": (
                self.root / "governance" / "aims" / "manifest.json"
            ).read_bytes(),
            "governance/aims/records/policy.md": document.read_bytes(),
        }

        with self.assertRaisesRegex(VerificationError, "lacks new approval evidence"):
            _verify_aims_history(self.root, baseline, commit, source_entries)

    def test_history_walk_rejects_approval_reuse_in_merge_resolution(self) -> None:
        document = self.root / "governance" / "aims" / "records" / "policy.md"
        document.parent.mkdir(parents=True)

        def write_approved(
            body: str, version: str, approval_ref: str, approved_at: str
        ) -> None:
            document.write_text(
                f"# Policy\n\nStatus: **APPROVED**\n\n{body}\n",
                encoding="utf-8",
                newline="\n",
            )
            manifest = {
                "schema_version": 1,
                "documents": [
                    {
                        "id": "aims.policy",
                        "path": "governance/aims/records/policy.md",
                        "version": version,
                        "status": "APPROVED",
                        "owner": "aims-manager",
                        "classification": "PUBLIC",
                        "approval_ref": approval_ref,
                        "approved_by": "project-owner",
                        "approved_at": approved_at,
                        "review_due": "2027-08-27",
                        "supersedes": [],
                        "superseded_by": None,
                        "sha256": hashlib.sha256(document.read_bytes()).hexdigest(),
                    }
                ],
            }
            (self.root / "governance" / "aims" / "manifest.json").write_text(
                json.dumps(manifest, indent=2) + "\n", encoding="utf-8", newline="\n"
            )

        subprocess.run(["git", "init", "-q"], cwd=self.root, check=True)
        subprocess.run(
            ["git", "config", "core.autocrlf", "false"], cwd=self.root, check=True
        )
        subprocess.run(
            ["git", "config", "user.email", "tests@agentos.invalid"],
            cwd=self.root,
            check=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Agent OS Tests"], cwd=self.root, check=True
        )
        write_approved(
            "Original approved bytes.",
            "1.0",
            "decision-original",
            "2026-08-27T10:00:00Z",
        )
        subprocess.run(["git", "add", "."], cwd=self.root, check=True)
        subprocess.run(["git", "commit", "-q", "-m", "baseline"], cwd=self.root, check=True)
        baseline = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.root).decode().strip()
        baseline_branch = subprocess.check_output(
            ["git", "branch", "--show-current"], cwd=self.root
        ).decode().strip()
        subprocess.run(["git", "checkout", "-q", "-b", "approved-v2"], cwd=self.root, check=True)
        write_approved(
            "Legitimately approved v2 bytes.",
            "1.1",
            "decision-v2",
            "2026-08-27T11:00:00Z",
        )
        subprocess.run(["git", "add", "."], cwd=self.root, check=True)
        subprocess.run(["git", "commit", "-q", "-m", "approved-v2"], cwd=self.root, check=True)
        subprocess.run(["git", "checkout", "-q", baseline_branch], cwd=self.root, check=True)
        subprocess.run(
            ["git", "merge", "-q", "--no-ff", "--no-commit", "approved-v2"],
            cwd=self.root,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        write_approved(
            "Unapproved merge bytes reusing v2 evidence.",
            "1.2",
            "decision-v2",
            "2026-08-27T11:00:00Z",
        )
        subprocess.run(["git", "add", "."], cwd=self.root, check=True)
        subprocess.run(["git", "commit", "-q", "-m", "merge-resolution"], cwd=self.root, check=True)
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.root).decode().strip()
        source_entries = {
            "governance/aims/manifest.json": (
                self.root / "governance" / "aims" / "manifest.json"
            ).read_bytes(),
            "governance/aims/records/policy.md": document.read_bytes(),
        }

        with self.assertRaisesRegex(VerificationError, "lacks new approval evidence"):
            _verify_aims_history(self.root, baseline, commit, source_entries)

    def test_history_walk_rejects_intermediate_non_regular_git_mode(self) -> None:
        subprocess.run(["git", "init", "-q"], cwd=self.root, check=True)
        subprocess.run(
            ["git", "config", "core.autocrlf", "false"], cwd=self.root, check=True
        )
        subprocess.run(
            ["git", "config", "user.email", "tests@agentos.invalid"],
            cwd=self.root,
            check=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Agent OS Tests"], cwd=self.root, check=True
        )
        manifest_path = "governance/aims/manifest.json"
        subprocess.run(["git", "add", manifest_path], cwd=self.root, check=True)
        subprocess.run(["git", "commit", "-q", "-m", "baseline"], cwd=self.root, check=True)
        baseline = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.root).decode().strip()
        blob = subprocess.check_output(
            ["git", "rev-parse", f"HEAD:{manifest_path}"], cwd=self.root
        ).decode().strip()
        subprocess.run(
            ["git", "update-index", "--cacheinfo", "120000", blob, manifest_path],
            cwd=self.root,
            check=True,
        )
        subprocess.run(["git", "commit", "-q", "-m", "symlink-mode"], cwd=self.root, check=True)
        subprocess.run(
            ["git", "update-index", "--cacheinfo", "100644", blob, manifest_path],
            cwd=self.root,
            check=True,
        )
        subprocess.run(["git", "commit", "-q", "-m", "restore-mode"], cwd=self.root, check=True)
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.root).decode().strip()
        source_entries = {
            manifest_path: (self.root / "governance" / "aims" / "manifest.json").read_bytes()
        }

        with self.assertRaisesRegex(VerificationError, "regular non-executable"):
            _verify_aims_history(self.root, baseline, commit, source_entries)

    def test_git_snapshot_rejects_oversized_blob_before_materializing_it(self) -> None:
        object_id = "a" * 40
        listing = (
            f"100644 blob {object_id} {MAX_DOCUMENT_BYTES + 1}"
            "\tgovernance/aims/records/oversized.md\0"
        ).encode()
        with patch(
            "scripts.build_aims_assessment_bundle._git", return_value=listing
        ) as git:
            with self.assertRaisesRegex(VerificationError, "blob exceeds"):
                _git_aims_entries(self.root, "b" * 40)
        git.assert_called_once()

    def test_rejects_controlled_file_hidden_from_captured_worktree(self) -> None:
        manifest = (self.root / "governance" / "aims" / "manifest.json").read_bytes()
        captured = {"governance/aims/manifest.json": manifest}
        committed = {
            **captured,
            "governance/aims/records/unlisted.md": b"# Unlisted\n",
        }
        with patch(
            "scripts.build_aims_assessment_bundle._git_aims_entries",
            return_value=committed,
        ):
            with self.assertRaisesRegex(VerificationError, "missing=.*unlisted"):
                _verify_controlled_commit_snapshot(
                    self.root, "a" * 40, captured
                )


if __name__ == "__main__":
    unittest.main()
