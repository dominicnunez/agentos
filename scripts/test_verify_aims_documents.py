#!/usr/bin/env python3
"""Tests for the controlled AIMS documented-information verifier."""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts.verify_aims_documents import VerificationError, verify


class VerifyAIMSDocumentsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.records = self.root / "governance" / "aims" / "records"
        self.records.mkdir(parents=True)
        self.document = self.records / "scope.md"
        self.document.write_text("# Scope\n\nDraft.\n", encoding="utf-8", newline="\n")

    def manifest(self, **changes: object) -> dict[str, object]:
        entry: dict[str, object] = {
            "id": "aims.scope",
            "path": "governance/aims/records/scope.md",
            "version": "0.1",
            "status": "DRAFT",
            "owner": "project-leadership",
            "classification": "PUBLIC",
            "approval_ref": None,
            "approved_by": None,
            "approved_at": None,
            "review_due": None,
            "supersedes": [],
            "superseded_by": None,
            "sha256": hashlib.sha256(self.document.read_bytes()).hexdigest(),
        }
        entry.update(changes)
        return {"schema_version": 1, "documents": [entry]}

    def write_manifest(self, manifest: dict[str, object]) -> Path:
        path = self.root / "governance" / "aims" / "manifest.json"
        path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8", newline="\n")
        return path

    def assert_rejected(self, manifest: dict[str, object], phrase: str) -> None:
        path = self.write_manifest(manifest)
        with self.assertRaisesRegex(VerificationError, phrase):
            verify(self.root, path)

    def test_accepts_exact_draft(self) -> None:
        verify(self.root, self.write_manifest(self.manifest()))

    def test_rejects_hash_mismatch(self) -> None:
        self.assert_rejected(self.manifest(sha256="0" * 64), "does not match")

    def test_rejects_draft_approval_claim(self) -> None:
        self.assert_rejected(self.manifest(approval_ref="issue-1"), "must not contain approval")

    def test_rejects_unknown_metadata(self) -> None:
        manifest = self.manifest()
        manifest["documents"][0]["extra"] = True  # type: ignore[index]
        self.assert_rejected(manifest, "keys are not closed")

    def test_rejects_boolean_schema_version(self) -> None:
        manifest = self.manifest()
        manifest["schema_version"] = True
        self.assert_rejected(manifest, "must equal 1")

    def test_rejects_duplicate_json_keys(self) -> None:
        path = self.root / "governance" / "aims" / "manifest.json"
        path.write_text(
            '{"schema_version":1,"schema_version":1,"documents":[]}\n',
            encoding="utf-8",
            newline="\n",
        )
        with self.assertRaisesRegex(VerificationError, "duplicate JSON key"):
            verify(self.root, path)

    def test_rejects_nonstandard_json_constant(self) -> None:
        path = self.root / "governance" / "aims" / "manifest.json"
        path.write_text(
            '{"schema_version":NaN,"documents":[]}\n',
            encoding="utf-8",
            newline="\n",
        )
        with self.assertRaisesRegex(VerificationError, "non-standard JSON constant"):
            verify(self.root, path)

    def test_rejects_path_escape(self) -> None:
        self.assert_rejected(self.manifest(path="governance/aims/records/../scope.md"), "allowlist")

    def test_rejects_carriage_returns(self) -> None:
        self.document.write_bytes(b"# Scope\r\n")
        manifest = self.manifest(sha256=hashlib.sha256(self.document.read_bytes()).hexdigest())
        self.assert_rejected(manifest, "LF line endings")

    def test_rejects_approved_placeholders(self) -> None:
        self.document.write_text("# Scope\n\nTODO decide.\n", encoding="utf-8", newline="\n")
        manifest = self.manifest(
            status="APPROVED",
            approval_ref="decision-1",
            approved_by="project-owner",
            approved_at="2026-08-27T12:00:00Z",
            review_due="2027-08-27",
            sha256=hashlib.sha256(self.document.read_bytes()).hexdigest(),
        )
        self.assert_rejected(manifest, "unresolved placeholder")

    def test_rejects_retirement_without_known_successor(self) -> None:
        manifest = self.manifest(
            status="RETIRED",
            approval_ref="decision-2",
            approved_by="project-owner",
            approved_at="2026-08-27T12:00:00Z",
            review_due="2027-08-27",
            superseded_by="aims.scope-v2",
        )
        self.assert_rejected(manifest, "unknown document")

    def test_rejects_unlisted_controlled_file(self) -> None:
        (self.records / "unlisted.md").write_text("# Unlisted\n", encoding="utf-8", newline="\n")
        self.assert_rejected(self.manifest(), "unlisted file")

    def test_rejects_review_due_on_approval_date(self) -> None:
        manifest = self.manifest(
            status="APPROVED",
            approval_ref="decision-3",
            approved_by="project-owner",
            approved_at="2026-08-27T12:00:00Z",
            review_due="2026-08-27",
        )
        self.assert_rejected(manifest, "must be after")


if __name__ == "__main__":
    unittest.main()
