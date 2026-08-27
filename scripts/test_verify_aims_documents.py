#!/usr/bin/env python3
"""Tests for the controlled AIMS documented-information verifier."""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.verify_aims_documents import (
    MAX_DOCUMENT_BYTES,
    VerificationError,
    _parse_manifest_bytes,
    _read_controlled_bytes,
    verify,
    verify_history_bytes,
)


class VerifyAIMSDocumentsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.records = self.root / "governance" / "aims" / "records"
        self.records.mkdir(parents=True)
        self.document = self.records / "scope.md"
        self.write_document("DRAFT", "Draft.\n")

    def write_document(self, status: str, body: str) -> None:
        self.document.write_text(
            f"# Scope\n\nStatus: **{status}**\n\n{body}", encoding="utf-8", newline="\n"
        )

    def test_controlled_file_limit_precedes_unbounded_materialization(self) -> None:
        self.document.write_bytes(b"x" * (MAX_DOCUMENT_BYTES + 1))
        with patch.object(
            Path,
            "read_bytes",
            side_effect=AssertionError("unbounded read_bytes must not be used"),
        ):
            with self.assertRaisesRegex(VerificationError, "controlled AIMS document exceeds"):
                _read_controlled_bytes(
                    self.document, MAX_DOCUMENT_BYTES, "controlled AIMS document"
                )

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

    def write_schema_two_outcomes(
        self,
        *,
        audit_record_result: str = "PASS",
        audit_successor: bool = False,
    ) -> Path:
        self.document.unlink()
        outcomes = self.affirmative_outcomes()
        records = {
            "aims.audit-result": ("audit", outcomes["audit"]),
            "aims.management-review-result": (
                "management_review",
                outcomes["management_review"],
            ),
            "aims.statement-of-applicability": (
                "statement_of_applicability",
                outcomes["statement_of_applicability"],
            ),
            "aims.assessment-readiness-decision": (
                "readiness_decision",
                outcomes["readiness_decision"],
            ),
        }
        documents: list[dict[str, object]] = []
        for document_id, (outcome_name, outcome) in records.items():
            record_outcome = json.loads(json.dumps(outcome))
            if outcome_name == "audit":
                record_outcome["result"] = audit_record_result
            path = f"governance/aims/records/{document_id.removeprefix('aims.')}.json"
            data = (
                json.dumps(
                    {
                        "schema_version": 1,
                        "lifecycle_status": "APPROVED",
                        "outcome": record_outcome,
                    },
                    indent=2,
                )
                + "\n"
            ).encode()
            self.root.joinpath(*path.split("/")).write_bytes(data)
            documents.append(
                {
                    "id": document_id,
                    "path": path,
                    "version": "1.0",
                    "status": "APPROVED",
                    "owner": "aims-manager",
                    "classification": "PUBLIC",
                    "approval_ref": f"decision-{document_id}",
                    "approved_by": "project-owner",
                    "approved_at": "2026-08-27T10:00:00Z",
                    "review_due": "2027-08-27",
                    "supersedes": [],
                    "superseded_by": None,
                    "sha256": hashlib.sha256(data).hexdigest(),
                }
            )
        if audit_successor:
            root = next(document for document in documents if document["id"] == "aims.audit-result")
            root["version"] = "1.1"
            root["status"] = "RETIRED"
            root["approval_ref"] = "decision-retire-audit-v1"
            root["approved_at"] = "2026-08-27T11:00:00Z"
            root["superseded_by"] = "aims.audit-result-v2"
            root_record = {
                "schema_version": 1,
                "lifecycle_status": "RETIRED",
                "outcome": outcomes["audit"],
            }
            root_data = (json.dumps(root_record, indent=2) + "\n").encode()
            self.root.joinpath(*str(root["path"]).split("/")).write_bytes(root_data)
            root["sha256"] = hashlib.sha256(root_data).hexdigest()

            successor_outcome = json.loads(json.dumps(outcomes["audit"]))
            successor_outcome["document_id"] = "aims.audit-result-v2"
            outcomes["audit"] = successor_outcome
            successor_path = "governance/aims/records/audit-result-v2.json"
            successor_data = (
                json.dumps(
                    {
                        "schema_version": 1,
                        "lifecycle_status": "APPROVED",
                        "outcome": successor_outcome,
                    },
                    indent=2,
                )
                + "\n"
            ).encode()
            self.root.joinpath(*successor_path.split("/")).write_bytes(successor_data)
            documents.append(
                {
                    "id": "aims.audit-result-v2",
                    "path": successor_path,
                    "version": "1.0",
                    "status": "APPROVED",
                    "owner": "aims-manager",
                    "classification": "PUBLIC",
                    "approval_ref": "decision-audit-v2",
                    "approved_by": "project-owner",
                    "approved_at": "2026-08-27T11:00:00Z",
                    "review_due": "2027-08-27",
                    "supersedes": ["aims.audit-result"],
                    "superseded_by": None,
                    "sha256": hashlib.sha256(successor_data).hexdigest(),
                }
            )
        manifest = {
            "schema_version": 2,
            "documents": sorted(documents, key=lambda document: str(document["id"])),
            "assessment_outcomes": outcomes,
        }
        return self.write_manifest(manifest)

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

    def test_rejects_structured_json_before_schema_two(self) -> None:
        structured = self.records / "scope.json"
        structured.write_text(
            '{"schema_version":1,"lifecycle_status":"DRAFT","outcome":{}}\n',
            encoding="utf-8",
            newline="\n",
        )
        manifest = self.manifest(
            path="governance/aims/records/scope.json",
            sha256=hashlib.sha256(structured.read_bytes()).hexdigest(),
        )
        self.assert_rejected(manifest, "requires manifest schema 2")

    def test_accepts_closed_machine_readable_outcome_schema(self) -> None:
        manifest = {
            "schema_version": 2,
            "documents": [],
            "assessment_outcomes": self.affirmative_outcomes(),
        }
        parsed = _parse_manifest_bytes(
            (json.dumps(manifest) + "\n").encode(), "test manifest"
        )
        self.assertEqual(parsed["assessment_outcomes"], manifest["assessment_outcomes"])

    def test_accepts_outcomes_bound_to_structured_approved_records(self) -> None:
        verify(self.root, self.write_schema_two_outcomes())

    def test_rejects_outcome_that_disagrees_with_hash_bound_record(self) -> None:
        path = self.write_schema_two_outcomes(audit_record_result="FAIL")
        with self.assertRaisesRegex(VerificationError, "does not match its hash-bound"):
            verify(self.root, path)

    def test_accepts_outcome_bound_to_current_approved_successor(self) -> None:
        verify(self.root, self.write_schema_two_outcomes(audit_successor=True))

    def test_rejects_boolean_blocking_finding_count(self) -> None:
        outcomes = self.affirmative_outcomes()
        outcomes["audit"]["open_blocking_findings"] = False  # type: ignore[index]
        manifest = {
            "schema_version": 2,
            "documents": [],
            "assessment_outcomes": outcomes,
        }
        with self.assertRaisesRegex(VerificationError, "must be an integer"):
            _parse_manifest_bytes((json.dumps(manifest) + "\n").encode(), "test manifest")

    def test_rejects_outcome_change_without_new_approved_evidence(self) -> None:
        approved = {
            "status": "APPROVED",
            "version": "1.0",
            "approval_ref": "decision-1",
            "approved_by": "project-owner",
            "approved_at": "2026-08-27T10:00:00Z",
            "review_due": "2027-08-27",
        }
        document_ids = (
            "aims.assessment-readiness-decision",
            "aims.audit-result",
            "aims.management-review-result",
            "aims.statement-of-applicability",
        )
        documents = []
        for document_id in document_ids:
            entry = self.manifest(**approved)["documents"][0]
            entry["id"] = document_id
            documents.append(entry)
        prior = {
            "schema_version": 2,
            "documents": documents,
            "assessment_outcomes": self.affirmative_outcomes(),
        }
        current = json.loads(json.dumps(prior))
        current["assessment_outcomes"]["readiness_decision"]["disposition"] = "NOT_READY"
        with self.assertRaisesRegex(VerificationError, "without newly approved evidence"):
            verify_history_bytes(
                (json.dumps(prior) + "\n").encode(),
                (json.dumps(current) + "\n").encode(),
            )

    def test_historical_snapshot_accepts_legacy_status_presentation_only_when_requested(self) -> None:
        self.document.write_text(
            "# Scope\n\nLifecycle state: DRAFT\n\nDraft.\n",
            encoding="utf-8",
            newline="\n",
        )
        manifest = self.manifest(
            sha256=hashlib.sha256(self.document.read_bytes()).hexdigest()
        )
        path = self.write_manifest(manifest)
        with self.assertRaisesRegex(VerificationError, "displayed lifecycle status"):
            verify(self.root, path)
        verify(self.root, path, enforce_display_status=False)

    def test_historical_transition_requires_draft_version_increment(self) -> None:
        prior = self.manifest(sha256="1" * 64)
        current = self.manifest(sha256="2" * 64)
        with self.assertRaisesRegex(VerificationError, "did not increment"):
            verify_history_bytes(
                (json.dumps(prior) + "\n").encode(),
                (json.dumps(current) + "\n").encode(),
            )

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
        self.assert_rejected(manifest, "must be one of")

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
        self.write_document("APPROVED", "TODO decide.\n")
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
        self.write_document("RETIRED", "Retired.\n")
        manifest = self.manifest(
            status="RETIRED",
            approval_ref="decision-2",
            approved_by="project-owner",
            approved_at="2026-08-27T12:00:00Z",
            review_due="2027-08-27",
            superseded_by="aims.scope-v2",
            sha256=hashlib.sha256(self.document.read_bytes()).hexdigest(),
        )
        self.assert_rejected(manifest, "unknown document")

    def test_rejects_unlisted_controlled_file(self) -> None:
        (self.records / "unlisted.md").write_text("# Unlisted\n", encoding="utf-8", newline="\n")
        self.assert_rejected(self.manifest(), "unlisted file")

    def test_rejects_review_due_on_approval_date(self) -> None:
        self.write_document("APPROVED", "Approved.\n")
        manifest = self.manifest(
            status="APPROVED",
            approval_ref="decision-3",
            approved_by="project-owner",
            approved_at="2026-08-27T12:00:00Z",
            review_due="2026-08-27",
            sha256=hashlib.sha256(self.document.read_bytes()).hexdigest(),
        )
        self.assert_rejected(manifest, "must be after")

    def test_rejects_noncanonical_review_date(self) -> None:
        self.write_document("APPROVED", "Approved.\n")
        manifest = self.manifest(
            status="APPROVED",
            approval_ref="decision-date",
            approved_by="project-owner",
            approved_at="2026-08-27T12:00:00Z",
            review_due="20270827",
            sha256=hashlib.sha256(self.document.read_bytes()).hexdigest(),
        )
        self.assert_rejected(manifest, "exact YYYY-MM-DD")

    def test_rejects_displayed_status_mismatch(self) -> None:
        manifest = self.manifest(
            status="APPROVED",
            approval_ref="decision-4",
            approved_by="project-owner",
            approved_at="2026-08-27T12:00:00Z",
            review_due="2027-08-27",
        )
        self.assert_rejected(manifest, "displayed lifecycle status")

    def test_rejects_removal_of_approved_history(self) -> None:
        prior = self.manifest(
            status="APPROVED",
            approval_ref="decision-5",
            approved_by="project-owner",
            approved_at="2026-08-27T12:00:00Z",
            review_due="2027-08-27",
        )
        self.document.unlink()
        current = {"schema_version": 1, "documents": []}
        path = self.write_manifest(current)
        with self.assertRaisesRegex(VerificationError, "controlled history was removed"):
            verify(self.root, path, prior_manifest_bytes=(json.dumps(prior) + "\n").encode())

    def test_rejects_removal_of_draft_history(self) -> None:
        prior = self.manifest()
        self.document.unlink()
        current = {"schema_version": 1, "documents": []}
        path = self.write_manifest(current)
        with self.assertRaisesRegex(VerificationError, "controlled history was removed"):
            verify(self.root, path, prior_manifest_bytes=(json.dumps(prior) + "\n").encode())

    def test_rejects_changed_approved_history_without_version_increment(self) -> None:
        self.write_document("APPROVED", "Approved.\n")
        approved = {
            "status": "APPROVED",
            "approval_ref": "decision-6",
            "approved_by": "project-owner",
            "approved_at": "2026-08-27T12:00:00Z",
            "review_due": "2027-08-27",
            "sha256": hashlib.sha256(self.document.read_bytes()).hexdigest(),
        }
        prior = self.manifest(**approved)
        current = self.manifest(**approved, owner="security-owner")
        path = self.write_manifest(current)
        with self.assertRaisesRegex(VerificationError, "did not increment"):
            verify(self.root, path, prior_manifest_bytes=(json.dumps(prior) + "\n").encode())

    def test_rejects_changed_draft_history_without_version_increment(self) -> None:
        prior = self.manifest()
        self.write_document("DRAFT", "Revised draft.\n")
        current = self.manifest(sha256=hashlib.sha256(self.document.read_bytes()).hexdigest())
        path = self.write_manifest(current)
        with self.assertRaisesRegex(VerificationError, "did not increment"):
            verify(self.root, path, prior_manifest_bytes=(json.dumps(prior) + "\n").encode())

    def test_rejects_reused_approval_for_changed_bytes(self) -> None:
        self.write_document("APPROVED", "Original approved bytes.\n")
        approval = {
            "status": "APPROVED",
            "approval_ref": "decision-original",
            "approved_by": "project-owner",
            "approved_at": "2026-08-27T12:00:00Z",
            "review_due": "2027-08-27",
        }
        prior = self.manifest(
            **approval, version="1.0", sha256=hashlib.sha256(self.document.read_bytes()).hexdigest()
        )
        self.write_document("APPROVED", "Changed bytes.\n")
        current = self.manifest(
            **approval, version="1.1", sha256=hashlib.sha256(self.document.read_bytes()).hexdigest()
        )
        path = self.write_manifest(current)
        with self.assertRaisesRegex(VerificationError, "lacks new approval evidence"):
            verify(self.root, path, prior_manifest_bytes=(json.dumps(prior) + "\n").encode())

    def test_rejects_approved_document_demotion_to_draft(self) -> None:
        self.write_document("APPROVED", "Approved bytes.\n")
        prior = self.manifest(
            status="APPROVED",
            version="1.0",
            approval_ref="decision-approved",
            approved_by="project-owner",
            approved_at="2026-08-27T12:00:00Z",
            review_due="2027-08-27",
            sha256=hashlib.sha256(self.document.read_bytes()).hexdigest(),
        )
        self.write_document("DRAFT", "Replacement draft bytes.\n")
        current = self.manifest(
            status="DRAFT",
            version="1.1",
            sha256=hashlib.sha256(self.document.read_bytes()).hexdigest(),
        )
        path = self.write_manifest(current)
        with self.assertRaisesRegex(VerificationError, "cannot return to draft"):
            verify(self.root, path, prior_manifest_bytes=(json.dumps(prior) + "\n").encode())

    def test_rejects_malformed_prior_approval_timestamp_as_verification_error(self) -> None:
        self.write_document("APPROVED", "Changed bytes.\n")
        current = self.manifest(
            status="APPROVED",
            version="1.1",
            approval_ref="decision-current",
            approved_by="project-owner",
            approved_at="2026-08-27T13:00:00Z",
            review_due="2027-08-27",
            sha256=hashlib.sha256(self.document.read_bytes()).hexdigest(),
        )
        prior = self.manifest(
            status="APPROVED",
            version="1.0",
            approval_ref="decision-prior",
            approved_by="project-owner",
            approved_at="not-a-timestamp",
            review_due="2027-08-27",
            sha256="0" * 64,
        )
        path = self.write_manifest(current)
        with self.assertRaisesRegex(VerificationError, "timestamp"):
            verify(self.root, path, prior_manifest_bytes=(json.dumps(prior) + "\n").encode())

    def test_rejects_two_successors_claiming_one_retired_document(self) -> None:
        records: list[dict[str, object]] = []
        specifications = (
            ("aims.scope", "scope.md", "RETIRED", [], "aims.scope-v2"),
            ("aims.scope-v2", "scope-v2.md", "APPROVED", ["aims.scope"], None),
            ("aims.scope-v3", "scope-v3.md", "APPROVED", ["aims.scope"], None),
        )
        for document_id, filename, status, supersedes, superseded_by in specifications:
            path = self.records / filename
            path.write_text(
                f"# Scope\n\nStatus: **{status}**\n\nControlled.\n",
                encoding="utf-8",
                newline="\n",
            )
            records.append(
                {
                    "id": document_id,
                    "path": f"governance/aims/records/{filename}",
                    "version": "1.0",
                    "status": status,
                    "owner": "project-leadership",
                    "classification": "PUBLIC",
                    "approval_ref": f"decision-{document_id}",
                    "approved_by": "project-owner",
                    "approved_at": "2026-08-27T12:00:00Z",
                    "review_due": "2027-08-27",
                    "supersedes": supersedes,
                    "superseded_by": superseded_by,
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                }
            )
        manifest = {"schema_version": 1, "documents": records}
        self.assert_rejected(manifest, "is not owned by this successor")

    def test_rejects_successor_from_different_governance_record_role(self) -> None:
        records: list[dict[str, object]] = []
        specifications = (
            ("aims.ai-policy", "policy.md", "RETIRED", [], "aims.audit-result"),
            ("aims.audit-result", "audit.md", "APPROVED", ["aims.ai-policy"], None),
        )
        for document_id, filename, status, supersedes, superseded_by in specifications:
            path = self.records / filename
            path.write_text(
                f"# Record\n\nStatus: **{status}**\n\nControlled.\n",
                encoding="utf-8",
                newline="\n",
            )
            records.append(
                {
                    "id": document_id,
                    "path": f"governance/aims/records/{filename}",
                    "version": "1.0",
                    "status": status,
                    "owner": "project-leadership",
                    "classification": "PUBLIC",
                    "approval_ref": f"decision-{document_id}",
                    "approved_by": "project-owner",
                    "approved_at": "2026-08-27T12:00:00Z",
                    "review_due": "2027-08-27",
                    "supersedes": supersedes,
                    "superseded_by": superseded_by,
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                }
            )
        self.assert_rejected(
            {"schema_version": 1, "documents": records},
            "preserve the governance-record role",
        )

    def test_accepts_multi_generation_successor_chain(self) -> None:
        records: list[dict[str, object]] = []
        specifications = (
            ("aims.scope", "scope.md", "RETIRED", [], "aims.scope-v2"),
            ("aims.scope-v2", "scope-v2.md", "RETIRED", ["aims.scope"], "aims.scope-v3"),
            ("aims.scope-v3", "scope-v3.md", "APPROVED", ["aims.scope-v2"], None),
        )
        for document_id, filename, status, supersedes, superseded_by in specifications:
            path = self.records / filename
            path.write_text(
                f"# Scope\n\nStatus: **{status}**\n\nControlled.\n",
                encoding="utf-8",
                newline="\n",
            )
            records.append(
                {
                    "id": document_id,
                    "path": f"governance/aims/records/{filename}",
                    "version": "1.0",
                    "status": status,
                    "owner": "project-leadership",
                    "classification": "PUBLIC",
                    "approval_ref": f"decision-{document_id}",
                    "approved_by": "project-owner",
                    "approved_at": "2026-08-27T12:00:00Z",
                    "review_due": "2027-08-27",
                    "supersedes": supersedes,
                    "superseded_by": superseded_by,
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                }
            )
        verify(self.root, self.write_manifest({"schema_version": 1, "documents": records}))


if __name__ == "__main__":
    unittest.main()

