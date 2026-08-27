#!/usr/bin/env python3
"""Build a deterministic, public Agent OS AIMS assessment-evidence bundle."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import re
import subprocess
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

try:
    from scripts.verify_aims_documents import VerificationError, verify, verify_history_bytes
except ModuleNotFoundError:
    from verify_aims_documents import VerificationError, verify, verify_history_bytes


MAX_SOURCE_FILE_BYTES = 512 * 1024
MAX_SOURCE_TOTAL_BYTES = 4 * 1024 * 1024
MAX_ARCHIVE_BYTES = 5 * 1024 * 1024
MAX_HISTORY_COMMITS = 512
COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
EVIDENCE_FILES = {
    "README.md": "product-purpose-and-use",
    "SECURITY.md": "security-reporting",
    "docs/AI_MANAGEMENT_SYSTEM.md": "aims-technical-evidence-boundary",
    "docs/APPROVAL_CONTROL.md": "user-authority",
    "docs/EVENT_LEDGER_INTEGRITY.md": "evidence-integrity",
    "docs/GOVERNANCE_INSPECTION.md": "monitoring-and-inspection",
    "docs/INCIDENT_REPLAY.md": "incident-investigation",
    "docs/LAB.md": "governed-experimentation",
    "docs/ORGANIZATIONAL_KNOWLEDGE.md": "organizational-memory",
    "docs/SHARED_COORDINATION.md": "shared-coordination",
    "docs/SQLITE_RECOVERY.md": "resilience-and-recovery",
    "docs/THREAT_MODEL.md": "ai-and-security-risk",
    "docs/development/BUILD_CONTRACT.md": "lifecycle-controls",
    "docs/development/ISO_IEC_42001_READINESS.md": "readiness-register",
    "docs/development/V1_ACCEPTANCE_STATUS.md": "acceptance-evidence",
}
REQUIRED_APPROVED_DOCUMENTS = {
    "aims.ai-policy",
    "aims.assessment-readiness-decision",
    "aims.audit-result",
    "aims.competence-communication",
    "aims.control-applicability",
    "aims.document-control",
    "aims.incident-corrective-action",
    "aims.internal-audit",
    "aims.management-review",
    "aims.management-review-result",
    "aims.objectives",
    "aims.risk-impact",
    "aims.roles-accountability",
    "aims.scope-context",
    "aims.statement-of-applicability",
    "aims.supplier-management",
}


def _canonical_json(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _read_public_file(repo_root: Path, relative: str) -> bytes:
    path = repo_root.joinpath(*PurePosixPath(relative).parts)
    current = repo_root
    for part in PurePosixPath(relative).parts:
        current = current / part
        if current.is_symlink():
            raise VerificationError(f"assessment evidence traverses a symbolic link: {relative}")
    try:
        path.resolve(strict=True).relative_to(repo_root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise VerificationError(f"assessment evidence is missing or escapes the repository: {relative}") from exc
    if not path.is_file():
        raise VerificationError(f"assessment evidence is not a regular file: {relative}")
    data = path.read_bytes()
    if len(data) > MAX_SOURCE_FILE_BYTES:
        raise VerificationError(f"assessment evidence exceeds {MAX_SOURCE_FILE_BYTES} bytes: {relative}")
    return data


def _git(repo_root: Path, arguments: list[str]) -> bytes:
    environment = os.environ.copy()
    environment["GIT_NO_REPLACE_OBJECTS"] = "1"
    for name in (
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    ):
        environment.pop(name, None)
    try:
        result = subprocess.run(
            ["git", "--no-optional-locks", "-C", str(repo_root), *arguments],
            check=False,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise VerificationError(f"cannot verify assessment source with Git: {exc}") from exc
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise VerificationError(f"Git source verification failed: {detail}")
    return result.stdout


def _verify_git_sources(repo_root: Path, commit: str, source_entries: dict[str, bytes]) -> None:
    head = _git(repo_root, ["rev-parse", "HEAD"]).decode("ascii", errors="strict").strip()
    if head != commit:
        raise VerificationError(f"declared commit {commit} does not equal checked-out HEAD {head}")
    for relative, data in source_entries.items():
        committed = _git(repo_root, ["show", f"{commit}:{relative}"])
        if committed != data:
            raise VerificationError(f"assessment evidence does not match declared commit: {relative}")


def _verify_captured_aims_snapshot(
    source_entries: dict[str, bytes],
    prior_manifest: bytes | None,
    *,
    enforce_display_status: bool = True,
) -> dict[str, Any]:
    controlled_prefix = "governance/aims/records/"
    controlled_paths = {
        path: data
        for path, data in source_entries.items()
        if path == "governance/aims/manifest.json" or path.startswith(controlled_prefix)
    }
    if "governance/aims/manifest.json" not in controlled_paths:
        raise VerificationError("captured assessment source lacks the AIMS manifest")
    with tempfile.TemporaryDirectory(prefix="agentos-aims-snapshot-") as directory:
        snapshot_root = Path(directory)
        for relative, data in controlled_paths.items():
            path = PurePosixPath(relative)
            if path.is_absolute() or ".." in path.parts or relative != path.as_posix():
                raise VerificationError(f"unsafe captured AIMS path: {relative}")
            destination = snapshot_root.joinpath(*path.parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(data)
        return verify(
            snapshot_root,
            prior_manifest_bytes=prior_manifest,
            enforce_display_status=enforce_display_status,
        )


def _captured_aims_entries(source_entries: dict[str, bytes]) -> dict[str, bytes]:
    return {
        path: data
        for path, data in source_entries.items()
        if path == "governance/aims/manifest.json"
        or path.startswith("governance/aims/records/")
    }


def _git_aims_entries(repo_root: Path, commit: str) -> dict[str, bytes]:
    listing = _git(
        repo_root,
        [
            "ls-tree",
            "-r",
            "--name-only",
            commit,
            "--",
            "governance/aims/manifest.json",
            "governance/aims/records",
        ],
    )
    try:
        paths = listing.decode("utf-8", errors="strict").splitlines()
    except UnicodeDecodeError as exc:
        raise VerificationError("Git AIMS path listing is not valid UTF-8") from exc
    entries: dict[str, bytes] = {}
    for relative in paths:
        path = PurePosixPath(relative)
        if (
            not relative
            or path.is_absolute()
            or ".." in path.parts
            or relative != path.as_posix()
            or (
                relative != "governance/aims/manifest.json"
                and not relative.startswith("governance/aims/records/")
            )
        ):
            raise VerificationError(f"unsafe Git AIMS path: {relative}")
        entries[relative] = _git(repo_root, ["show", f"{commit}:{relative}"])
    if entries and "governance/aims/manifest.json" not in entries:
        raise VerificationError(f"AIMS records exist without a manifest at commit {commit}")
    return entries


def _verify_aims_history(
    repo_root: Path,
    baseline: str,
    commit: str,
    source_entries: dict[str, bytes],
) -> dict[str, Any]:
    if not COMMIT_PATTERN.fullmatch(baseline):
        raise VerificationError("history baseline must be an exact lowercase 40-character Git commit SHA")
    merge_base = _git(repo_root, ["merge-base", baseline, commit]).decode(
        "ascii", errors="strict"
    ).strip()
    if merge_base != baseline:
        raise VerificationError("history baseline is not an ancestor of the assessed commit")
    revisions = _git(
        repo_root,
        [
            "rev-list",
            "--ancestry-path",
            "--topo-order",
            "--reverse",
            f"{baseline}..{commit}",
        ],
    ).decode("ascii", errors="strict").splitlines()
    if not revisions or revisions[-1] != commit:
        raise VerificationError("assessed commit is not reachable after the history baseline")
    if len(revisions) > MAX_HISTORY_COMMITS:
        raise VerificationError(f"AIMS history exceeds {MAX_HISTORY_COMMITS} commits")

    entries_by_commit = {baseline: _git_aims_entries(repo_root, baseline)}
    baseline_manifest = entries_by_commit[baseline].get("governance/aims/manifest.json")
    if baseline_manifest is not None:
        _verify_captured_aims_snapshot(
            entries_by_commit[baseline], None, enforce_display_status=False
        )

    final_manifest: dict[str, Any] | None = None
    for revision in revisions:
        parents = _git(repo_root, ["rev-list", "--parents", "-n", "1", revision]).decode(
            "ascii", errors="strict"
        ).split()
        if not parents or parents[0] != revision:
            raise VerificationError(f"invalid parent listing in trusted AIMS history: {revision}")
        trusted_parents = [parent for parent in parents[1:] if parent in entries_by_commit]
        if not trusted_parents:
            raise VerificationError(
                f"no parent falls inside the trusted AIMS history walk: {revision}"
            )
        entries = (
            _captured_aims_entries(source_entries)
            if revision == commit
            else _git_aims_entries(repo_root, revision)
        )
        entries_by_commit[revision] = entries
        manifest_bytes = entries.get("governance/aims/manifest.json")
        if manifest_bytes is None:
            if any(
                entries_by_commit[parent].get("governance/aims/manifest.json") is not None
                for parent in trusted_parents
            ):
                raise VerificationError(f"AIMS manifest was removed at commit {revision}")
            continue
        final_manifest = _verify_captured_aims_snapshot(
            entries,
            None,
            enforce_display_status=revision == commit,
        )
        for parent in trusted_parents:
            prior_manifest = entries_by_commit[parent].get(
                "governance/aims/manifest.json"
            )
            if prior_manifest is not None:
                verify_history_bytes(
                    prior_manifest,
                    manifest_bytes,
                    enforce_draft_version=revision == commit,
                )
    if final_manifest is None:
        raise VerificationError("assessed history does not contain an AIMS manifest")
    return final_manifest


def _verify_controlled_commit_snapshot(
    repo_root: Path, commit: str, source_entries: dict[str, bytes]
) -> None:
    committed = _git_aims_entries(repo_root, commit)
    captured = _captured_aims_entries(source_entries)
    if set(committed) != set(captured):
        missing = sorted(set(committed) - set(captured))
        extra = sorted(set(captured) - set(committed))
        raise VerificationError(
            f"captured AIMS paths differ from the assessed commit; missing={missing}, extra={extra}"
        )
    for path, data in captured.items():
        if committed[path] != data:
            raise VerificationError(f"captured AIMS bytes differ from the assessed commit: {path}")


def _verified_commit_at(repo_root: Path, commit: str, assessment_at: datetime) -> datetime:
    value = _git(repo_root, ["show", "-s", "--format=%cI", commit]).decode(
        "ascii", errors="strict"
    ).strip()
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise VerificationError("source commit has an invalid committer timestamp") from exc
    if parsed.tzinfo is None:
        raise VerificationError("source commit timestamp lacks a UTC offset")
    commit_at = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    if commit_at > assessment_at:
        raise VerificationError("source commit postdates the assessment instant")
    return commit_at


def _effective_document(document_id: str, documents_by_id: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    seen: set[str] = set()
    current = documents_by_id.get(document_id)
    while current is not None and current["status"] == "RETIRED":
        if current["id"] in seen:
            raise VerificationError(f"controlled successor cycle: {document_id}")
        seen.add(current["id"])
        current = documents_by_id.get(current["superseded_by"])
    return current


def readiness_report(
    manifest: dict[str, Any],
    commit: str,
    assessment_at: datetime,
    commit_at: datetime | None = None,
    history_baseline: str | None = None,
) -> dict[str, Any]:
    documents = manifest["documents"]
    documents_by_id = {entry["id"]: entry for entry in documents}
    missing = sorted(REQUIRED_APPROVED_DOCUMENTS - set(documents_by_id))
    effective = {
        document_id: _effective_document(document_id, documents_by_id)
        for document_id in REQUIRED_APPROVED_DOCUMENTS & set(documents_by_id)
    }
    not_approved = sorted(
        document_id
        for document_id, document in effective.items()
        if document is None or document["status"] != "APPROVED"
    )
    overdue = sorted(
        document_id
        for document_id, document in effective.items()
        if document is not None
        and document["status"] == "APPROVED"
        and datetime.strptime(document["review_due"], "%Y-%m-%d").date() < assessment_at.date()
    )
    postdated = sorted(
        document_id
        for document_id, document in effective.items()
        if document is not None
        and document["status"] == "APPROVED"
        and datetime.strptime(document["approved_at"], "%Y-%m-%dT%H:%M:%SZ") > assessment_at
    )
    blockers: list[str] = []
    if missing:
        blockers.append("REQUIRED_GOVERNANCE_RECORDS_MISSING")
    if not_approved:
        blockers.append("REQUIRED_GOVERNANCE_RECORDS_NOT_APPROVED")
    if overdue:
        blockers.append("CONTROLLED_REVIEWS_OVERDUE")
    if postdated:
        blockers.append("APPROVALS_POSTDATE_ASSESSMENT")
    if any(entry["status"] == "DRAFT" for entry in documents):
        blockers.append("CONTROLLED_DRAFTS_REMAIN")
    if "aims.audit-result" in missing:
        blockers.append("INTERNAL_AUDIT_RESULT_NOT_RECORDED")
    if "aims.management-review-result" in missing:
        blockers.append("MANAGEMENT_REVIEW_RESULT_NOT_RECORDED")
    if "aims.statement-of-applicability" in missing:
        blockers.append("AUTHORIZED_STANDARD_CONTROL_REVIEW_NOT_RECORDED")
    if "aims.assessment-readiness-decision" in missing:
        blockers.append("ACCOUNTABLE_READINESS_DECISION_NOT_RECORDED")

    return {
        "schema_version": 1,
        "project": "Agent OS",
        "source": {
            "repository": None,
            "commit": commit,
            "commit_at": (
                commit_at.strftime("%Y-%m-%dT%H:%M:%SZ")
                if commit_at is not None
                else None
            ),
            "history_baseline": history_baseline,
            "binding": "LOCAL_GIT_OBJECTS_VERIFIED_REPOSITORY_IDENTITY_UNAUTHENTICATED",
        },
        "assessment_at": assessment_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "claim": "READINESS_WORK_IN_PROGRESS",
        "conformity_determined": False,
        "certified": False,
        "certification_assessment_ready": not blockers,
        "controlled_document_counts": {
            status: sum(entry["status"] == status for entry in documents)
            for status in ("DRAFT", "APPROVED", "RETIRED")
        },
        "required_approved_document_ids": sorted(REQUIRED_APPROVED_DOCUMENTS),
        "missing_required_document_ids": missing,
        "not_approved_required_document_ids": not_approved,
        "overdue_required_document_ids": overdue,
        "postdated_approval_document_ids": postdated,
        "blockers": blockers,
        "limitations": [
            "This automated report validates bounded repository evidence and controlled-document state only.",
            "It does not evaluate confidential operating evidence or reproduce ISO/IEC 42001 requirements.",
            "Repository identity requires separately authenticated provenance or attestation.",
            "The history baseline must be trusted through reviewed branch controls or separate attestation.",
            "A Git committer timestamp is not an independently trusted time source.",
            "A true readiness state still does not determine conformity or certification.",
        ],
    }


def _tar_bytes(entries: dict[str, bytes]) -> bytes:
    tar_buffer = io.BytesIO()
    try:
        with tarfile.open(fileobj=tar_buffer, mode="w", format=tarfile.PAX_FORMAT) as archive:
            for name in sorted(entries):
                path = PurePosixPath(name)
                if path.is_absolute() or ".." in path.parts or name != path.as_posix():
                    raise VerificationError(f"unsafe assessment archive path: {name}")
                data = entries[name]
                info = tarfile.TarInfo(name)
                info.size = len(data)
                info.mode = 0o644
                info.uid = 0
                info.gid = 0
                info.uname = ""
                info.gname = ""
                info.mtime = 0
                archive.addfile(info, io.BytesIO(data))
    except (tarfile.TarError, ValueError) as exc:
        raise VerificationError(f"cannot encode assessment archive: {exc}") from exc
    gzip_buffer = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=gzip_buffer, compresslevel=9, mtime=0) as compressed:
        compressed.write(tar_buffer.getvalue())
    return gzip_buffer.getvalue()


def build(
    repo_root: Path,
    commit: str,
    assessment_at: datetime,
    output: Path,
    *,
    verify_source: bool = True,
    history_baseline: str | None = None,
) -> tuple[str, int]:
    repo_root = repo_root.resolve(strict=True)
    if not COMMIT_PATTERN.fullmatch(commit):
        raise VerificationError("commit must be an exact lowercase 40-character Git commit SHA")
    manifest = verify(repo_root)

    source_paths = dict(EVIDENCE_FILES)
    source_paths["governance/aims/README.md"] = "controlled-documentation-boundary"
    source_paths["governance/aims/manifest.json"] = "controlled-document-manifest"
    for entry in manifest["documents"]:
        source_paths[entry["path"]] = "controlled-aims-document"

    entries: dict[str, bytes] = {}
    source_entries: dict[str, bytes] = {}
    evidence_index: list[dict[str, Any]] = []
    source_total = 0
    for relative in sorted(source_paths):
        data = _read_public_file(repo_root, relative)
        source_total += len(data)
        if source_total > MAX_SOURCE_TOTAL_BYTES:
            raise VerificationError(f"assessment source evidence exceeds {MAX_SOURCE_TOTAL_BYTES} aggregate bytes")
        archive_name = f"repository/{relative}"
        entries[archive_name] = data
        source_entries[relative] = data
        evidence_index.append(
            {
                "path": relative,
                "category": source_paths[relative],
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        )

    if verify_source:
        if history_baseline is None:
            raise VerificationError("history baseline is required for Git-verified assessment bundles")
        _verify_git_sources(repo_root, commit, source_entries)
        _verify_controlled_commit_snapshot(repo_root, commit, source_entries)
        commit_at = _verified_commit_at(repo_root, commit, assessment_at)
        manifest = _verify_aims_history(
            repo_root, history_baseline, commit, source_entries
        )
    else:
        commit_at = None

    entries["assessment/evidence-index.json"] = _canonical_json(
        {
            "schema_version": 1,
            "source": {
                "repository": None,
                "commit": commit,
                "commit_at": (
                    commit_at.strftime("%Y-%m-%dT%H:%M:%SZ")
                    if commit_at is not None
                    else None
                ),
                "history_baseline": history_baseline,
                "binding": "LOCAL_GIT_OBJECTS_VERIFIED_REPOSITORY_IDENTITY_UNAUTHENTICATED",
            },
            "entries": evidence_index,
        }
    )
    entries["assessment/readiness.json"] = _canonical_json(
        readiness_report(
            manifest,
            commit,
            assessment_at,
            commit_at,
            history_baseline,
        )
    )
    entries["ASSESSMENT_README.txt"] = (
        "Agent OS public AIMS assessment-evidence bundle\n\n"
        "This deterministic bundle contains bounded public repository evidence for the exact source commit.\n"
        "The local Git object check does not authenticate repository identity; use separate trusted provenance.\n"
        "It does not contain confidential operating evidence, determine conformity, or establish certification.\n"
        "Use an approved AIMS scope, an authorized ISO/IEC 42001 copy, and competent independent assessment.\n"
    ).encode("utf-8")

    checksum_lines = [f"{hashlib.sha256(entries[name]).hexdigest()}  {name}" for name in sorted(entries)]
    entries["SHA256SUMS"] = ("\n".join(checksum_lines) + "\n").encode("ascii")
    archive = _tar_bytes(entries)
    if len(archive) > MAX_ARCHIVE_BYTES:
        raise VerificationError(f"assessment archive exceeds {MAX_ARCHIVE_BYTES} bytes")
    output = output if output.is_absolute() else repo_root / output
    output = output.resolve(strict=False)
    allowed_root = (repo_root / "work").resolve(strict=False)
    try:
        output.relative_to(allowed_root)
    except ValueError as exc:
        raise VerificationError("assessment output must be below the repository work directory") from exc
    if output.suffixes[-2:] != [".tar", ".gz"]:
        raise VerificationError("assessment output must use a .tar.gz filename")
    if output.exists() or output.is_symlink():
        raise VerificationError("assessment output already exists and will not be overwritten")
    current = repo_root
    for part in output.relative_to(repo_root).parts[:-1]:
        current = current / part
        if current.is_symlink():
            raise VerificationError("assessment output path traverses a symbolic link")
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with output.open("xb") as destination:
            destination.write(archive)
    except FileExistsError as exc:
        raise VerificationError("assessment output already exists and will not be overwritten") from exc
    return hashlib.sha256(archive).hexdigest(), len(archive)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--commit", required=True)
    parser.add_argument("--assessment-at", required=True)
    parser.add_argument("--history-baseline", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if not re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z",
            args.assessment_at,
        ):
            raise VerificationError("assessment-at must use exact RFC 3339 UTC whole-second syntax")
        assessment_at = datetime.strptime(args.assessment_at, "%Y-%m-%dT%H:%M:%SZ")
        digest, size = build(
            args.root,
            args.commit,
            assessment_at,
            args.output,
            history_baseline=args.history_baseline,
        )
    except VerificationError as exc:
        print(f"AIMS assessment bundle failed: {exc}")
        return 1
    print(f"{digest}  {args.output} ({size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
