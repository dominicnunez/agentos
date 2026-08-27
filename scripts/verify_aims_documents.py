#!/usr/bin/env python3
"""Verify Agent OS controlled AIMS documented information."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import date, datetime
from pathlib import Path, PurePosixPath
from typing import Any


MAX_MANIFEST_BYTES = 256 * 1024
MAX_DOCUMENT_BYTES = 256 * 1024
MAX_TOTAL_DOCUMENT_BYTES = 2 * 1024 * 1024
MAX_DOCUMENTS = 128
MANIFEST_KEYS = {"schema_version", "documents"}
DOCUMENT_KEYS = {
    "id",
    "path",
    "version",
    "status",
    "owner",
    "classification",
    "approval_ref",
    "approved_by",
    "approved_at",
    "review_due",
    "supersedes",
    "superseded_by",
    "sha256",
}
STATUSES = {"DRAFT", "APPROVED", "RETIRED"}
ID_PATTERN = re.compile(r"^[a-z][a-z0-9.-]{2,63}$")
VERSION_PATTERN = re.compile(r"^(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
PATH_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._/-]*$")
TIMESTAMP_PATTERN = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")
UNRESOLVED_MARKERS = ("TODO", "TBD", "{{", "}}", "<INSERT", "[INSERT")
FORBIDDEN_METADATA_CLAIMS = ("CERTIFIED", "CONFORMANT")
DISPLAYED_STATUS_PATTERN = re.compile(r"^Status: \*\*(DRAFT|APPROVED|RETIRED)\*\*$", re.MULTILINE)


class VerificationError(ValueError):
    """Raised when controlled documented information fails closed validation."""


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise VerificationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise VerificationError(f"non-standard JSON constant: {value}")


def _parse_manifest_bytes(data: bytes, label: str) -> dict[str, Any]:
    try:
        manifest = json.loads(
            data,
            object_pairs_hook=_strict_object,
            parse_constant=_reject_json_constant,
        )
    except (json.JSONDecodeError, VerificationError) as exc:
        raise VerificationError(f"invalid {label} JSON: {exc}") from exc
    if not isinstance(manifest, dict):
        raise VerificationError(f"{label} must be a JSON object")
    _require_exact_keys(manifest, MANIFEST_KEYS, label)
    if type(manifest["schema_version"]) is not int or manifest["schema_version"] != 1:
        raise VerificationError(f"{label}.schema_version must equal 1")
    if not isinstance(manifest["documents"], list) or len(manifest["documents"]) > MAX_DOCUMENTS:
        raise VerificationError(f"{label}.documents must be an array of at most {MAX_DOCUMENTS} entries")
    return manifest


def _read_controlled_bytes(path: Path, limit: int, label: str) -> bytes:
    if path.is_symlink():
        raise VerificationError(f"{label} must not be a symbolic link: {path}")
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise VerificationError(f"cannot read {label}: {path}: {exc}") from exc
    if len(data) > limit:
        raise VerificationError(f"{label} exceeds {limit} bytes: {path}")
    if data.startswith(b"\xef\xbb\xbf"):
        raise VerificationError(f"{label} must not contain a UTF-8 BOM: {path}")
    if b"\r" in data:
        raise VerificationError(f"{label} must use LF line endings: {path}")
    try:
        data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise VerificationError(f"{label} is not valid UTF-8: {path}") from exc
    return data


def _require_exact_keys(value: dict[str, Any], expected: set[str], label: str) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        unknown = sorted(actual - expected)
        raise VerificationError(f"{label} keys are not closed; missing={missing}, unknown={unknown}")


def _require_string(value: Any, label: str, maximum: int = 256) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise VerificationError(f"{label} must be a non-empty trimmed string of at most {maximum} characters")
    return value


def _validate_timestamp(value: str, label: str) -> None:
    if not TIMESTAMP_PATTERN.fullmatch(value):
        raise VerificationError(f"{label} must be an RFC 3339 UTC timestamp with whole seconds")
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise VerificationError(f"{label} is not a valid timestamp") from exc


def _validate_date(value: str, label: str) -> None:
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise VerificationError(f"{label} must be a valid YYYY-MM-DD date") from exc


def _validate_document_path(repo_root: Path, raw_path: Any, label: str) -> Path:
    path_text = _require_string(raw_path, f"{label}.path")
    posix_path = PurePosixPath(path_text)
    if (
        posix_path.is_absolute()
        or ".." in posix_path.parts
        or path_text != posix_path.as_posix()
        or path_text != path_text.lower()
        or not PATH_PATTERN.fullmatch(path_text)
        or not path_text.startswith("governance/aims/records/")
        or posix_path.suffix != ".md"
    ):
        raise VerificationError(f"{label}.path is outside the controlled Markdown allowlist: {path_text}")

    candidate = repo_root.joinpath(*posix_path.parts)
    current = repo_root
    for part in posix_path.parts:
        current = current / part
        if current.is_symlink():
            raise VerificationError(f"{label}.path traverses a symbolic link: {path_text}")
    try:
        candidate.resolve(strict=True).relative_to(repo_root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise VerificationError(f"{label}.path is missing or escapes the repository: {path_text}") from exc
    if not candidate.is_file():
        raise VerificationError(f"{label}.path is not a regular file: {path_text}")
    return candidate


def _version_tuple(value: Any, label: str) -> tuple[int, int]:
    version = _require_string(value, label, 32)
    if not VERSION_PATTERN.fullmatch(version) or version == "0.0":
        raise VerificationError(f"{label} must be a nonzero major.minor version")
    major, minor = version.split(".")
    return int(major), int(minor)


def verify_history(prior_manifest: dict[str, Any], manifest: dict[str, Any]) -> None:
    prior_by_id: dict[str, dict[str, Any]] = {}
    for index, document in enumerate(prior_manifest["documents"]):
        label = f"prior manifest.documents[{index}]"
        if not isinstance(document, dict):
            raise VerificationError(f"{label} must be an object")
        _require_exact_keys(document, DOCUMENT_KEYS, label)
        document_id = _require_string(document["id"], f"{label}.id", 64)
        if not ID_PATTERN.fullmatch(document_id) or document_id in prior_by_id:
            raise VerificationError(f"{label}.id is invalid or duplicated")
        if document["status"] not in STATUSES:
            raise VerificationError(f"{label}.status is invalid")
        _version_tuple(document["version"], f"{label}.version")
        prior_by_id[document_id] = document

    current_by_id = {document["id"]: document for document in manifest["documents"]}
    for document_id, prior in prior_by_id.items():
        current = current_by_id.get(document_id)
        prior_status = prior["status"]
        if current is None:
            if prior_status in {"APPROVED", "RETIRED"}:
                raise VerificationError(f"approved controlled history was removed: {document_id}")
            continue
        current_status = current["status"]
        prior_version = _version_tuple(prior["version"], f"prior {document_id}.version")
        current_version = _version_tuple(current["version"], f"current {document_id}.version")
        if prior_status == "RETIRED" and current != prior:
            raise VerificationError(f"retired controlled history was changed: {document_id}")
        if prior_status == "APPROVED" and current_status == "DRAFT":
            raise VerificationError(f"approved controlled document regressed to DRAFT: {document_id}")
        if prior_status == "DRAFT" and current_status == "RETIRED":
            raise VerificationError(f"draft controlled document cannot be retired directly: {document_id}")
        versioned_change = current != prior and not (
            prior_status == "DRAFT" and current_status == "DRAFT"
        )
        if versioned_change and current_version <= prior_version:
            raise VerificationError(f"changed controlled document did not increment its version: {document_id}")
        if current == prior and current_version != prior_version:
            raise VerificationError(f"unchanged controlled document has inconsistent version history: {document_id}")


def verify(
    repo_root: Path,
    manifest_path: Path | None = None,
    prior_manifest_path: Path | None = None,
    prior_manifest_bytes: bytes | None = None,
) -> dict[str, Any]:
    repo_root = repo_root.resolve(strict=True)
    manifest_path = manifest_path or repo_root / "governance" / "aims" / "manifest.json"
    manifest_bytes = _read_controlled_bytes(manifest_path, MAX_MANIFEST_BYTES, "AIMS manifest")
    manifest = _parse_manifest_bytes(manifest_bytes, "AIMS manifest")
    documents = manifest["documents"]

    ids: set[str] = set()
    paths: set[str] = set()
    prior_id = ""
    total_bytes = 0
    for index, document in enumerate(documents):
        label = f"manifest.documents[{index}]"
        if not isinstance(document, dict):
            raise VerificationError(f"{label} must be an object")
        _require_exact_keys(document, DOCUMENT_KEYS, label)

        document_id = _require_string(document["id"], f"{label}.id", 64)
        if not ID_PATTERN.fullmatch(document_id):
            raise VerificationError(f"{label}.id has an invalid stable identifier")
        if document_id in ids:
            raise VerificationError(f"duplicate controlled document id: {document_id}")
        if document_id <= prior_id:
            raise VerificationError("manifest.documents must be ordered by id")
        ids.add(document_id)
        prior_id = document_id

        path_text = document["path"]
        path = _validate_document_path(repo_root, path_text, label)
        if path_text in paths:
            raise VerificationError(f"duplicate controlled document path: {path_text}")
        paths.add(path_text)

        _version_tuple(document["version"], f"{label}.version")
        status = document["status"]
        if status not in STATUSES:
            raise VerificationError(f"{label}.status must be one of {sorted(STATUSES)}")
        owner = _require_string(document["owner"], f"{label}.owner", 64)
        if not ID_PATTERN.fullmatch(owner):
            raise VerificationError(f"{label}.owner must be a stable role identifier")
        if document["classification"] != "PUBLIC":
            raise VerificationError(f"{label}.classification must be PUBLIC for repository content")

        supersedes = document["supersedes"]
        if not isinstance(supersedes, list) or len(supersedes) > 16:
            raise VerificationError(f"{label}.supersedes must be an array of at most 16 ids")
        if len(set(supersedes)) != len(supersedes):
            raise VerificationError(f"{label}.supersedes contains duplicates")
        for superseded_id in supersedes:
            _require_string(superseded_id, f"{label}.supersedes entry", 64)
            if not ID_PATTERN.fullmatch(superseded_id) or superseded_id == document_id:
                raise VerificationError(f"{label}.supersedes contains an invalid id")

        approval_fields = ("approval_ref", "approved_by", "approved_at", "review_due")
        if status == "DRAFT":
            if any(document[field] is not None for field in approval_fields):
                raise VerificationError(f"{label} is DRAFT and must not contain approval metadata")
            if document["superseded_by"] is not None:
                raise VerificationError(f"{label} is DRAFT and must not be marked superseded")
        else:
            approval_ref = _require_string(document["approval_ref"], f"{label}.approval_ref")
            approved_by = _require_string(document["approved_by"], f"{label}.approved_by")
            approved_at = _require_string(document["approved_at"], f"{label}.approved_at")
            review_due = _require_string(document["review_due"], f"{label}.review_due")
            _validate_timestamp(approved_at, f"{label}.approved_at")
            _validate_date(review_due, f"{label}.review_due")
            if date.fromisoformat(review_due) <= datetime.strptime(approved_at, "%Y-%m-%dT%H:%M:%SZ").date():
                raise VerificationError(f"{label}.review_due must be after approved_at")
            metadata_text = " ".join((approval_ref, approved_by, document["owner"])).upper()
            if any(claim in metadata_text for claim in FORBIDDEN_METADATA_CLAIMS):
                raise VerificationError(f"{label} approval metadata contains a prohibited certification claim")
            if status == "APPROVED" and document["superseded_by"] is not None:
                raise VerificationError(f"{label} is APPROVED and must not be marked superseded")
            if status == "RETIRED":
                successor = _require_string(document["superseded_by"], f"{label}.superseded_by", 64)
                if not ID_PATTERN.fullmatch(successor) or successor == document_id:
                    raise VerificationError(f"{label}.superseded_by contains an invalid successor id")

        expected_sha = document["sha256"]
        if not isinstance(expected_sha, str) or not SHA256_PATTERN.fullmatch(expected_sha):
            raise VerificationError(f"{label}.sha256 must be a lowercase SHA-256 digest")
        data = _read_controlled_bytes(path, MAX_DOCUMENT_BYTES, "controlled AIMS document")
        total_bytes += len(data)
        if total_bytes > MAX_TOTAL_DOCUMENT_BYTES:
            raise VerificationError(f"controlled AIMS documents exceed {MAX_TOTAL_DOCUMENT_BYTES} aggregate bytes")
        actual_sha = hashlib.sha256(data).hexdigest()
        if actual_sha != expected_sha:
            raise VerificationError(f"{label}.sha256 does not match {path_text}")
        displayed_statuses = DISPLAYED_STATUS_PATTERN.findall(data.decode("utf-8"))
        if displayed_statuses != [status]:
            raise VerificationError(f"{label} displayed lifecycle status does not match the manifest")
        if status in {"APPROVED", "RETIRED"}:
            text = data.decode("utf-8").upper()
            if any(marker in text for marker in UNRESOLVED_MARKERS):
                raise VerificationError(f"{label} contains an unresolved placeholder marker")

    retired_ids = {entry["id"] for entry in documents if entry["status"] == "RETIRED"}
    for index, document in enumerate(documents):
        successor = document["superseded_by"]
        if successor is not None and successor not in ids:
            raise VerificationError(f"manifest.documents[{index}].superseded_by references an unknown document")
        if successor is not None:
            successor_document = next(entry for entry in documents if entry["id"] == successor)
            if successor_document["status"] != "APPROVED":
                raise VerificationError(
                    f"manifest.documents[{index}].superseded_by must identify an APPROVED successor"
                )
            if document["id"] not in successor_document["supersedes"]:
                raise VerificationError(
                    f"manifest.documents[{index}].superseded_by is not reciprocated by the successor"
                )
        for superseded_id in document["supersedes"]:
            if superseded_id not in ids:
                raise VerificationError(f"manifest.documents[{index}].supersedes references an unknown document")
            if superseded_id not in retired_ids:
                raise VerificationError(f"manifest.documents[{index}].supersedes must reference a RETIRED document")
            retired_document = next(entry for entry in documents if entry["id"] == superseded_id)
            if retired_document["superseded_by"] != document["id"]:
                raise VerificationError(
                    f"manifest.documents[{index}].supersedes is not owned by this successor"
                )

    records_root = repo_root / "governance" / "aims" / "records"
    if records_root.exists():
        for candidate in records_root.rglob("*"):
            if candidate.is_symlink():
                raise VerificationError(f"controlled records directory contains a symbolic link: {candidate}")
            if candidate.is_file():
                relative = candidate.relative_to(repo_root).as_posix()
                if relative not in paths:
                    raise VerificationError(f"controlled records directory contains an unlisted file: {relative}")
    if prior_manifest_path is not None and prior_manifest_bytes is not None:
        raise VerificationError("provide at most one prior AIMS manifest source")
    if prior_manifest_path is not None:
        prior_manifest_bytes = _read_controlled_bytes(
            prior_manifest_path, MAX_MANIFEST_BYTES, "prior AIMS manifest"
        )
    if prior_manifest_bytes is not None:
        if len(prior_manifest_bytes) > MAX_MANIFEST_BYTES:
            raise VerificationError(f"prior AIMS manifest exceeds {MAX_MANIFEST_BYTES} bytes")
        verify_history(_parse_manifest_bytes(prior_manifest_bytes, "prior AIMS manifest"), manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--prior-manifest", type=Path)
    args = parser.parse_args()
    try:
        verify(args.root, args.manifest, args.prior_manifest)
    except VerificationError as exc:
        print(f"AIMS document verification failed: {exc}", file=sys.stderr)
        return 1
    print("AIMS controlled documents verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
