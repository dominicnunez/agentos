#!/usr/bin/env python3
"""Materialize the exact staged AIMS verification files without checkout filters."""

from __future__ import annotations

import argparse
import os
from pathlib import Path, PurePosixPath

try:
    from scripts.build_aims_assessment_bundle import _git
    from scripts.verify_aims_documents import MAX_DOCUMENTS, VerificationError
except ModuleNotFoundError:
    from build_aims_assessment_bundle import _git
    from verify_aims_documents import MAX_DOCUMENTS, VerificationError


MAX_INDEX_LISTING_BYTES = 256 * 1024
MAX_STAGED_FILE_BYTES = 1024 * 1024
MAX_STAGED_TOTAL_BYTES = 8 * 1024 * 1024
MAX_STAGED_FILES = MAX_DOCUMENTS + 16
FIXED_PATHS = {
    ".github/workflows/ci.yml",
    ".githooks/pre-commit",
    "scripts/build_aims_assessment_bundle.py",
    "scripts/materialize_staged_aims.py",
    "scripts/test_build_aims_assessment_bundle.py",
    "scripts/test_verify_aims_documents.py",
    "scripts/verify_aims_documents.py",
}
CONTROLLED_PREFIX = "governance/aims/"


def materialize(repo_root: Path, output_root: Path) -> None:
    repo_root = repo_root.resolve(strict=True)
    if output_root.is_symlink():
        raise VerificationError("staged output root must be an existing non-symbolic-link directory")
    output_root = output_root.resolve(strict=True)
    if not output_root.is_dir():
        raise VerificationError("staged output root must be an existing non-symbolic-link directory")
    with os.scandir(output_root) as entries:
        if next(entries, None) is not None:
            raise VerificationError("staged output root must be empty")

    listing = _git(
        repo_root,
        [
            "ls-files",
            "--stage",
            "-z",
            "--",
            *sorted(FIXED_PATHS),
            "governance/aims",
        ],
        max_output_bytes=MAX_INDEX_LISTING_BYTES,
    )
    records = [record for record in listing.split(b"\0") if record]
    if len(records) > MAX_STAGED_FILES:
        raise VerificationError(f"staged AIMS verification set exceeds {MAX_STAGED_FILES} files")

    seen: set[str] = set()
    total_bytes = 0
    for record in records:
        try:
            metadata, raw_path = record.split(b"\t", 1)
            mode, object_id, stage = metadata.decode("ascii", errors="strict").split(" ")
            relative = raw_path.decode("utf-8", errors="strict")
        except (UnicodeDecodeError, ValueError) as exc:
            raise VerificationError("staged AIMS index contains malformed metadata") from exc
        if stage != "0":
            raise VerificationError(f"staged AIMS path is unresolved: {relative}")
        if mode not in {"100644", "100755"}:
            raise VerificationError(f"staged AIMS path is not a regular file: {relative}")
        if relative.startswith(CONTROLLED_PREFIX) and mode != "100644":
            raise VerificationError(f"controlled AIMS path must use mode 100644: {relative}")
        path = PurePosixPath(relative)
        if (
            path.is_absolute()
            or ".." in path.parts
            or relative != path.as_posix()
            or (relative not in FIXED_PATHS and not relative.startswith(CONTROLLED_PREFIX))
            or relative in seen
        ):
            raise VerificationError(f"unsafe or duplicate staged AIMS path: {relative}")
        seen.add(relative)

        raw_size = _git(repo_root, ["cat-file", "-s", object_id], max_output_bytes=32)
        try:
            size = int(raw_size.decode("ascii", errors="strict").strip())
        except (UnicodeDecodeError, ValueError) as exc:
            raise VerificationError(f"invalid staged blob size: {relative}") from exc
        if size < 0 or size > MAX_STAGED_FILE_BYTES:
            raise VerificationError(f"staged AIMS file exceeds {MAX_STAGED_FILE_BYTES} bytes: {relative}")
        total_bytes += size
        if total_bytes > MAX_STAGED_TOTAL_BYTES:
            raise VerificationError(
                f"staged AIMS verification files exceed {MAX_STAGED_TOTAL_BYTES} aggregate bytes"
            )
        data = _git(repo_root, ["cat-file", "blob", object_id], max_output_bytes=size)
        if len(data) != size:
            raise VerificationError(f"staged AIMS blob size changed while reading: {relative}")

        destination = output_root.joinpath(*path.parts)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
        os.chmod(destination, 0o755 if mode == "100755" else 0o644)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        materialize(args.root, args.output)
    except (OSError, VerificationError) as exc:
        print(f"staged AIMS materialization failed: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
