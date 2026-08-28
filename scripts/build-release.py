#!/usr/bin/env python3
"""Build deterministic Agent OS release-candidate artifacts without publishing."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import platform
import re
import subprocess
import tarfile
import tempfile
from pathlib import Path
from urllib.parse import quote


ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = "https://github.com/dominicnunez/agentos"
TARGETS = (
    ("linux", "amd64"),
    ("linux", "arm64"),
)
BINARIES = (
    ("agentos", "./cmd/agentos"),
    ("agentos-recovery", "./cmd/agentos-recovery"),
)
SEMVER = re.compile(
    r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-((?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
    r"(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*))?"
    r"(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$"
)
BUILD_FLAGS = ("-mod=readonly", "-trimpath", "-buildvcs=false")
LINK_FLAGS = "-s -w -buildid="
LICENSE_NAME = re.compile(
    r"^(?:LICEN[CS]E|COPYING)(?:$|[._-])", re.IGNORECASE
)
SUPPLEMENTAL_NOTICE_NAME = re.compile(
    r"^(?:NOTICE|COPYRIGHT)(?:$|[._-])", re.IGNORECASE
)
MAX_LICENSE_BYTES = 2 << 20
MAX_DEPENDENCY_SOURCE_FILE_BYTES = 64 << 20
MAX_DEPENDENCY_SOURCE_BYTES = 512 << 20
APACHE2_SHA256 = "c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4"
PROJECT_NOTICE = b"Agent OS\nCopyright 2026 Dominic Nunez\n"


def command(
    args: list[str], *, env: dict[str, str] | None = None, capture: bool = False
) -> str:
    result = subprocess.run(
        args,
        cwd=ROOT,
        env=env,
        check=True,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )
    return result.stdout if capture else ""


def command_bytes(args: list[str]) -> bytes:
    result = subprocess.run(
        args,
        cwd=ROOT,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return result.stdout


def canonical_version(path: Path) -> str:
    raw = path.read_bytes()
    if b"\x00" in raw or raw.count(b"\n") != 1 or not raw.endswith(b"\n"):
        raise RuntimeError("VERSION must contain one newline-terminated semantic version")
    version = raw[:-1].decode("ascii")
    if not SEMVER.fullmatch(version):
        raise RuntimeError(f"VERSION is not canonical SemVer: {version!r}")
    return version


def pinned_go_version(path: Path) -> str:
    directives = re.findall(
        r"^go ([0-9]+\.[0-9]+(?:\.[0-9]+)?)\s*$",
        path.read_text(encoding="utf-8"),
        flags=re.MULTILINE,
    )
    if len(directives) != 1:
        raise RuntimeError("go.mod must contain exactly one canonical Go directive")
    return "go" + directives[0]


def pinned_python_version(path: Path) -> str:
    raw = path.read_bytes()
    if b"\x00" in raw or raw.count(b"\n") != 1 or not raw.endswith(b"\n"):
        raise RuntimeError(".python-version must contain one newline-terminated version")
    version = raw[:-1].decode("ascii")
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        raise RuntimeError(".python-version must contain one exact CPython version")
    return version


def source_identity() -> tuple[str, int]:
    commit = command(["git", "rev-parse", "HEAD"], capture=True).strip()
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise RuntimeError("release source must resolve to one full Git commit")
    status = command(
        [
            "git",
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--ignored=matching",
        ],
        capture=True,
    )
    if status:
        raise RuntimeError(
            "release source must contain only files tracked by the source commit"
        )
    commit_epoch_text = command(
        ["git", "show", "-s", "--format=%ct", "HEAD"], capture=True
    ).strip()
    configured_epoch = os.environ.get("SOURCE_DATE_EPOCH", "")
    if configured_epoch and configured_epoch != commit_epoch_text:
        raise RuntimeError("SOURCE_DATE_EPOCH must equal the source commit timestamp")
    epoch_text = configured_epoch or commit_epoch_text
    if not epoch_text.isdigit() or int(epoch_text) < 315532800:
        raise RuntimeError("SOURCE_DATE_EPOCH must be a Unix timestamp on or after 1980-01-01")
    return commit, int(epoch_text)


def build_environment(goos: str | None = None, goarch: str | None = None) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(
        {
            "CGO_ENABLED": "0",
            "GOENV": "off",
            "GOEXPERIMENT": "",
            "GOFIPS140": "off",
            "GOFLAGS": "-mod=readonly" if goos is not None else "",
            "GOTOOLCHAIN": "local",
            "GOWORK": "off",
        }
    )
    environment.pop("GOOS", None)
    environment.pop("GOARCH", None)
    environment.pop("GOAMD64", None)
    environment.pop("GOARM64", None)
    if goos is not None:
        environment["GOOS"] = goos
    if goarch is not None:
        environment["GOARCH"] = goarch
        if goarch == "amd64":
            environment["GOAMD64"] = "v1"
        elif goarch == "arm64":
            environment["GOARM64"] = "v8.0"
    return environment


def json_stream(text: str) -> list[dict]:
    decoder = json.JSONDecoder()
    values: list[dict] = []
    position = 0
    while position < len(text):
        while position < len(text) and text[position].isspace():
            position += 1
        if position == len(text):
            break
        value, position = decoder.raw_decode(text, position)
        if not isinstance(value, dict):
            raise RuntimeError("Go emitted a non-object package record")
        values.append(value)
    return values


def module_purl(path: str, version: str) -> str:
    return "pkg:golang/" + quote(path, safe="/._-") + "@" + quote(
        version, safe="._-"
    )


def target_modules(goos: str, goarch: str, version: str) -> list[dict]:
    environment = build_environment(goos, goarch)
    packages = json_stream(
        command(
            ["go", "list", "-mod=readonly", "-deps", "-json", *[item[1] for item in BINARIES]],
            env=environment,
            capture=True,
        )
    )
    modules: dict[str, dict] = {}
    for package in packages:
        module = package.get("Module")
        if not isinstance(module, dict):
            continue
        if module.get("Replace") is not None:
            raise RuntimeError("release builds do not allow replaced Go modules")
        path = module.get("Path")
        if not isinstance(path, str) or not path:
            raise RuntimeError("compiled package has an invalid module path")
        if module.get("Main"):
            module_version = "v" + version
        else:
            module_version = module.get("Version")
            if not isinstance(module_version, str) or not module_version:
                raise RuntimeError(f"compiled module {path!r} has no immutable version")
        directory = module.get("Dir")
        if not isinstance(directory, str) or not directory:
            raise RuntimeError(f"compiled module {path!r} has no source directory")
        record = {"path": path, "version": module_version, "directory": directory}
        previous = modules.get(path)
        if previous is not None and previous != record:
            raise RuntimeError(f"compiled module {path!r} resolved inconsistently")
        modules[path] = record
    return sorted(modules.values(), key=lambda item: (item["path"], item["version"]))


def compiled_modules(version: str) -> list[dict]:
    modules: dict[str, dict] = {}
    for goos, goarch in TARGETS:
        for module in target_modules(goos, goarch, version):
            previous = modules.get(module["path"])
            if previous is not None and previous != module:
                raise RuntimeError(
                    f"compiled module {module['path']!r} differs across targets"
                )
            modules[module["path"]] = module
    return sorted(modules.values(), key=lambda item: (item["path"], item["version"]))


def third_party_component_path(path: str, version: str) -> str:
    segments = path.split("/")
    if any(not segment or segment in {".", ".."} for segment in segments):
        raise RuntimeError(f"compiled module path is unsafe: {path!r}")
    encoded = "/".join(quote(segment, safe="._-") for segment in segments)
    return f"THIRD_PARTY_LICENSES/{encoded}@{quote(version, safe='._-+')}"


def third_party_licenses(modules: list[dict], version: str, commit: str) -> dict[str, tuple[bytes, int]]:
    files: dict[str, tuple[bytes, int]] = {}
    manifest_modules: list[dict] = []
    for module in modules:
        if module["path"] == "github.com/dominicnunez/agentos":
            continue
        directory = Path(module["directory"])
        if not directory.is_dir():
            raise RuntimeError(
                f"compiled module {module['path']!r} source directory is unavailable"
            )
        evidence: list[Path] = []
        license_evidence: list[Path] = []
        for candidate in sorted(directory.iterdir(), key=lambda item: item.name):
            if not candidate.is_file() or candidate.is_symlink():
                continue
            if LICENSE_NAME.match(candidate.name):
                license_evidence.append(candidate)
                evidence.append(candidate)
            elif SUPPLEMENTAL_NOTICE_NAME.match(candidate.name):
                evidence.append(candidate)
        if not license_evidence:
            raise RuntimeError(
                f"compiled module {module['path']} {module['version']} lacks root license evidence"
            )
        component = third_party_component_path(module["path"], module["version"])
        bundled_names: list[str] = []
        for evidence_file in evidence:
            content = evidence_file.read_bytes()
            if not content or len(content) > MAX_LICENSE_BYTES:
                raise RuntimeError(
                    f"license evidence {evidence_file} is empty or exceeds {MAX_LICENSE_BYTES} bytes"
                )
            destination = f"{component}/{evidence_file.name}"
            if destination in files:
                raise RuntimeError(f"duplicate bundled license path {destination!r}")
            files[destination] = (content, 0o644)
            bundled_names.append(destination.removeprefix("THIRD_PARTY_LICENSES/"))
        manifest_modules.append(
            {
                "licenseFiles": bundled_names,
                "path": module["path"],
                "purl": module_purl(module["path"], module["version"]),
                "version": module["version"],
            }
        )
    if not manifest_modules:
        raise RuntimeError("release binaries unexpectedly have no third-party modules")
    manifest = {
        "generatedFrom": {
            "commit": commit,
            "repository": REPOSITORY,
            "tag": "v" + version,
        },
        "modules": manifest_modules,
        "schemaVersion": 1,
    }
    files["THIRD_PARTY_LICENSES/manifest.json"] = (
        (json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8"),
        0o644,
    )
    return files


def add_frontend_license_bundle(
    files: dict[str, tuple[bytes, int]],
    source_files: dict[str, tuple[bytes, int]],
) -> list[dict[str, str]]:
    bundle_name = "internal/dashboard/dist/THIRD_PARTY_LICENSES.json"
    lockfile_name = "web/dashboard/pnpm-lock.yaml"
    bundle_record = source_files.get(bundle_name)
    lockfile_record = source_files.get(lockfile_name)
    if bundle_record is None or lockfile_record is None:
        raise RuntimeError("dashboard source lacks its compiled license bundle or lockfile")
    try:
        bundle = json.loads(bundle_record[0].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError("dashboard compiled license bundle is invalid JSON") from error
    if not isinstance(bundle, dict) or bundle.get("schema_version") != 1:
        raise RuntimeError("dashboard compiled license bundle schema is invalid")
    lockfile_digest = hashlib.sha256(lockfile_record[0]).hexdigest()
    if bundle.get("lockfile_sha256") != lockfile_digest:
        raise RuntimeError("dashboard compiled license bundle does not match pnpm-lock.yaml")
    packages = bundle.get("packages")
    texts = bundle.get("license_texts")
    if not isinstance(packages, list) or not packages or not isinstance(texts, list) or not texts:
        raise RuntimeError("dashboard compiled license bundle is empty")
    text_hashes: set[str] = set()
    for item in texts:
        if not isinstance(item, dict) or set(item) != {"sha256", "text"}:
            raise RuntimeError("dashboard license text entry is invalid")
        digest, text = item["sha256"], item["text"]
        if (
            not isinstance(digest, str)
            or not re.fullmatch(r"[0-9a-f]{64}", digest)
            or not isinstance(text, str)
            or not text
            or len(text.encode("utf-8")) > MAX_LICENSE_BYTES
            or hashlib.sha256(text.encode("utf-8")).hexdigest() != digest
            or digest in text_hashes
        ):
            raise RuntimeError("dashboard license text evidence is invalid")
        text_hashes.add(digest)
    identities: set[tuple[str, str]] = set()
    for package in packages:
        if not isinstance(package, dict) or set(package) != {
            "name", "version", "declared_license", "evidence"
        }:
            raise RuntimeError("dashboard compiled package record is invalid")
        name, package_version = package["name"], package["version"]
        evidence = package["evidence"]
        identity = (name, package_version)
        if (
            not isinstance(name, str)
            or not name
            or not isinstance(package_version, str)
            or not package_version
            or not isinstance(package["declared_license"], str)
            or not package["declared_license"]
            or not isinstance(evidence, list)
            or not evidence
            or identity in identities
        ):
            raise RuntimeError("dashboard compiled package identity or evidence is invalid")
        identities.add(identity)
        for record in evidence:
            if (
                not isinstance(record, dict)
                or set(record) != {"file", "sha256"}
                or not isinstance(record["file"], str)
                or not LICENSE_NAME.match(record["file"])
                or record["sha256"] not in text_hashes
            ):
                raise RuntimeError("dashboard compiled package lacks discoverable license evidence")
    destination = "THIRD_PARTY_LICENSES/npm-frontend.json"
    files[destination] = bundle_record
    manifest_body, manifest_mode = files["THIRD_PARTY_LICENSES/manifest.json"]
    manifest = json.loads(manifest_body)
    manifest["frontend"] = {
        "licenseBundle": destination.removeprefix("THIRD_PARTY_LICENSES/"),
        "lockfileSha256": lockfile_digest,
        "packages": [
            {"name": package["name"], "version": package["version"]}
            for package in packages
        ],
    }
    files["THIRD_PARTY_LICENSES/manifest.json"] = (
        (json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8"),
        manifest_mode,
    )
    return [
        {
            "declared_license": package["declared_license"],
            "name": package["name"],
            "version": package["version"],
        }
        for package in packages
    ]


def dependency_source_files(modules: list[dict]) -> dict[str, tuple[bytes, int]]:
    required = {
        (module["path"], module["version"])
        for module in modules
        if module["path"] != "github.com/dominicnunez/agentos"
    }
    if not required:
        raise RuntimeError("release binaries unexpectedly have no external modules")
    with tempfile.TemporaryDirectory(prefix="agentos-vendor-") as temporary:
        vendor = Path(temporary) / "vendor"
        command(
            ["go", "mod", "vendor", "-o", str(vendor)],
            env=build_environment(),
        )
        modules_path = vendor / "modules.txt"
        if not modules_path.is_file() or modules_path.is_symlink():
            raise RuntimeError("Go did not produce a regular vendor/modules.txt")
        vendored: set[tuple[str, str]] = set()
        for line in modules_path.read_text(encoding="utf-8").splitlines():
            if not line.startswith("# "):
                continue
            fields = line.split()
            if len(fields) >= 3:
                vendored.add((fields[1], fields[2]))
        missing = sorted(required - vendored)
        if missing:
            formatted = ", ".join(f"{path}@{version}" for path, version in missing)
            raise RuntimeError(
                "vendored source omits modules compiled into release binaries: "
                + formatted
            )

        files: dict[str, tuple[bytes, int]] = {}
        total_bytes = 0
        for candidate in sorted(
            vendor.rglob("*"), key=lambda item: item.relative_to(vendor).as_posix()
        ):
            if candidate.is_symlink():
                raise RuntimeError(f"vendored source contains a symlink: {candidate}")
            if candidate.is_dir():
                continue
            if not candidate.is_file():
                raise RuntimeError(
                    f"vendored source contains a non-regular entry: {candidate}"
                )
            content = candidate.read_bytes()
            if len(content) > MAX_DEPENDENCY_SOURCE_FILE_BYTES:
                raise RuntimeError(
                    f"vendored source file exceeds {MAX_DEPENDENCY_SOURCE_FILE_BYTES} bytes: "
                    f"{candidate}"
                )
            total_bytes += len(content)
            if total_bytes > MAX_DEPENDENCY_SOURCE_BYTES:
                raise RuntimeError(
                    f"vendored source exceeds {MAX_DEPENDENCY_SOURCE_BYTES} bytes"
                )
            relative = candidate.relative_to(vendor).as_posix()
            if relative.startswith("/") or ".." in Path(relative).parts:
                raise RuntimeError(f"vendored source path is unsafe: {relative!r}")
            files[f"vendor/{relative}"] = (content, 0o644)
    if "vendor/modules.txt" not in files:
        raise RuntimeError("vendored source is empty")
    return files


def cyclone_dx(
    goos: str,
    goarch: str,
    version: str,
    commit: str,
    go_version: str,
    frontend_packages: list[dict[str, str]],
) -> bytes:
    root_purl = module_purl("github.com/dominicnunez/agentos", "v" + version)
    components = []
    for module in target_modules(goos, goarch, version):
        if module["path"] == "github.com/dominicnunez/agentos":
            continue
        purl = module_purl(module["path"], module["version"])
        components.append(
            {
                "bom-ref": purl,
                "name": module["path"],
                "purl": purl,
                "type": "library",
                "version": module["version"],
            }
        )
    for package in frontend_packages:
        purl = (
            "pkg:npm/"
            + quote(package["name"], safe="/")
            + "@"
            + quote(package["version"], safe="._-+")
        )
        components.append(
            {
                "bom-ref": purl,
                "licenses": [{"license": {"name": package["declared_license"]}}],
                "name": package["name"],
                "purl": purl,
                "type": "library",
                "version": package["version"],
            }
        )
    components.sort(key=lambda item: item["purl"])
    document = {
        "bomFormat": "CycloneDX",
        "components": components,
        "metadata": {
            "component": {
                "bom-ref": root_purl,
                "externalReferences": [
                    {
                        "type": "vcs",
                        "url": f"git+{REPOSITORY}.git@{commit}",
                    }
                ],
                "name": "agentos",
                "properties": [
                    {"name": "agentos:build:go-version", "value": go_version},
                    {"name": "agentos:target:goarch", "value": goarch},
                    {"name": "agentos:target:goos", "value": goos},
                ],
                "purl": root_purl,
                "type": "application",
                "version": version,
            }
        },
        "specVersion": "1.6",
        "version": 1,
    }
    return (json.dumps(document, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")


def build_binary(
    package: str,
    destination: Path,
    goos: str,
    goarch: str,
    version: str,
) -> None:
    environment = build_environment(goos, goarch)
    command(
        [
            "go",
            "build",
            *BUILD_FLAGS,
            "-ldflags",
            LINK_FLAGS + " -X main.version=" + version,
            "-o",
            str(destination),
            package,
        ],
        env=environment,
    )


def source_notice(version: str, commit: str) -> bytes:
    return (
        "# Agent OS corresponding source\n\n"
        f"- Repository: {REPOSITORY}\n"
        f"- Commit: `{commit}`\n"
        f"- Immutable release tag: `v{version}`\n"
        f"- Corresponding-source archive: `agentos_{version}_source.tar.gz`\n"
        "- License: `Apache-2.0`\n\n"
        "The release tag must resolve to the commit above when the release is "
        "published. The source archive is generated deterministically from that "
        "commit and contains the complete tracked source, this notice, and an "
        "exact vendored source tree for the external Go modules needed to build "
        "and test the release without network access.\n"
    ).encode("utf-8")


def tracked_source_files(source_md: bytes) -> dict[str, tuple[bytes, int]]:
    records = command_bytes(["git", "ls-files", "--stage", "-z"])
    files: dict[str, tuple[bytes, int]] = {}
    for raw_record in records.split(b"\x00"):
        if not raw_record:
            continue
        try:
            metadata, raw_name = raw_record.split(b"\t", 1)
            mode_text, object_id, stage = metadata.decode("ascii").split(" ")
            name = raw_name.decode("utf-8")
        except (UnicodeDecodeError, ValueError) as error:
            raise RuntimeError("Git returned an invalid tracked-file record") from error
        if stage != "0" or not re.fullmatch(r"[0-9a-f]{40,64}", object_id):
            raise RuntimeError(f"tracked source entry {name!r} is not a normal stage-0 file")
        if mode_text not in {"100644", "100755"}:
            raise RuntimeError(
                f"tracked source entry {name!r} has unsupported mode {mode_text!r}"
            )
        path = ROOT / name
        if not path.is_file() or path.is_symlink():
            raise RuntimeError(f"tracked source entry {name!r} is not a regular file")
        normalized = Path(name).as_posix()
        if normalized.startswith("/") or ".." in Path(normalized).parts:
            raise RuntimeError(f"tracked source entry is unsafe: {name!r}")
        files[normalized] = (
            command_bytes(["git", "cat-file", "blob", object_id]),
            int(mode_text[-3:], 8),
        )
    if not files:
        raise RuntimeError("source commit contains no tracked files")
    if "SOURCE.md" in files:
        raise RuntimeError("SOURCE.md is generated by the release builder and must not be tracked")
    if any(name == "vendor" or name.startswith("vendor/") for name in files):
        raise RuntimeError("vendor is generated by the release builder and must not be tracked")
    files["SOURCE.md"] = (source_md, 0o644)
    return files


def tar_gzip(path: Path, root_name: str, files: dict[str, tuple[bytes, int]], epoch: int) -> None:
    with path.open("xb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, compresslevel=9, mtime=epoch) as compressed:
            with tarfile.open(fileobj=compressed, mode="w", format=tarfile.GNU_FORMAT) as archive:
                for name in sorted(files):
                    content, mode = files[name]
                    info = tarfile.TarInfo(name=f"{root_name}/{name}")
                    info.gid = 0
                    info.gname = ""
                    info.mode = mode
                    info.mtime = epoch
                    info.size = len(content)
                    info.uid = 0
                    info.uname = ""
                    archive.addfile(info, io.BytesIO(content))


def verify_archive(
    path: Path, root_name: str, files: dict[str, tuple[bytes, int]], epoch: int
) -> None:
    expected = {f"{root_name}/{name}": value for name, value in files.items()}
    with tarfile.open(path, mode="r:gz") as archive:
        entries = archive.getmembers()
        if len(entries) != len(expected) or len({entry.name for entry in entries}) != len(entries):
            raise RuntimeError(f"archive {path.name} has missing or duplicate entries")
        for entry in entries:
            value = expected.get(entry.name)
            if value is None or not entry.isfile():
                raise RuntimeError(f"archive {path.name} has an unexpected entry")
            content, mode = value
            source = archive.extractfile(entry)
            if source is None or source.read() != content:
                raise RuntimeError(f"archive {path.name} entry {entry.name} changed")
            if entry.mode != mode or entry.uid != 0 or entry.gid != 0 or entry.mtime != epoch:
                raise RuntimeError(f"archive {path.name} entry metadata is not reproducible")


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def smoke(binary: Path, version: str) -> None:
    result = subprocess.run(
        [str(binary), "--version"],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.stdout != version + "\n" or result.stderr:
        raise RuntimeError(f"version smoke failed for {binary.name}")


def provenance(
    version: str,
    commit: str,
    epoch: int,
    go_version: str,
    python_version: str,
    subjects: list[dict],
) -> bytes:
    statement = {
        "_type": "https://in-toto.io/Statement/v1",
        "predicate": {
            "buildDefinition": {
                "buildType": f"{REPOSITORY}/blob/{commit}/docs/RELEASE.md#reproducible-builder-v1",
                "externalParameters": {
                    "cgoEnabled": False,
                    "targets": [f"{goos}/{goarch}" for goos, goarch in TARGETS],
                    "version": version,
                },
                "internalParameters": {
                    "buildFlags": list(BUILD_FLAGS),
                    "goVersion": go_version,
                    "linkFlags": LINK_FLAGS + " -X main.version=<version>",
                    "moduleVerification": "go mod verify",
                    "pythonVersion": python_version,
                    "sourceDateEpoch": epoch,
                },
                "resolvedDependencies": [
                    {
                        "digest": {"gitCommit": commit},
                        "uri": f"git+{REPOSITORY}.git@{commit}",
                    }
                ],
            },
            "runDetails": {
                "builder": {
                    "id": f"{REPOSITORY}/blob/{commit}/scripts/build-release.py"
                }
            },
        },
        "predicateType": "https://slsa.dev/provenance/v1",
        "subject": subjects,
    }
    return (json.dumps(statement, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("work/release"))
    args = parser.parse_args()

    output = args.output if args.output.is_absolute() else ROOT / args.output
    version_file = ROOT / "VERSION"
    version = canonical_version(version_file)
    commit, epoch = source_identity()
    expected_go_version = pinned_go_version(ROOT / "go.mod")
    expected_python_version = pinned_python_version(ROOT / ".python-version")
    go_version = command(
        ["go", "env", "GOVERSION"], env=build_environment(), capture=True
    ).strip()
    if not re.fullmatch(r"go[0-9]+\.[0-9]+(?:\.[0-9]+)?", go_version):
        raise RuntimeError("Go toolchain version is not canonical")
    if go_version != expected_go_version:
        raise RuntimeError(
            f"Go toolchain {go_version!r} does not match go.mod {expected_go_version!r}"
        )
    runtime_python_version = platform.python_version()
    if (
        platform.python_implementation() != "CPython"
        or runtime_python_version != expected_python_version
    ):
        raise RuntimeError(
            "Python runtime "
            f"{platform.python_implementation()} {runtime_python_version!r} "
            "does not match required CPython .python-version "
            f"{expected_python_version!r}"
        )
    python_version = "CPython " + runtime_python_version
    dependency_environment = build_environment()
    command(["go", "mod", "download"], env=dependency_environment)
    command(["go", "mod", "verify"], env=dependency_environment)
    source_md = source_notice(version, commit)
    source_files = tracked_source_files(source_md)
    license = source_files.get("LICENSE")
    if license is None or hashlib.sha256(license[0]).hexdigest() != APACHE2_SHA256:
        raise RuntimeError("LICENSE must contain the unmodified Apache License 2.0 text")
    license_bytes = license[0]
    notice = source_files.get("NOTICE")
    if notice is None or notice[0] != PROJECT_NOTICE:
        raise RuntimeError("NOTICE must contain the canonical Agent OS attribution")
    notice_bytes = notice[0]
    module_records = compiled_modules(version)
    vendored_sources = dependency_source_files(module_records)
    if source_files.keys() & vendored_sources.keys():
        raise RuntimeError("vendored dependency source collides with tracked source")
    source_files.update(vendored_sources)
    bundled_licenses = third_party_licenses(module_records, version, commit)
    frontend_packages = add_frontend_license_bundle(bundled_licenses, source_files)
    output.mkdir(parents=True, exist_ok=False)
    host_goos = command(
        ["go", "env", "GOOS"], env=build_environment(), capture=True
    ).strip()
    host_goarch = command(
        ["go", "env", "GOARCH"], env=build_environment(), capture=True
    ).strip()

    produced: list[Path] = []
    source_md_path = output / "SOURCE.md"
    source_md_path.write_bytes(source_md)
    produced.append(source_md_path)
    source_root = f"agentos_{version}_source"
    source_archive = output / f"{source_root}.tar.gz"
    tar_gzip(source_archive, source_root, source_files, epoch)
    verify_archive(source_archive, source_root, source_files, epoch)
    produced.append(source_archive)
    license_root = f"agentos_{version}_third-party-licenses"
    license_archive = output / f"{license_root}.tar.gz"
    standalone_licenses = {
        name.removeprefix("THIRD_PARTY_LICENSES/"): value
        for name, value in bundled_licenses.items()
    }
    tar_gzip(license_archive, license_root, standalone_licenses, epoch)
    verify_archive(license_archive, license_root, standalone_licenses, epoch)
    produced.append(license_archive)
    with tempfile.TemporaryDirectory(prefix="agentos-release-") as temporary:
        staging = Path(temporary)
        for goos, goarch in TARGETS:
            target = f"{goos}_{goarch}"
            target_dir = staging / target
            target_dir.mkdir()
            packaged: dict[str, tuple[bytes, int]] = {
                "LICENSE": (license_bytes, 0o644),
                "NOTICE": (notice_bytes, 0o644),
                "README.md": (source_files["README.md"][0], 0o644),
                "SOURCE.md": (source_md, 0o644),
                "VERSION": (source_files["VERSION"][0], 0o644),
            }
            packaged.update(bundled_licenses)
            for binary_name, package in BINARIES:
                filename = binary_name
                binary = target_dir / filename
                build_binary(package, binary, goos, goarch, version)
                packaged[filename] = (binary.read_bytes(), 0o755)
            sbom_name = f"agentos_{version}_{target}.cdx.json"
            sbom = cyclone_dx(
                goos, goarch, version, commit, go_version, frontend_packages
            )
            sbom_path = output / sbom_name
            sbom_path.write_bytes(sbom)
            produced.append(sbom_path)
            packaged[sbom_name] = (sbom, 0o644)
            root_name = f"agentos_{version}_{target}"
            archive_path = output / (root_name + ".tar.gz")
            tar_gzip(archive_path, root_name, packaged, epoch)
            verify_archive(archive_path, root_name, packaged, epoch)
            for required in (
                "LICENSE",
                "NOTICE",
                "SOURCE.md",
                "THIRD_PARTY_LICENSES/manifest.json",
                "THIRD_PARTY_LICENSES/npm-frontend.json",
            ):
                if required not in packaged:
                    raise RuntimeError(
                        f"binary package {archive_path.name} lacks required {required}"
                    )
            produced.append(archive_path)

            if (goos, goarch) == (host_goos, host_goarch):
                for binary_name, _ in BINARIES:
                    smoke(target_dir / binary_name, version)

    subjects = [
        {"digest": {"sha256": digest(path)}, "name": path.name}
        for path in sorted(produced, key=lambda item: item.name)
    ]
    provenance_path = output / f"agentos_{version}_provenance.intoto.jsonl"
    provenance_path.write_bytes(
        provenance(version, commit, epoch, go_version, python_version, subjects)
    )
    produced.append(provenance_path)
    checksums = "".join(
        f"{digest(path)}  {path.name}\n"
        for path in sorted(produced, key=lambda item: item.name)
    )
    checksums_path = output / "SHA256SUMS"
    checksums_path.write_text(checksums, encoding="ascii", newline="\n")
    for line in checksums_path.read_text(encoding="ascii").splitlines():
        expected_digest, filename = line.split("  ", 1)
        if digest(output / filename) != expected_digest:
            raise RuntimeError(f"checksum verification failed for {filename}")
    print(
        json.dumps(
            {
                "commit": commit,
                "files": sorted([path.name for path in produced] + ["SHA256SUMS"]),
                "go_version": go_version,
                "python_version": python_version,
                "source_date_epoch": epoch,
                "version": version,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
