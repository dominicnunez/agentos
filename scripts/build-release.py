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
        modules[path] = {"path": path, "version": module_version}
    return sorted(modules.values(), key=lambda item: (item["path"], item["version"]))


def cyclone_dx(
    goos: str, goarch: str, version: str, commit: str, go_version: str
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
    output.mkdir(parents=True, exist_ok=False)
    host_goos = command(
        ["go", "env", "GOOS"], env=build_environment(), capture=True
    ).strip()
    host_goarch = command(
        ["go", "env", "GOARCH"], env=build_environment(), capture=True
    ).strip()

    produced: list[Path] = []
    with tempfile.TemporaryDirectory(prefix="agentos-release-") as temporary:
        staging = Path(temporary)
        for goos, goarch in TARGETS:
            target = f"{goos}_{goarch}"
            target_dir = staging / target
            target_dir.mkdir()
            packaged: dict[str, tuple[bytes, int]] = {
                "README.md": ((ROOT / "README.md").read_bytes(), 0o644),
                "VERSION": (version_file.read_bytes(), 0o644),
            }
            for binary_name, package in BINARIES:
                filename = binary_name
                binary = target_dir / filename
                build_binary(package, binary, goos, goarch, version)
                packaged[filename] = (binary.read_bytes(), 0o755)
            sbom_name = f"agentos_{version}_{target}.cdx.json"
            sbom = cyclone_dx(goos, goarch, version, commit, go_version)
            sbom_path = output / sbom_name
            sbom_path.write_bytes(sbom)
            produced.append(sbom_path)
            packaged[sbom_name] = (sbom, 0o644)
            root_name = f"agentos_{version}_{target}"
            archive_path = output / (root_name + ".tar.gz")
            tar_gzip(archive_path, root_name, packaged, epoch)
            verify_archive(archive_path, root_name, packaged, epoch)
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
