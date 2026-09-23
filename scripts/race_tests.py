"""Run every package's race tests, splitting the large ledger suite by test."""

import json
from pathlib import Path
import re
import subprocess
import tempfile


def test_groups(listing, count=4):
    names = []
    for name in listing.splitlines():
        if name.startswith("Benchmark"):
            continue
        if not name.startswith(("Test", "Example", "Fuzz")) or not name.isidentifier():
            raise ValueError(f"Unexpected test listing: {name!r}")
        names.append(name)
    if not names or len(names) != len(set(names)) or count < 1:
        raise ValueError("Test discovery must be nonempty and unique")
    names.sort()
    return [names[index::count] for index in range(min(count, len(names)))]


def run_ledger(root):
    package = root / "internal" / "ledger"
    # Keep one complete process run to detect interactions across group boundaries.
    subprocess.run(["go", "test", "-count=1", "-timeout=20m", "./internal/ledger"],
                   cwd=root, check=True)
    with tempfile.TemporaryDirectory(prefix="agentos-race-") as temp:
        binary = str(Path(temp) / "ledger.test")
        subprocess.run(["go", "test", "-race", "-c", "-o", binary, "./internal/ledger"],
                       cwd=root, check=True)
        listing = subprocess.run([binary, "-test.list=."], cwd=package,
                                 check=True, capture_output=True, text=True).stdout
        groups = test_groups(listing)
        for index, names in enumerate(groups, 1):
            print(f"Ledger race group {index}/{len(groups)}: {len(names)} tests", flush=True)
            pattern = "^(" + "|".join(re.escape(name) for name in names) + ")$"
            result = subprocess.run(
                ["go", "tool", "test2json", "-t", binary, "-test.v=test2json",
                 "-test.count=1", "-test.timeout=20m", "-test.run=" + pattern],
                cwd=package, capture_output=True, text=True)
            print(result.stdout, end="", flush=True)
            print(result.stderr, end="", flush=True)
            result.check_returncode()
            completed = []
            for line in result.stdout.splitlines():
                event = json.loads(line)
                name = event.get("Test", "")
                if name and "/" not in name and event.get("Action") in ("pass", "skip"):
                    completed.append(name)
            if sorted(completed) != sorted(names):
                raise ValueError("Executed tests differ from the discovered group")


def main():
    root = Path(__file__).resolve().parents[1]
    ledger = json.loads(subprocess.run(
        ["go", "list", "-json", "./internal/ledger"], cwd=root,
        check=True, capture_output=True, text=True).stdout)["ImportPath"]
    packages = subprocess.run(["go", "list", "./..."], cwd=root,
                              check=True, capture_output=True, text=True).stdout.splitlines()
    if packages.count(ledger) != 1:
        raise ValueError("Ledger package missing or duplicated in discovery")
    others = [package for package in packages if package != ledger]
    if others:
        subprocess.run(["go", "test", "-race", "-count=1", "-timeout=20m", *others],
                       cwd=root, check=True)
    run_ledger(root)


if __name__ == "__main__":
    main()
