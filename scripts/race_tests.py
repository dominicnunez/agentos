"""Run every test under race, with independently runnable complete-root groups."""

import argparse
import json
from pathlib import Path
import re
import subprocess
import tempfile


def test_groups(listing, count=8):
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
    groups = []
    # This complete differential stream is the longest individual root. Keep
    # its deadline independent of unrelated fixtures without splitting cases.
    differential = "TestProjectionSourceGateDifferential"
    if differential in names:
        groups.append([differential])
        names.remove(differential)
    groups.extend(names[index::count] for index in range(min(count, len(names))))
    return groups


def selected_groups(groups, group):
    if group is None:
        return list(enumerate(groups, 1))
    if not 1 <= group <= len(groups):
        raise ValueError("Race group is outside the discovered plan")
    return [(group, groups[group - 1])]


def run_ledger(root, group=None):
    package = root / "internal" / "ledger"
    # Keep one complete process run to detect interactions across group boundaries.
    if group is None:
        subprocess.run(["go", "test", "-count=1", "-timeout=20m", "./internal/ledger"],
                       cwd=root, check=True)
    with tempfile.TemporaryDirectory(prefix="agentos-race-") as temp:
        binary = str(Path(temp) / "ledger.test")
        subprocess.run(["go", "test", "-race", "-c", "-o", binary, "./internal/ledger"],
                       cwd=root, check=True)
        listing = subprocess.run([binary, "-test.list=."], cwd=package,
                                 check=True, capture_output=True, text=True).stdout
        groups = test_groups(listing)
        for index, names in selected_groups(groups, group):
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


def checked_output(result, expected, required=()):
    print(result.stdout, end="", flush=True)
    print(result.stderr, end="", flush=True)
    result.check_returncode()
    events = [json.loads(line) for line in result.stdout.splitlines()]
    completed = [event.get("Test", "") for event in events
                 if event.get("Test") and event.get("Action") in ("pass", "skip")]
    roots = [name for name in completed if "/" not in name]
    if sorted(roots) != sorted(expected):
        raise ValueError("Executed tests differ from the discovered app group")
    for name in required:
        if sum(event.get("Test") == name and event.get("Action") == "pass"
               for event in events) != 1:
            raise ValueError(f"Required app case did not pass exactly once: {name}")


def run_app(root, group=None):
    package = root / "internal" / "app"
    with tempfile.TemporaryDirectory(prefix="agentos-app-race-") as temp:
        binary = str(Path(temp) / "app.test")
        subprocess.run(["go", "test", "-race", "-c", "-o", binary, "./internal/app"],
                       cwd=root, check=True)
        listing = subprocess.run([binary, "-test.list=."], cwd=package,
                                 check=True, capture_output=True, text=True).stdout
        groups = test_groups(listing, count=2)
        names = [name for names in groups for name in names]
        completion = "TestIncidentCompletionGrowth"
        history = "TestIncidentUnrelatedHistoryGrowth"
        if completion not in names or history not in names:
            raise ValueError("App growth coverage policy no longer matches discovery")
        if group is None:
            print("Complete normal app suite", flush=True)
            normal = subprocess.run(
                ["go", "test", "-json", "-count=1", "-timeout=20m", "./internal/app"],
                cwd=root, capture_output=True, text=True)
            checked_output(normal, names,
                           required=(completion + "/1", completion + "/16",
                                     history + "/0", history + "/1000"))
        for index, names in selected_groups(groups, group):
            print(f"App race group {index}/{len(groups)}: {len(names)} tests", flush=True)
            pattern = "^(" + "|".join(re.escape(name) for name in names) + ")$"
            race = subprocess.run(
                ["go", "tool", "test2json", "-t", binary, "-test.v=test2json",
                 "-test.count=1", "-test.timeout=20m", "-test.run=" + pattern],
                cwd=package, capture_output=True, text=True)
            required = []
            if completion in names:
                required.extend((completion + "/1", completion + "/16"))
            if history in names:
                required.extend((history + "/0", history + "/1000"))
            checked_output(race, names, required=required)


def race_plan(root):
    jobs = [{"suite": "other", "group": 0}]
    with tempfile.TemporaryDirectory(prefix="agentos-race-plan-") as temp:
        for suite, count in (("app", 2), ("ledger", 8)):
            package = root / "internal" / suite
            binary = str(Path(temp) / (suite + ".test"))
            subprocess.run(["go", "test", "-race", "-c", "-o", binary,
                            "./internal/" + suite], cwd=root, check=True)
            listing = subprocess.run([binary, "-test.list=."], cwd=package,
                                     check=True, capture_output=True, text=True).stdout
            groups = test_groups(listing, count=count)
            jobs.extend({"suite": suite, "group": index}
                        for index in range(1, len(groups) + 1))
    return {"include": jobs}


def main(suite="all", group=None):
    root = Path(__file__).resolve().parents[1]
    if suite == "plan":
        print(json.dumps(race_plan(root)))
        return
    if suite == "normal":
        subprocess.run(["go", "test", "-json", "-count=1", "-timeout=20m", "./..."],
                       cwd=root, check=True)
        return
    if suite == "app":
        run_app(root, group=group)
        return
    if suite == "ledger":
        run_ledger(root, group=group)
        return
    ledger = json.loads(subprocess.run(
        ["go", "list", "-json", "./internal/ledger"], cwd=root,
        check=True, capture_output=True, text=True).stdout)["ImportPath"]
    app = json.loads(subprocess.run(
        ["go", "list", "-json", "./internal/app"], cwd=root,
        check=True, capture_output=True, text=True).stdout)["ImportPath"]
    packages = subprocess.run(["go", "list", "./..."], cwd=root,
                              check=True, capture_output=True, text=True).stdout.splitlines()
    if packages.count(ledger) != 1 or packages.count(app) != 1 or ledger == app:
        raise ValueError("Ledger/app package missing or duplicated in discovery")
    if suite == "all":
        run_app(root)
    others = [package for package in packages if package not in (ledger, app)]
    if others:
        subprocess.run(["go", "test", "-race", "-count=1", "-timeout=20m", *others],
                       cwd=root, check=True)
    if suite == "all":
        run_ledger(root)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", default="all",
                        choices=("all", "normal", "plan", "app", "ledger", "other"))
    parser.add_argument("--group", type=int)
    args = parser.parse_args()
    if args.group is not None and args.suite not in ("app", "ledger"):
        parser.error("--group requires --suite app or ledger")
    main(args.suite, args.group)
