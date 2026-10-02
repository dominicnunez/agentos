from pathlib import Path
import json
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from scripts.race_tests import (checked_output, main, race_plan, run_ledger,
                               selected_groups, test_groups)


class RaceTests(unittest.TestCase):
    def test_app_groups_keep_every_normal_and_race_case(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "go.mod").write_text("module example.test/race\n\ngo 1.26\n")
            ledger = root / "internal" / "ledger"
            ledger.mkdir(parents=True)
            (ledger / "fixture_test.go").write_text(
                'package ledger\nimport "testing"\nfunc TestLedger(t *testing.T) {}\n')
            app = root / "internal" / "app"
            app.mkdir(parents=True)
            (app / "race_on_test.go").write_text(
                '//go:build race\n\npackage app\nconst raceEnabled = true\n')
            (app / "race_off_test.go").write_text(
                '//go:build !race\n\npackage app\nconst raceEnabled = false\n')
            (app / "fixture_test.go").write_text('''package app
import ("fmt"; "os"; "testing")
func TestIncidentCompletionGrowth(t *testing.T) {
    for _, name := range []string{"1", "16"} {
        t.Run(name, func(t *testing.T) {
            mode := "normal"
            if raceEnabled { mode = "race" }
            if err := os.WriteFile(mode+"-completion-"+name, nil, 0600); err != nil { t.Fatal(err) }
        })
    }
}
func TestIncidentUnrelatedHistoryGrowth(t *testing.T) {
    for _, name := range []string{"0", "1000"} {
        t.Run(name, func(t *testing.T) {
            results := make(chan int, 4)
            for i := range 4 { go func() { results <- i }() }
            seen := map[int]bool{}
            for range 4 { seen[<-results] = true }
            if len(seen) != 4 { t.Fatal("lost concurrent callers") }
            mode := "normal"
            if raceEnabled { mode = "race" }
            if err := os.WriteFile(mode+"-history-"+name, nil, 0600); err != nil { t.Fatal(err) }
        })
    }
}
func TestFutureCase(t *testing.T) {
    if raceEnabled {
        if err := os.WriteFile("race-future", nil, 0600); err != nil { t.Fatal(err) }
    }
}
func Example() { fmt.Println("example"); /* Output: example */ }
''')
            with patch("scripts.race_tests.__file__", str(root / "scripts" / "race_tests.py")):
                plan = race_plan(root)
                self.assertEqual(plan, {"include": [
                    {"suite": "other", "group": 0},
                    {"suite": "app", "group": 1},
                    {"suite": "app", "group": 2},
                    {"suite": "ledger", "group": 1},
                ]})
                main("normal")
                for job in plan["include"]:
                    main(job["suite"], job["group"] if job["group"] else None)
            for name in ("normal-completion-1", "normal-completion-16",
                         "normal-history-0", "normal-history-1000",
                         "race-completion-1", "race-completion-16",
                         "race-history-0", "race-history-1000", "race-future"):
                self.assertTrue((app / name).is_file(), name)

    def test_single_group_does_not_expand_to_the_full_plan(self):
        groups = [["TestOne"], ["TestTwo"], ["TestThree"]]
        self.assertEqual(selected_groups(groups, 2), [(2, ["TestTwo"])])
        for invalid in (0, -1, 4):
            with self.subTest(group=invalid), self.assertRaises(ValueError):
                selected_groups(groups, invalid)

    def test_app_group_rejects_missing_duplicate_and_skipped_required_cases(self):
        valid = [{"Test": "TestOne", "Action": "pass"},
                 {"Test": "TestOne/child", "Action": "pass"}]
        variants = ([], valid + valid, [valid[0]],
                    [valid[0], {"Test": "TestOne/child", "Action": "skip"}])
        for events in variants:
            with self.subTest(events=events), self.assertRaises(ValueError):
                checked_output(subprocess.CompletedProcess(
                    [], 0, stdout="\n".join(json.dumps(row) for row in events), stderr=""),
                    ["TestOne"], required=["TestOne/child"])

    def test_app_group_propagates_process_failure(self):
        with self.assertRaises(subprocess.CalledProcessError):
            checked_output(subprocess.CompletedProcess([], 1, stdout="", stderr="race failure"),
                           ["TestOne"])

    def test_only_exact_grouped_packages_are_excluded(self):
        ledger = "example.test/internal/ledger"
        app = "example.test/internal/app"
        recovery = ledger + "/recovery"
        with patch("scripts.race_tests.subprocess.run") as run, \
                patch("scripts.race_tests.run_ledger") as grouped, \
                patch("scripts.race_tests.run_app") as app_suite:
            run.side_effect = [
                subprocess.CompletedProcess([], 0, stdout=json.dumps({"ImportPath": ledger})),
                subprocess.CompletedProcess([], 0, stdout=json.dumps({"ImportPath": app})),
                subprocess.CompletedProcess([], 0, stdout=ledger + "\n" + app + "\n" + recovery + "\n"),
                subprocess.CompletedProcess([], 0),
            ]
            main()
            self.assertEqual(run.call_args.args[0],
                             ["go", "test", "-race", "-count=1", "-timeout=20m", recovery])
            grouped.assert_called_once()
            app_suite.assert_called_once()

    def test_complete_unique_assignment(self):
        names = ["TestOne", "TestOneMore", "ExampleOutput", "FuzzInput", "TestNew"]
        groups = test_groups("\n".join(names + ["BenchmarkSpeed"]))
        flattened = [name for group in groups for name in group]
        self.assertCountEqual(flattened, names)
        self.assertEqual(len(flattened), len(set(flattened)))
        self.assertEqual(groups, test_groups("\n".join(reversed(names))))

    def test_invalid_discovery_fails(self):
        for listing in ("", "TestOne\nTestOne", "unexpected output", "TestOne/subtest"):
            with self.subTest(listing=listing), self.assertRaises(ValueError):
                test_groups(listing)

    def test_small_suite_has_no_empty_group(self):
        self.assertEqual(test_groups("TestOnly"), [["TestOnly"]])

    def test_long_differential_keeps_its_own_process(self):
        differential = "TestProjectionSourceGateDifferential"
        others = [f"TestFixture{index}" for index in range(40)]
        groups = test_groups("\n".join([*others, differential]))
        self.assertEqual(groups[0], [differential])
        self.assertEqual(len(groups), 9)
        self.assertCountEqual([name for group in groups for name in group],
                              [*others, differential])
        self.assertTrue(all(differential not in group for group in groups[1:]))
        self.assertTrue(all(len(group) == 5 for group in groups[1:]))

    def test_only_long_differential_has_no_empty_group(self):
        name = "TestProjectionSourceGateDifferential"
        self.assertEqual(test_groups(name), [[name]])

    def test_new_roots_remain_covered_with_isolated_differential(self):
        names = ["TestProjectionSourceGateDifferential", "TestFutureCase",
                 "ExampleFuture", "FuzzFuture"]
        groups = test_groups("\n".join(names))
        self.assertEqual(groups[0], [names[0]])
        self.assertCountEqual([name for group in groups for name in group], names)
        self.assertEqual(groups, test_groups("\n".join(reversed(names))))

    def test_eight_ordinary_groups_without_differential(self):
        names = [f"TestFixture{index}" for index in range(40)]
        groups = test_groups("\n".join(names))
        self.assertEqual(len(groups), 8)
        self.assertCountEqual([name for group in groups for name in group], names)

    def test_execution_preserves_limits_and_package_directory(self):
        with patch("scripts.race_tests.subprocess.run") as run:
            names = ["ExampleOutput", "FuzzInput", "TestOne", "TestOneMore"]
            listing = subprocess.CompletedProcess([], 0, stdout="\n".join(names))
            results = [subprocess.CompletedProcess([], 0, stdout=json.dumps(
                {"Test": name, "Action": "pass"}) + "\n", stderr="") for name in names]
            run.side_effect = [listing, listing, listing, *results]
            run_ledger(Path("/repo"))
        calls = run.call_args_list
        self.assertIn("-count=1", calls[0].args[0])
        self.assertIn("-race", calls[1].args[0])
        self.assertEqual(len(calls), 7)
        patterns = []
        for call in calls[3:]:
            self.assertEqual(call.kwargs["cwd"], Path("/repo/internal/ledger"))
            self.assertIn("test2json", call.args[0])
            self.assertIn("-test.timeout=20m", call.args[0])
            self.assertIn("-test.count=1", call.args[0])
            patterns.append(call.args[0][-1])
        self.assertIn("-test.run=^(TestOne)$", patterns)
        self.assertIn("-test.run=^(FuzzInput)$", patterns)

    def test_failed_group_fails_runner(self):
        with patch("scripts.race_tests.subprocess.run") as run:
            result = subprocess.CompletedProcess([], 0, stdout="TestOne\nTestTwo\n")
            run.side_effect = [result, result, result, subprocess.CalledProcessError(1, "test")]
            with self.assertRaises(subprocess.CalledProcessError):
                run_ledger(Path("/repo"))
            self.assertEqual(run.call_count, 4)

    def test_missing_executed_test_fails(self):
        with patch("scripts.race_tests.subprocess.run") as run:
            listing = subprocess.CompletedProcess([], 0, stdout="TestOne\n")
            empty = subprocess.CompletedProcess([], 0, stdout="", stderr="")
            run.side_effect = [listing, listing, listing, empty]
            with self.assertRaisesRegex(ValueError, "Executed tests"):
                run_ledger(Path("/repo"))

    def test_extra_executed_test_fails(self):
        with patch("scripts.race_tests.subprocess.run") as run:
            listing = subprocess.CompletedProcess([], 0, stdout="TestOne\n")
            extra = subprocess.CompletedProcess([], 0, stdout="\n".join(
                json.dumps({"Test": name, "Action": "pass"})
                for name in ("TestOne", "TestUnexpected")), stderr="")
            run.side_effect = [listing, listing, listing, extra]
            with self.assertRaisesRegex(ValueError, "Executed tests"):
                run_ledger(Path("/repo"))

    def test_nonzero_group_result_fails_runner(self):
        with patch("scripts.race_tests.subprocess.run") as run:
            listing = subprocess.CompletedProcess([], 0, stdout="TestOne\n")
            failed = subprocess.CompletedProcess([], 1, stdout="", stderr="race failure")
            run.side_effect = [listing, listing, listing, failed]
            with self.assertRaises(subprocess.CalledProcessError):
                run_ledger(Path("/repo"))

    def test_real_go_discovery_and_execution(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "go.mod").write_text("module example.test/race\n\ngo 1.26\n")
            package = root / "internal" / "ledger"
            package.mkdir(parents=True)
            (package / "marker").write_text("package working directory")
            (package / "fixture_test.go").write_text('''package ledger
import ("fmt"; "os"; "testing")
func TestDirectory(t *testing.T) {
    t.Run("child", func(t *testing.T) {
        if _, err := os.ReadFile("marker"); err != nil { t.Fatal(err) }
    })
}
func TestProjectionSourceGateDifferential(t *testing.T) {
    for _, name := range []string{"first", "middle", "last"} {
        t.Run(name, func(t *testing.T) {
            if _, err := os.ReadFile("marker"); err != nil { t.Fatal(err) }
        })
    }
}
func TestSkipped(t *testing.T) { t.Skip("intentional fixture skip") }
func Example() {
    fmt.Println("example")
    // Output: example
}
func FuzzInput(f *testing.F) {
    f.Add("seed")
    f.Fuzz(func(t *testing.T, value string) {
        if value != "seed" { t.Fatal("unexpected seed") }
    })
}
func BenchmarkUnused(b *testing.B) { b.Fatal("benchmarks must not run") }
''')
            run_ledger(root)


if __name__ == "__main__":
    unittest.main()
