from pathlib import Path
import json
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from scripts.race_tests import main, run_ledger, test_groups


class RaceTests(unittest.TestCase):
    def test_only_exact_ledger_package_is_excluded(self):
        ledger = "example.test/internal/ledger"
        recovery = ledger + "/recovery"
        with patch("scripts.race_tests.subprocess.run") as run, \
                patch("scripts.race_tests.run_ledger") as grouped:
            run.side_effect = [
                subprocess.CompletedProcess([], 0, stdout=json.dumps({"ImportPath": ledger})),
                subprocess.CompletedProcess([], 0, stdout=ledger + "\n" + recovery + "\n"),
                subprocess.CompletedProcess([], 0),
            ]
            main()
            self.assertEqual(run.call_args.args[0],
                             ["go", "test", "-race", "-count=1", "-timeout=20m", recovery])
            grouped.assert_called_once()

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
