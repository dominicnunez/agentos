import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts.ci_scope import code_checks_needed


class ScopeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name) / "repo"
        self.repo.mkdir()
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.name", "Scope Test")
        self.git("config", "user.email", "scope@example.invalid")
        self.git("config", "core.autocrlf", "false")
        # Detached maintenance must not outlive these short-lived repositories.
        self.git("config", "maintenance.auto", "false")
        self.write("README.md", "readme")
        self.write("docs/guide.md", "guide")
        self.write("internal/main.go", "package main")
        self.base = self.commit()

    def git(self, *args):
        return subprocess.run(
            ["git", "-C", str(self.repo), *args], check=True,
            capture_output=True, text=True,
        ).stdout.strip()

    def write(self, path, body):
        target = self.repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body, encoding="utf-8")

    def commit(self):
        self.git("add", "--all")
        self.git("commit", "-qm", "test: record fixture")
        return self.git("rev-parse", "HEAD")

    def scope(self, head, base=None, event_name="push", ref="refs/heads/main", **extra):
        event = {"before": base or self.base, "after": head, **extra}
        return code_checks_needed(self.repo, event_name, event, head, ref)

    def test_ordinary_documents(self):
        paths = ["docs/guide.md", "docs/image.svg", "AGENTS.md", "README.md",
                 "SECURITY.md", "governance/aims/records/scope.md", "docs/a\nname.md",
                 "docs/caf\u00e9.md"]
        before = self.base
        for path in paths:
            with self.subTest(path=path):
                self.write(path, "new content")
                head = self.commit()
                self.assertFalse(self.scope(head, base=before))
                before = head

    def test_code_and_unknown_files(self):
        paths = ["internal/main.go", "docs/schemas/knowledge-record.schema.json",
                 "docs/example.py", ".github/workflows/ci.yml", "go.mod", "LICENSE",
                 "NOTICE", "web/dashboard/src/app.ts", "unknown.md"]
        before = self.base
        for path in paths:
            with self.subTest(path=path):
                self.write(path, "changed content")
                head = self.commit()
                self.assertTrue(self.scope(head, base=before))
                before = head

    def test_deletions_and_renames(self):
        (self.repo / "docs/guide.md").unlink()
        head = self.commit()
        self.assertFalse(self.scope(head))
        self.git("mv", "internal/main.go", "docs/main.md")
        renamed = self.commit()
        self.assertTrue(self.scope(renamed, base=head))
        (self.repo / "README.md").unlink()
        self.assertTrue(self.scope(self.commit(), base=renamed))

    def test_invalid_utf8_path_runs_code(self):
        # Git permits byte filenames that the source archive builder rejects.
        path = os.fsencode(self.repo) + b"/docs/\xff.md"
        with open(path, "wb") as document:
            document.write(b"text")
        head = self.commit()
        self.assertTrue(self.scope(head))
        event = {"pull_request": {"base": {"sha": self.base}}}
        self.assertTrue(code_checks_needed(self.repo, "pull_request", event, head, "refs/pull/1/merge"))

    def test_executable_or_link_docs(self):
        target = self.repo / "docs/guide.md"
        target.chmod(0o755)
        head = self.commit()
        self.assertTrue(self.scope(head))
        os.symlink("guide.md", self.repo / "docs/link.md")
        self.assertTrue(self.scope(self.commit(), base=head))

    def test_full_changed_file_list(self):
        for number in range(3100):
            self.write(f"docs/{number:04}.md", "text")
        self.write("zz-code.go", "package main")
        self.assertTrue(self.scope(self.commit()))

    def test_pr_checks_tested_merge(self):
        for number, (path, expected) in enumerate([("docs/new.md", False), ("new.go", True)]):
            with self.subTest(path=path):
                self.git("checkout", "-qb", f"topic-{number}")
                self.write(path, "topic change")
                self.commit()
                self.git("checkout", "-q", "main")
                self.write("docs/base.md", str(number))
                base = self.commit()
                self.git("merge", "--no-ff", "-qm", "test: merge fixture", f"topic-{number}")
                head = self.git("rev-parse", "HEAD")
                event = {"pull_request": {"base": {"sha": base}}}
                self.assertEqual(expected, code_checks_needed(self.repo, "pull_request", event, head, "refs/pull/1/merge"))

    def test_new_branch_uses_base(self):
        self.git("update-ref", "refs/remotes/origin/main", self.base)
        self.write("docs/new.md", "text")
        head = self.commit()
        self.assertFalse(self.scope(head, base="0" * 40, repository={"default_branch": "main"}))
        self.assertTrue(self.scope(head, base="0" * 40, repository={"default_branch": "missing"}))
        self.assertTrue(self.scope(head, base="0" * 40, repository={"default_branch": "main^{tree}"}))

    def test_unknown_inputs_run_code(self):
        self.write("docs/new.md", "text")
        head = self.commit()
        self.assertTrue(self.scope(head, base="f" * 40))
        self.assertTrue(self.scope(head, base="--help"))
        self.assertTrue(self.scope(head, base=self.git("rev-parse", "HEAD^{tree}")))
        self.assertTrue(self.scope(head, base=head))
        self.assertTrue(self.scope(self.base))  # Checkout does not match the supplied head.
        self.assertTrue(self.scope(head, event_name="workflow_dispatch"))
        self.assertTrue(self.scope(head, ref="refs/tags/v1.0.0"))
        self.assertTrue(self.scope(head, deleted=True))
        self.assertTrue(code_checks_needed(self.repo, "pull_request", {}, head, ""))
        self.assertTrue(code_checks_needed(self.repo, "push", {"after": self.base}, head, "refs/heads/main"))

    def test_git_failure_runs_code(self):
        with patch("scripts.ci_scope.subprocess.run", side_effect=subprocess.TimeoutExpired("git", 30)):
            self.assertTrue(self.scope(self.base))

    def test_cli_outputs_decision(self):
        self.write("docs/new.md", "text")
        head = self.commit()
        event_path = Path(self.temp.name) / "event.json"
        output_path = Path(self.temp.name) / "output"
        event_path.write_text(json.dumps({"before": self.base, "after": head}), encoding="utf-8")
        env = {**os.environ, "GITHUB_EVENT_PATH": str(event_path), "GITHUB_OUTPUT": str(output_path),
               "GITHUB_EVENT_NAME": "push", "GITHUB_SHA": head, "GITHUB_REF": "refs/heads/main"}
        script = Path(__file__).with_name("ci_scope.py").resolve()
        subprocess.run([sys.executable, str(script)], cwd=self.repo, env=env, check=True, capture_output=True)
        self.assertEqual("code=false\n", output_path.read_text(encoding="utf-8"))
        event_path.write_text("invalid JSON", encoding="utf-8")
        subprocess.run([sys.executable, str(script)], cwd=self.repo, env=env, check=True, capture_output=True)
        self.assertEqual("code=false\ncode=true\n", output_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
