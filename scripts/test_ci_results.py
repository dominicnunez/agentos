import itertools
import unittest

from scripts.check_ci_results import checks_pass


class CIResultsTests(unittest.TestCase):
    def test_only_complete_applicable_job_sets_pass(self):
        accepted = {
            ("true", "success", "success", "success"),
            ("false", "success", "success", "skipped"),
        }
        states = ("success", "failure", "cancelled", "skipped", "")
        for row in itertools.product(("true", "false", "unknown", ""),
                                     states, states, states):
            with self.subTest(row=row):
                self.assertEqual(checks_pass(*row), row in accepted)


if __name__ == "__main__":
    unittest.main()
