"""Keep the required CI result dependent on every applicable job."""

import os


def checks_pass(code, plan, checks, race):
    if plan != "success" or checks != "success":
        return False
    if code == "true":
        return race == "success"
    if code == "false":
        return race == "skipped"
    return False


if __name__ == "__main__":
    states = tuple(os.environ.get(name, "") for name in
                   ("APPLICABLE_CODE", "PLAN_RESULT", "CHECKS_RESULT", "RACE_RESULT"))
    if not checks_pass(*states):
        raise SystemExit("Applicable CI jobs did not all succeed: " + repr(states))
