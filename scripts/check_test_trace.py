"""Require new Rust tests in a PR to name their specification and matrix row."""

import re
import subprocess
import sys
from pathlib import Path


TEST_ATTRIBUTE = re.compile(r"#\[(?:\w+::)?test(?:\b|\()")
HUNK = re.compile(r"^@@ .* \+(\d+)(?:,\d+)? @@")
FUNCTION = re.compile(r"\b(?:async\s+)?fn\s+(\w+)\b")


def added_tests(base: str):
    diff = subprocess.run(
        ["git", "diff", "--unified=0", "--diff-filter=ACMR", f"{base}...HEAD", "--", "*.rs"],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout
    path = None
    line = 0
    for change in diff.splitlines():
        if change.startswith("+++ b/"):
            path = change[6:]
        elif match := HUNK.match(change):
            line = int(match.group(1))
        elif change.startswith("+") and not change.startswith("+++"):
            if TEST_ATTRIBUTE.search(change[1:]):
                yield path, line
            line += 1
        elif change.startswith(" "):
            line += 1


def main(base: str) -> int:
    matrix = Path("specs/traceability/verification.md").read_text(encoding="utf-8")
    rows = [row.split("|") for row in matrix.splitlines() if row.startswith("|")]
    failures = []
    source_lines = {}
    for path, line in added_tests(base):
        if path not in source_lines:
            source_lines[path] = subprocess.run(
                ["git", "show", f"HEAD:{path}"],
                check=True,
                capture_output=True,
                encoding="utf-8",
                text=True,
            ).stdout.splitlines()
        lines = source_lines[path]
        previous = line - 2
        while previous >= 0 and lines[previous].lstrip().startswith(("///", "#[")):
            previous -= 1
        comments = lines[previous + 1 : line - 1]
        if not any(comment.lstrip().startswith("/// Trace: ") for comment in comments):
            failures.append(f"{path}:{line}: missing adjacent /// Trace: comment")
        function = next((FUNCTION.search(item) for item in lines[line : line + 6] if FUNCTION.search(item)), None)
        if function is None:
            failures.append(f"{path}:{line}: cannot locate test function")
            continue
        name = function.group(1)
        if not any(len(row) > 3 and row[1].strip().endswith(f"::{name}") and row[3].strip() == path for row in rows):
            failures.append(f"{path}:{line}: missing verification matrix row for {name}")
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print("New Rust tests have Trace comments and verification matrix rows.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
