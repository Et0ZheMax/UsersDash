"""Utility script to spot obvious syntax issues in project assets.

The checker performs three lightweight steps:
- byte-compiles all Python modules to surface syntax errors;
- validates every JSON file to ensure it can be loaded;
- parses HTML templates to make sure they are at least structurally readable.

It prints a short summary and exits with a non-zero status code when any check fails.
"""
from __future__ import annotations

import compileall
import sys
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
import json
from typing import Iterable, List

ROOT = Path(__file__).resolve().parent


@dataclass
class CheckResult:
    name: str
    errors: List[str] = field(default_factory=list)

    def add_error(self, message: str) -> None:
        self.errors.append(message)

    @property
    def ok(self) -> bool:
        return not self.errors


class _SilentHTMLParser(HTMLParser):
    """HTMLParser subclass that records parsing errors."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.errors: List[str] = []

    def error(self, message: str) -> None:  # type: ignore[override]
        self.errors.append(message)


def _walk_files(patterns: Iterable[str]) -> Iterable[Path]:
    for pattern in patterns:
        yield from ROOT.rglob(pattern)


def check_python() -> CheckResult:
    result = CheckResult("python")
    ok = compileall.compile_dir(str(ROOT), quiet=1, force=True)
    if not ok:
        result.add_error("Python byte-compilation failed; see stdout for details.")
    return result


def check_json() -> CheckResult:
    result = CheckResult("json")
    for path in _walk_files(["*.json"]):
        try:
            with path.open("r", encoding="utf-8") as fh:
                json.load(fh)
        except Exception as exc:  # pragma: no cover - runtime guard
            result.add_error(f"{path.relative_to(ROOT)}: {exc}")
    return result


def check_html() -> CheckResult:
    result = CheckResult("html")
    parser = _SilentHTMLParser()

    for path in _walk_files(["*.html"]):
        parser.reset()
        parser.errors.clear()
        try:
            parser.feed(path.read_text(encoding="utf-8"))
            parser.close()
        except Exception as exc:  # pragma: no cover - runtime guard
            result.add_error(f"{path.relative_to(ROOT)}: {exc}")
            continue

        if parser.errors:
            result.add_error(
                f"{path.relative_to(ROOT)}: parsing reported {len(parser.errors)} errors"
            )
    return result


def main() -> int:
    checks = [check_python(), check_json(), check_html()]

    for check in checks:
        status = "OK" if check.ok else "FAIL"
        print(f"[{status}] {check.name}")
        for err in check.errors:
            print(f"  - {err}")

    has_errors = any(not check.ok for check in checks)
    return 1 if has_errors else 0


if __name__ == "__main__":
    sys.exit(main())
