"""Utility helpers for interacting with the `ipsw` CLI tool.

The original version of this script simply listed a couple of commands to be
executed manually which meant that importing the module immediately raised a
syntax error.  To make it usable we provide tiny wrappers that forward the
requested action to the `ipsw` command line client via :mod:`subprocess`.
"""

from __future__ import annotations

import subprocess
from typing import Iterable, List


def _run_ipsw(args: Iterable[str]) -> subprocess.CompletedProcess[str]:
    """Execute the ``ipsw`` CLI with *args* and return the completed process.

    ``check=True`` ensures we raise immediately if the CLI is unavailable or the
    command fails which mirrors the expectations of the previous manual
    invocation notes.
    """

    cmd: List[str] = ["ipsw", *args]
    return subprocess.run(cmd, check=True, capture_output=True, text=True)


def list_models() -> str:
    """Return the output of ``ipsw list-models``."""

    return _run_ipsw(["list-models"]).stdout


def get_kernelcache_key(model: str, os_version: str) -> str:
    """Fetch the KernelCache key for *model* and *os_version*."""

    return _run_ipsw(
        ["key", "--model", model, "--os", os_version, "--type", "KernelCache"]
    ).stdout


if __name__ == "__main__":
    print(list_models())
    print(get_kernelcache_key("iPad7,5", "17.6.1"))
