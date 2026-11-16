#!/usr/bin/env python3
# main.py - замер скорости скачивания: 10 последовательных GET-запросов.

from __future__ import annotations

from asyncio import run
from sys import argv, stdout
from typing import Final
from urllib.parse import urlsplit

from measure import Result, format_report, measure
from table import Table

REQUESTS: Final[int] = 10


def _read_url() -> str:
    if len(argv) != 2:
        raise SystemExit("usage: python3 main.py URL")
    if urlsplit(argv[1]).scheme not in ("http", "https"):
        raise SystemExit("error: URL must start with http:// or https://")
    return argv[1]


def main() -> None:
    """Прочитать URL, провести замер и напечатать таблицу и итог."""
    url: str = _read_url()
    try:
        result: Result = run(measure(url, REQUESTS, Table(stdout)))
    except (OSError, EOFError, ValueError) as exc:
        raise SystemExit(f"error: {exc}") from exc
    print(f"\n{format_report(result)}")


if __name__ == "__main__":
    main()
