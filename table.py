#!/usr/bin/env python3
# table.py - таблица запросов, которая обновляется в реальном времени.

from __future__ import annotations

from time import perf_counter
from typing import Final, TextIO

REDRAW_EVERY: Final[float] = 0.1


class Table:
    """Таблица запросов, строка текущего запроса обновляется на лету."""

    __slots__: tuple[str, ...] = ("_drawn", "_headed", "_index", "_live", "_out", "_start")

    def __init__(self, out: TextIO) -> None:
        self._out: TextIO = out
        self._live: bool = out.isatty()
        self._index: int = 0
        self._start: float = 0.0
        self._drawn: float = 0.0
        self._headed: bool = False

    def begin(self, index: int) -> None:
        """Начать строку нового запроса."""
        self._index = index
        self._start = self._drawn = perf_counter()

    def progress(self, size: int) -> None:
        """Обновить строку текущего запроса, не чаще раза в REDRAW_EVERY."""
        now: float = perf_counter()
        if self._live and now - self._drawn >= REDRAW_EVERY:
            self._drawn = now
            self._draw(size, now - self._start, "")

    def finish(self, size: int, elapsed: float) -> None:
        """Дописать итоговую строку запроса."""
        self._draw(size, elapsed, "\n")

    def _draw(self, size: int, elapsed: float, end: str) -> None:
        if not self._headed:
            self._header()
        speed: float = size * 8 / max(elapsed, 1e-9) / 1e6
        self._out.write(
            f"\r{self._index:>3}  {size / 1e6:>9.2f} MB  {elapsed * 1000:>7.0f} ms"
            f"  {speed:>8.2f} Mbit/s{end}"
        )
        self._out.flush()

    def _header(self) -> None:
        self._headed = True
        self._out.write(
            f"{'#':>3}  {'Downloaded':>12}  {'Time':>10}  {'Speed':>15}\n"
        )
