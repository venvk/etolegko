#!/usr/bin/env python3
# measure.py - серия последовательных скачиваний и итоговый отчёт.

from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter

from downloader import Downloader
from table import Table


@dataclass(slots=True)
class Result:
    """Время каждого запроса и общий объём скачанного."""

    times: list[float]
    total_bytes: int


async def measure(url: str, count: int, table: Table) -> Result:
    """Сделать count последовательных скачиваний url, выводя строки в table."""
    downloader: Downloader = Downloader()
    times: list[float] = []
    total: int = 0
    index: int
    try:
        for index in range(1, count + 1):
            table.begin(index)
            start: float = perf_counter()
            size: int = await downloader.fetch(url, table.progress)
            elapsed: float = perf_counter() - start
            table.finish(size, elapsed)
            times.append(elapsed)
            total += size
    finally:
        downloader.close()
    return Result(times=times, total_bytes=total)


def format_report(result: Result) -> str:
    """Собрать итоговые строки (единицы СИ: 1 MB = 10^6 байт)."""
    elapsed: float = sum(result.times)
    if elapsed <= 0:
        raise ValueError("elapsed time must be positive")
    rate: float = result.total_bytes / elapsed
    return "\n".join(
        (
            f"Requests:        {len(result.times)}",
            f"Average time:    {elapsed / len(result.times) * 1000:.1f} ms",
            f"Downloaded:      {result.total_bytes / 1e6:.2f} MB"
            f" ({result.total_bytes} bytes)",
            f"Download speed:  {rate * 8 / 1e6:.2f} Mbit/s ({rate / 1e6:.2f} MB/s)",
        )
    )
