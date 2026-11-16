#!/usr/bin/env python3
# downloader.py - скачивание URL с редиректами через переиспользуемое соединение.

from __future__ import annotations

from asyncio import get_running_loop, timeout
from ssl import SSLContext, create_default_context
from typing import Final
from urllib.parse import SplitResult, urljoin, urlsplit

from connection import TIMEOUT, Connection, Progress, Reply

MAX_REDIRECTS: Final[int] = 5
REDIRECT_CODES: Final[frozenset[int]] = frozenset({301, 302, 303, 307, 308})
SSL_CONTEXT: Final[SSLContext] = create_default_context()


class Downloader:
    """Качает URL через одно переиспользуемое keep-alive соединение."""

    __slots__: tuple[str, ...] = ("_conn", "_origin")

    def __init__(self) -> None:
        self._conn: Connection | None = None
        self._origin: tuple[str, str] = ("", "")

    async def fetch(self, url: str, progress: Progress) -> int:
        """Скачать тело по url с учётом редиректов, вернуть его размер в байтах."""
        status: int
        location: str
        size: int
        for _ in range(MAX_REDIRECTS + 1):
            status, location, size = await self._get(urlsplit(url), progress)
            if status not in REDIRECT_CODES:
                _check_status(status, url)
                return size
            url = urljoin(url, location)
        raise SystemExit(f"error: more than {MAX_REDIRECTS} redirects")

    def close(self) -> None:
        """Закрыть соединение."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    async def _get(
        self, parts: SplitResult, progress: Progress
    ) -> tuple[int, str, int]:
        conn: Connection = await self._connection(parts)
        try:
            reply: Reply = await conn.request(_request_bytes(parts), progress)
        except (EOFError, ConnectionResetError, BrokenPipeError):
            if not conn.served:
                raise
            self.close()
            conn = await self._connection(parts)
            reply = await conn.request(_request_bytes(parts), progress)
        return reply[0], reply[1], reply[2]

    async def _connection(self, parts: SplitResult) -> Connection:
        origin: tuple[str, str] = (parts.scheme, parts.netloc)
        if self._conn is not None and not self._conn.closed and origin == self._origin:
            return self._conn
        if parts.hostname is None:
            raise ValueError(f"no host in URL {parts.geturl()}")
        self.close()
        https: bool = parts.scheme == "https"
        conn: Connection
        async with timeout(TIMEOUT):
            _, conn = await get_running_loop().create_connection(
                Connection,
                parts.hostname,
                parts.port or (443 if https else 80),
                ssl=SSL_CONTEXT if https else None,
            )
        self._conn, self._origin = conn, origin
        return conn


def _request_bytes(parts: SplitResult) -> bytes:
    path: str = parts.path or "/"
    if parts.query:
        path = f"{path}?{parts.query}"
    return (
        f"GET {path} HTTP/1.1\r\nHost: {parts.netloc}\r\n"
        "User-Agent: python-asyncio\r\nAccept-Encoding: identity\r\n\r\n"
    ).encode("ascii")


def _check_status(status: int, url: str) -> None:
    if not 200 <= status < 300:
        raise SystemExit(f"error: HTTP {status} for {url}")
