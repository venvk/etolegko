#!/usr/bin/env python3
# connection.py - одно TCP/TLS-соединение с разбором HTTP/1.1 на лету.

from __future__ import annotations

from asyncio import BaseTransport, BufferedProtocol, Future, Transport, get_running_loop, wait
from collections.abc import Callable
from enum import IntEnum
from time import perf_counter
from typing import Final

TIMEOUT: Final[float] = 30.0
BUFFER_SIZE: Final[int] = 256 * 1024
MAX_HEAD: Final[int] = 64 * 1024
NO_BODY_CODES: Final[frozenset[int]] = frozenset({204, 304})

type Progress = Callable[[int], None]
type Reply = tuple[int, str, int, bool]


class _State(IntEnum):
    HEAD = 0
    LENGTH = 1
    CHUNK_SIZE = 2
    CHUNK_DATA = 3
    CHUNK_CRLF = 4
    TRAILER = 5
    UNTIL_CLOSE = 6
    DONE = 7


class Connection(BufferedProtocol):
    """Соединение, которое разбирает HTTP-ответ по мере прихода данных.

    Тело ответа только подсчитывается и нигде не хранится.
    """

    __slots__: tuple[str, ...] = (
        "_buffer",
        "_done",
        "_head",
        "_keep_alive",
        "_left",
        "_line",
        "_location",
        "_progress",
        "_size",
        "_state",
        "_status",
        "_transport",
        "_view",
        "closed",
        "last_read",
        "served",
    )

    _buffer: bytearray
    _done: Future[Reply] | None
    _head: bytearray
    _keep_alive: bool
    _left: int
    _line: bytearray
    _location: str
    _progress: Progress
    _size: int
    _state: _State
    _status: int
    _transport: Transport | None
    _view: memoryview
    closed: bool
    last_read: float
    served: int

    def __init__(self) -> None:
        self._buffer = bytearray(BUFFER_SIZE)
        self._view = memoryview(self._buffer)
        self._transport = None
        self._done = None
        self._state = _State.DONE
        self.closed = False
        self.served = 0

    async def request(self, data: bytes, progress: Progress) -> Reply:
        """Отправить запрос и дождаться конца ответа."""
        if self._transport is None:
            raise EOFError("connection is not open")
        done: Future[Reply] = self._reset(progress)
        self._transport.write(data)
        while not done.done():
            await wait((done,), timeout=TIMEOUT)
            if not done.done() and perf_counter() - self.last_read >= TIMEOUT:
                self.close()
                raise TimeoutError(f"no data for {TIMEOUT:.0f} s")
        reply: Reply = done.result()
        self.served += 1
        if not reply[3]:
            self.close()
        return reply

    def close(self) -> None:
        """Закрыть соединение."""
        self.closed = True
        if self._transport is not None:
            self._transport.close()

    def connection_made(self, transport: BaseTransport) -> None:
        if not isinstance(transport, Transport):
            raise TypeError("stream transport expected")
        self._transport = transport

    def connection_lost(self, exc: Exception | None) -> None:
        self.closed = True
        if self._state is _State.UNTIL_CLOSE:
            self._keep_alive = False
            self._finish()
        else:
            self._fail(exc or EOFError("connection closed before the response ended"))

    def get_buffer(self, sizehint: int) -> memoryview:
        return self._view

    def buffer_updated(self, nbytes: int) -> None:
        self.last_read = perf_counter()
        try:
            self._feed(nbytes)
        except ValueError as exc:
            self._fail(exc)
            self.close()

    def _reset(self, progress: Progress) -> Future[Reply]:
        self._done = get_running_loop().create_future()
        self._progress = progress
        self._state = _State.HEAD
        self._head = bytearray()
        self._line = bytearray()
        self._size = 0
        self.last_read = perf_counter()
        return self._done

    def _feed(self, end: int) -> None:
        pos: int = 0
        while pos < end:
            match self._state:
                case _State.HEAD:
                    pos = self._feed_head(pos, end)
                case _State.LENGTH | _State.CHUNK_DATA:
                    pos = self._feed_body(pos, end)
                case _State.CHUNK_SIZE | _State.TRAILER:
                    pos = self._feed_line(pos, end)
                case _State.CHUNK_CRLF:
                    pos = self._feed_crlf(pos, end)
                case _State.UNTIL_CLOSE:
                    self._count(end - pos)
                    pos = end
                case _State.DONE:
                    return

    def _feed_head(self, pos: int, end: int) -> int:
        seen: int = len(self._head)
        self._head += self._view[pos:end]
        found: int = self._head.find(b"\r\n\r\n", max(seen - 3, 0))
        if found < 0:
            if len(self._head) > MAX_HEAD:
                raise ValueError("response headers are too large")
            return end
        self._start_body(bytes(self._head[:found]))
        return pos + found + 4 - seen

    def _start_body(self, head: bytes) -> None:
        status: int
        keep_alive: bool
        headers: dict[str, str]
        status, keep_alive, headers = _parse_head(head)
        self._status = status
        self._keep_alive = keep_alive
        self._location = headers.get("location", "")
        if self._status in NO_BODY_CODES or self._status < 200:
            self._finish()
        elif "chunked" in headers.get("transfer-encoding", "").lower():
            self._state = _State.CHUNK_SIZE
        elif "content-length" in headers:
            self._left = int(headers["content-length"])
            self._state = _State.LENGTH
            if not self._left:
                self._finish()
        else:
            self._state = _State.UNTIL_CLOSE

    def _feed_body(self, pos: int, end: int) -> int:
        take: int = min(self._left, end - pos)
        self._left -= take
        self._count(take)
        if self._left:
            return pos + take
        if self._state is _State.LENGTH:
            self._finish()
        else:
            self._state, self._left = _State.CHUNK_CRLF, 2
        return pos + take

    def _feed_crlf(self, pos: int, end: int) -> int:
        take: int = min(self._left, end - pos)
        self._left -= take
        if not self._left:
            self._state = _State.CHUNK_SIZE
        return pos + take

    def _feed_line(self, pos: int, end: int) -> int:
        newline: int = self._buffer.find(b"\n", pos, end)
        if newline < 0:
            self._line += self._view[pos:end]
            return end
        self._line += self._view[pos : newline + 1]
        line: bytes = bytes(self._line).strip()
        self._line.clear()
        if self._state is _State.CHUNK_SIZE:
            self._left = int(line.split(b";")[0], 16)
            self._state = _State.CHUNK_DATA if self._left else _State.TRAILER
        elif not line:
            self._finish()
        return newline + 1

    def _count(self, size: int) -> None:
        self._size += size
        self._progress(self._size)

    def _finish(self) -> None:
        self._state = _State.DONE
        if self._done is not None and not self._done.done():
            self._done.set_result(
                (self._status, self._location, self._size, self._keep_alive)
            )

    def _fail(self, exc: Exception) -> None:
        self._state = _State.DONE
        if self._done is not None and not self._done.done():
            self._done.set_exception(exc)


def _parse_head(head: bytes) -> tuple[int, bool, dict[str, str]]:
    status_line: str
    lines: list[str]
    status_line, *lines = head.decode("latin-1").split("\r\n")
    version: str
    code: str
    version, code = status_line.split(" ", 2)[:2]
    headers: dict[str, str] = {}
    line: str
    name: str
    sep: str
    value: str
    for line in lines:
        name, sep, value = line.partition(":")
        if sep:
            headers[name.strip().lower()] = value.strip()
    connection: str = headers.get("connection", "").lower()
    keep_alive: bool = connection != "close" and (
        version != "HTTP/1.0" or connection == "keep-alive"
    )
    return int(code), keep_alive, headers
