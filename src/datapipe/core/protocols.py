from collections.abc import AsyncIterator, Iterable, Iterator
from typing import Protocol

type Record = dict[str, object]


class Source(Protocol):
    def records(self) -> Iterator[Record]: ...


class AsyncSource(Protocol):
    def records(self) -> AsyncIterator[Record]: ...


class Transform(Protocol):
    def apply(self, records: Iterable[Record]) -> Iterator[Record]: ...


class Sink(Protocol):
    def write(self, records: Iterable[Record]) -> int: ...
