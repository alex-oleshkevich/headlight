from __future__ import annotations

import abc
import types
import typing
from datetime import datetime

T = typing.TypeVar('T')


class AppliedMigration(typing.TypedDict):
    name: str
    revision: str
    applied: str


class DbDriver(abc.ABC):
    @abc.abstractclassmethod
    def from_url(cls: typing.Type[T]) -> T:
        ...

    @abc.abstractmethod
    def create_migrations_table(self, table: str) -> None:
        ...

    @abc.abstractmethod
    def execute(self, stmt: str, params: list[str] | None = None) -> None:
        ...

    @abc.abstractmethod
    def fetch_all(self, stmt: str) -> typing.Iterable[dict]:
        ...

    def transaction(self) -> Transaction:
        return Transaction(self)

    def add_applied_migration(self, table: str, revision: str, name: str) -> None:
        self.execute(
            f'INSERT INTO {table} (revision, name, applied) VALUES (?, ?, ?)',
            [revision, name, datetime.now().isoformat()],
        )

    def get_applied_migrations(self, table: str) -> typing.Iterable[dict]:
        stmt = f'SELECT revision, name, applied FROM {table}'
        for row in self.fetch_all(stmt):
            yield {
                'revision': row[0],
                'name': row[1],
                'applied': datetime.fromisoformat(row[2]),
            }


class Transaction:
    def __init__(self, db: DbDriver) -> None:
        self._db = db

    def begin(self) -> Transaction:
        self._db.execute('BEGIN')
        return self

    def commit(self) -> None:
        self._db.execute('COMMIT')

    def rollback(self) -> None:
        self._db.execute('ROLLBACK')

    def __enter__(self) -> Transaction:
        return self.begin()

    def __exit__(self, exc_type: typing.Type[Exception], exc: BaseException, tb: types.TracebackType) -> None:
        if exc:
            self.rollback()
        else:
            self.commit()


class DummyTransaction(Transaction):
    def begin(self) -> Transaction:
        return self

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass
