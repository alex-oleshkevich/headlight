from __future__ import annotations

import abc
import typing
from datetime import datetime
from types import TracebackType

from headlight.schema import types

T = typing.TypeVar('T', bound='DbDriver')


class AppliedMigration(typing.TypedDict):
    name: str
    revision: str
    applied: datetime


class DbDriver(abc.ABC):
    table_template = ''
    placeholder_mark = '?'

    create_table_template = 'CREATE TABLE{if_not_exists}{name} ({column_sql})'
    drop_table_template = 'DROP TABLE {name}'
    column_template = '{name} {type}{primary_key}{unique}{null}{default}'

    @classmethod
    @abc.abstractmethod
    def from_url(cls: typing.Type[T], url: str) -> T:
        raise NotImplementedError()

    @abc.abstractmethod
    def fetch_all(self, stmt: str) -> typing.Iterable[dict]:
        ...

    @abc.abstractmethod
    def execute(self, stmt: str, params: list[str] | None = None) -> None:
        ...

    def create_migrations_table(self, table: str) -> None:
        assert self.table_template
        self.execute(self.table_template.format(table=table))

    def transaction(self) -> Transaction:
        return Transaction(self)

    def add_applied_migration(self, table: str, revision: str, name: str) -> None:
        self.execute(
            f'INSERT INTO {table} (revision, name, applied) '
            f'VALUES ({self.placeholder_mark}, {self.placeholder_mark}, {self.placeholder_mark})',
            [revision, name, datetime.now().isoformat()],
        )

    def remove_applied_migration(self, table: str, revision: str) -> None:
        self.execute(f'DELETE FROM {table} WHERE revision = {self.placeholder_mark}', [revision])

    def get_applied_migrations(self, table: str, limit: int | None = None) -> typing.Iterable[AppliedMigration]:
        stmt = f'SELECT revision, name, applied FROM {table} ORDER BY applied DESC'
        if limit:
            stmt += f' LIMIT {limit}'
        for row in self.fetch_all(stmt):
            yield {
                'revision': row[0],
                'name': row[1],
                'applied': datetime.fromisoformat(row[2]),
            }

    @abc.abstractmethod
    def get_sql_for_type(self, type: types.Type) -> str:
        raise NotImplementedError


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

    def __exit__(self, exc_type: typing.Type[Exception], exc: BaseException, tb: TracebackType) -> None:
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
