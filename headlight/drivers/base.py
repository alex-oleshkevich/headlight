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
    column_template = '{name} {type}{primary_key}{check}{unique}{null}{default}{foreign}'
    create_index_template = (
        'CREATE{unique} INDEX{concurrently}{if_not_exists}{name} ON{only} {table}{using} ({columns})'
        '{include}{with_}{tablespace}{where}'
    )
    drop_index_template = 'DROP INDEX {name}'
    index_column_template = '{expr}{collation}{opclass}{opclass_params}{sorting}{nulls}'
    unique_constraint_template = '{constraint}UNIQUE{columns}{include}'
    primary_key_constraint_template = '{constraint}PRIMARY KEY({columns}){include}'
    check_constraint_template = '{constraint}CHECK ({expr})'
    foreign_key_template = '{constraint}{self_columns}{references}{columns}{match}{on_delete}{on_update}'
    add_column_template = (
        'ALTER TABLE{if_table_exists}{only} {table} '
        'ADD{if_column_not_exists} {name} {type}{pk}{collate}{check}{unique}{null}{default}{foreign}'
    )
    drop_column_template = 'ALTER TABLE{if_table_exists}{only} {table} DROP{if_column_exists} {name}'
    add_column_default_template = 'ALTER TABLE{if_table_exists}{only} {table} ALTER {name} SET DEFAULT {expr}'
    drop_column_default_template = 'ALTER TABLE{if_table_exists}{only} {table} ALTER {name} DROP DEFAULT'

    add_column_null_template = 'ALTER TABLE{if_table_exists}{only} {table} ALTER {name} SET NOT NULL'
    drop_column_null_template = 'ALTER TABLE{if_table_exists}{only} {table} ALTER {name} DROP NOT NULL'
    change_column_type = 'ALTER TABLE{if_table_exists}{only} {table} ALTER {name} TYPE {type}{collate}{using}'
    add_table_check_template = 'ALTER TABLE{if_table_exists}{only} {table} ADD {constraint}'
    drop_table_constraint_template = 'ALTER TABLE{if_table_exists}{only} {table} DROP CONSTRAINT{if_exists} {name}'

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
        self.execute('BEGIN')
        self.execute(self.table_template.format(table=table))
        self.execute('COMMIT')

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
