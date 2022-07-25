from __future__ import annotations

import sqlite3
import typing
from urllib.parse import urlparse

from headlight.drivers.base import DbDriver


class SqliteDriver(DbDriver):
    table_template = 'CREATE TABLE IF NOT EXISTS {table} (revision TEXT PRIMARY KEY, name TEXT, applied TEXT)'

    def __init__(self, path: str) -> None:
        self.conn = sqlite3.connect(path)

    @classmethod
    def from_url(cls, url: str) -> SqliteDriver:
        parts = urlparse(url)
        return cls(path=parts.netloc or parts.path)

    def create_migrations_table(self, table: str) -> None:
        self.execute(self.table_template.format(table=table))

    def execute(self, stmt: str, params: list[str] | None = None) -> None:
        cursor = self.conn.cursor()
        cursor.execute(stmt, params or [])

    def fetch_all(self, stmt: str) -> typing.Iterable[dict]:
        cursor = self.conn.cursor()
        for row in cursor.execute(stmt):
            yield row
