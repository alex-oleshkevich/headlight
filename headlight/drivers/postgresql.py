from __future__ import annotations

import psycopg2
import typing

from headlight.drivers.base import DbDriver


class PgDriver(DbDriver):
    placeholder_mark = '%s'
    table_template = (
        'CREATE TABLE IF NOT EXISTS {table} '
        '(revision TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, applied TEXT NOT NULL)'
    )

    def __init__(self, url: str) -> None:
        self.conn = psycopg2.connect(url)

    @classmethod
    def from_url(cls, url: str) -> PgDriver:
        return cls(url)

    def fetch_all(self, stmt: str) -> typing.Iterable[dict]:
        cursor = self.conn.cursor()
        cursor.execute(stmt)
        for row in cursor.fetchall():
            yield row

    def execute(self, stmt: str, params: list[str] | None = None) -> None:
        cursor = self.conn.cursor()
        cursor.execute(stmt, params or [])
