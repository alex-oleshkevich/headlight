from __future__ import annotations

import psycopg2

from headlight.drivers.base import DbDriver


class PgDriver(DbDriver):
    placeholder_mark = '%s'
    table_template = 'CREATE TABLE IF NOT EXISTS {table} (revision TEXT PRIMARY KEY, name TEXT, applied TEXT)'

    def __init__(self, url: str) -> None:
        self.conn = psycopg2.connect(url)

    @classmethod
    def from_url(cls, url: str) -> PgDriver:
        return cls(url)
