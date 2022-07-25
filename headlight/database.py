from __future__ import annotations

import typing
from urllib.parse import urlparse

from headlight.drivers.base import DbDriver
from headlight.drivers.postgresql import PgDriver
from headlight.drivers.sqlite import SqliteDriver
from headlight.exceptions import HeadlightError

drivers: dict[str, typing.Type[DbDriver]] = {
    'postgresql': PgDriver,
    'sqlite': SqliteDriver,
}


def create_database(url: str) -> DbDriver:
    parts = urlparse(url)
    driver_class = drivers.get(parts.scheme)
    if not driver_class:
        raise HeadlightError('Unknown driver type: %s.' % parts.scheme)
    return driver_class.from_url(url)
