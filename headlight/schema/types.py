from __future__ import annotations

import abc
import typing

if typing.TYPE_CHECKING:
    from headlight import DbDriver


class Type(abc.ABC):
    @abc.abstractmethod
    def get_sql(self, driver: DbDriver) -> str:
        raise NotImplementedError()


class StringType(Type):
    def __init__(self, length: int) -> None:
        self.length = length

    def get_sql(self, driver: DbDriver) -> str:
        return driver.get_sql_for_type(self)


class TextType(Type):

    def get_sql(self, driver: DbDriver) -> str:
        return driver.get_sql_for_type(self)


class IntegerType(Type):
    type = 'INTEGER'

    def __init__(self, auto_increment: bool = False) -> None:
        self.auto_increment = auto_increment

    def get_sql(self, driver: DbDriver) -> str:
        return driver.get_sql_for_type(self)
