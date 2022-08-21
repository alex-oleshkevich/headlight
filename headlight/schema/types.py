from __future__ import annotations

import abc
import typing

if typing.TYPE_CHECKING:
    from headlight import DbDriver


class Type(abc.ABC):
    def get_sql(self, driver: DbDriver) -> str:
        return driver.get_sql_for_type(self)


class CharType(Type):
    def __init__(self, length: int) -> None:
        self.length = length


class VarCharType(Type):
    def __init__(self, length: int | None = None) -> None:
        self.length = length


class TextType(Type):
    pass


class SmallIntegerType(Type):
    def __init__(self, auto_increment: bool = False) -> None:
        self.auto_increment = auto_increment


class IntegerType(Type):
    def __init__(self, auto_increment: bool = False) -> None:
        self.auto_increment = auto_increment


class BigIntegerType(Type):
    def __init__(self, auto_increment: bool = False) -> None:
        self.auto_increment = auto_increment


class RealType(Type):
    pass


class DoubleType(Type):
    pass


class FloatType(Type):
    def __init__(self, precision: int | None = None) -> None:
        self.precision = precision


class NumericType(Type):
    def __init__(self, precision: int | None = None, scale: int | None = None) -> None:
        self.precision = precision
        self.scale = scale


class MoneyType(Type):
    pass


class BytesType(Type):
    pass


class DateTimeType(Type):
    def __init__(self, tz: bool = False, precision: int | None = None) -> None:
        if precision:
            assert 0 <= precision <= 6

        self.tz = tz
        self.precision = precision


class DateType(Type):
    def __init__(self, precision: int | None = None) -> None:
        if precision:
            assert 0 <= precision <= 6

        self.precision = precision


class TimeType(Type):
    def __init__(self, tz: bool = False, precision: int | None = None) -> None:
        if precision:
            assert 0 <= precision <= 6

        self.tz = tz
        self.precision = precision


IntervalField = typing.Literal[
    'YEAR',
    'MONTH',
    'DAY',
    'HOUR',
    'MINUTE',
    'SECOND',
    'YEAR TO MONTH',
    'DAY TO HOUR',
    'DAY TO MINUTE',
    'DAY TO SECOND',
    'HOUR TO MINUTE',
    'HOUR TO SECOND',
    'MINUTE TO SECOND',
]


class IntervalType(Type):
    def __init__(self, fields: IntervalField | None = None, precision: int | None = None) -> None:
        if precision:
            assert 0 <= precision <= 6

        self.fields = fields
        self.precision = precision


class BooleanType(Type):
    pass


class PointType(Type):
    pass


class LineType(Type):
    pass


class LsegType(Type):
    pass


class BoxType(Type):
    pass


class PathType(Type):
    pass


class PolygonType(Type):
    pass


class CircleType(Type):
    pass


class CIDRType(Type):
    pass


class InetType(Type):
    pass


class MacAddrType(Type):
    pass


class MacAddr8Type(Type):
    pass


class JSONType(Type):
    pass


class ArrayType(Type):
    def __init__(self, type_: Type) -> None:
        self.type_ = type_


class UUIDType(Type):
    pass
