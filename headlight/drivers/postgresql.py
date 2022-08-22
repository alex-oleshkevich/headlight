from __future__ import annotations

import psycopg2
import typing

from headlight.drivers.base import DbDriver
from headlight.schema import types


class PgDriver(DbDriver):
    placeholder_mark = "%s"

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

    def get_sql_for_type(self, type: types.Type) -> str:
        match type:
            case types.SmallIntegerType(auto_increment=auto_increment):
                return "SMALLSERIAL" if auto_increment else "SMALLINT"
            case types.IntegerType(auto_increment=auto_increment):
                return "SERIAL" if auto_increment else "INTEGER"
            case types.BigIntegerType(auto_increment=auto_increment):
                return "BIGSERIAL" if auto_increment else "BIGINT"
            case types.RealType():
                return "REAL"
            case types.DoubleType():
                return "DOUBLE PRECISION"
            case types.FloatType(precision=precision):
                return f"FLOAT({precision})" if precision is not None else "FLOAT"
            case types.NumericType(precision=precision, scale=scale):
                if precision is not None and scale is not None:
                    return f"NUMERIC({precision}, {scale})"
                elif precision is not None:
                    return f"NUMERIC({precision})"
                else:
                    return "NUMERIC"
            case types.MoneyType():
                return "MONEY"
            case types.CharType(length=length):
                return f"CHAR({length})"
            case types.VarCharType(length=length):
                return f"VARCHAR({length})" if length else "VARCHAR"
            case types.TextType():
                return "TEXT"
            case types.BytesType():
                return "BYTEA"
            case types.DateTimeType(tz=tz, precision=precision):
                return "TIMESTAMP{precision}{timezone}".format(
                    precision=f"({precision})" if precision else "",
                    timezone=" WITH TIME ZONE" if tz else "",
                )
            case types.DateType():
                return "DATE"
            case types.TimeType(tz=tz, precision=precision):
                return "TIME{precision}{timezone}".format(
                    precision=f"({precision})" if precision else "",
                    timezone=" WITH TIME ZONE" if tz else "",
                )
            case types.IntervalType(fields=fields, precision=precision):
                return "INTERVAL{fields}{precision}".format(
                    fields=f" {fields}" if fields else "",
                    precision=f"({precision})" if precision else "",
                )
            case types.BooleanType():
                return "BOOLEAN"
            case types.PointType():
                return "POINT"
            case types.LineType():
                return "LINE"
            case types.LsegType():
                return "LSEG"
            case types.BoxType():
                return "BOX"
            case types.PathType():
                return "PATH"
            case types.PolygonType():
                return "POLYGON"
            case types.CircleType():
                return "CIRCLE"
            case types.CIDRType():
                return "CIDR"
            case types.InetType():
                return "INET"
            case types.MacAddrType():
                return "MACADDR"
            case types.MacAddr8Type():
                return "MACADDR8"
            case types.JSONType():
                return "JSONB"
            case types.JSONType():
                return "JSONB"
            case types.ArrayType(type_=type_):
                type_sql = self.get_sql_for_type(type_)
                return f"{type_sql}[]"
            case types.UUIDType():
                return "UUID"

        raise ValueError(f"Cannot generate SQL for type: {type:r}")
