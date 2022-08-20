from __future__ import annotations

import dataclasses

import abc

from headlight.drivers.base import DbDriver
from headlight.schema.types import Type


@dataclasses.dataclass
class CheckConstraint:
    expr: str
    name: str | None = None

    def __str__(self) -> str:
        expr = self.expr.replace('%', '%%')
        stmt = f'CHECK ({expr})'
        if self.name:
            stmt = f'CONSTRAINT {self.name} {stmt}'
        return stmt


@dataclasses.dataclass
class UniqueConstraint:
    name: str | None = None
    include: list[str] | None = None
    columns: list[str] | None = None

    def __str__(self) -> str:
        stmt = 'UNIQUE'
        if self.name:
            stmt = f'CONSTRAINT {self.name} {stmt}'
        if self.columns:
            stmt += ' (%s)' % ', '.join(self.columns)
        if self.include:
            stmt += ' INCLUDE (%s)' % ', '.join(self.include)
        return stmt


@dataclasses.dataclass
class Column:
    name: str
    type: Type
    null: bool = False
    default: str | None = None
    primary_key: bool = False
    auto_increment: bool = False
    if_not_exists: bool = False
    comment: str | None = None
    unique_constraint: UniqueConstraint | None = None
    check_constraint: CheckConstraint | None = None

    def check(self, expr: str, name: str | None = None) -> Column:
        self.check_constraint = CheckConstraint(expr, name)
        return self

    def unique(self, name: str | None = None) -> Column:
        self.unique_constraint = UniqueConstraint(name)
        return self


class Operation(abc.ABC):
    @abc.abstractmethod
    def to_up_sql(self, driver: DbDriver) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def to_down_sql(self, driver: DbDriver) -> str:
        raise NotImplementedError()


class RunSqlOp(Operation):
    def __init__(self, up_sql: str, down_sql: str) -> None:
        self.up_sql = up_sql
        self.down_sql = down_sql

    def to_up_sql(self, driver: DbDriver) -> str:
        return self.up_sql

    def to_down_sql(self, driver: DbDriver) -> str:
        return self.down_sql


class AddColumnOp(Operation):
    def __init__(
        self,
        column_name: str,
        column_type: Type,
        null: bool = False,
        default: str | None = None,
        unique: bool = False,
        primary_key: bool = False,
        auto_increment: bool = False,
        if_not_exists: bool = False,
        comment: str | None = None,
    ) -> None:
        self._column_name = column_name
        self._column_type = column_type
        self._null = null
        self._default = default
        self._unique = unique
        self._primary_key = primary_key
        self._auto_increment = auto_increment
        self._if_not_exists = if_not_exists
        self._comment = comment


class CreateTableOp(Operation):
    def __init__(
        self,
        table_name: str,
        if_not_exists: bool = False,
    ) -> None:
        self.table_name = table_name
        self.if_not_exists = if_not_exists
        self.columns: list[Column] = []
        self.checks: list[CheckConstraint] = []
        self.extra_ops: list[Operation] = []
        self.unique: UniqueConstraint | None = None

    def add_column(self, column: Column) -> Column:
        self.columns.append(column)
        return column

    def add_check_constraint(self, expr: str, name: str | None = None) -> None:
        self.checks.append(CheckConstraint(expr, name))

    def add_unique_constraint(
        self,
        columns: list[str],
        name: str | None = None,
        include: list[str] | None = None,
    ) -> None:
        self.unique = UniqueConstraint(name=name, include=include, columns=columns)

    def to_up_sql(self, driver: DbDriver) -> str:
        pk_cols = [col for col in self.columns if col.primary_key]
        pk_count = len(pk_cols)

        column_stmts = [
            '    '
            + driver.column_template.format(
                name=column.name,
                null='' if column.null else ' NOT NULL',
                check=f' {column.check_constraint}' if column.check_constraint else '',
                unique=f' {column.unique_constraint}' if column.unique_constraint else '',
                type=driver.get_sql_for_type(column.type),
                default=f" DEFAULT '{column.default}'" if column.default is not None else '',
                primary_key=' PRIMARY KEY' if pk_count == 1 and column.primary_key else '',
            )
            for column in self.columns
        ]

        if pk_count > 1:
            column_stmts.append('    PRIMARY KEY (%s)' % ', '.join([col.name for col in pk_cols]))

        if self.checks:
            for check in self.checks:
                column_stmts.append('    ' + str(check))

        if self.unique:
            column_stmts.append(f'    {self.unique}')

        return driver.create_table_template.format(
            name=self.table_name,
            column_sql='\n' + ',\n'.join(column_stmts) + '\n',
            if_not_exists=' IF NOT EXISTS' if self.if_not_exists else ' ',
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        return driver.drop_table_template.format(name=self.table_name)
