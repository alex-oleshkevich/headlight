from __future__ import annotations

import dataclasses

import typing

from headlight.drivers.base import DbDriver
from headlight.schema import types

Action = typing.Literal['RESTRICT', 'CASCADE', 'NO ACTION', 'SET NULL', 'SET DEFAULT']
MatchType = typing.Literal['FULL', 'PARTIAL', 'SIMPLE']


@dataclasses.dataclass
class IndexExpr:
    column: str
    collation: str = ''
    opclass: str = ''
    opclass_params: str = ''
    sorting: typing.Literal['ASC', 'DESC'] | None = None
    nulls: typing.Literal['FIRST', 'LAST'] | None = None


@dataclasses.dataclass
class Constraint:
    def compile(self, driver: DbDriver) -> str:
        raise NotImplementedError()


@dataclasses.dataclass
class CheckConstraint(Constraint):
    expr: str
    name: str | None = None

    def compile(self, driver: DbDriver) -> str:
        expr = self.expr.replace('%', '%%')
        return driver.check_constraint_template.format(
            expr=expr,
            constraint=f'CONSTRAINT {self.name} ' if self.name else '',
        )


@dataclasses.dataclass
class UniqueConstraint(Constraint):
    name: str | None = None
    include: list[str] | None = None
    columns: list[str] | None = None

    def compile(self, driver: DbDriver) -> str:
        return driver.unique_constraint_template.format(
            constraint=f'CONSTRAINT {self.name} ' if self.name else '',
            columns=' (%s)' % ', '.join(self.columns) if self.columns else '',
            include=' INCLUDE (%s)' % ', '.join(self.include) if self.include else '',
        )


@dataclasses.dataclass
class PrimaryKeyConstraint(Constraint):
    name: str | None
    columns: list[str]
    include: list[str] = dataclasses.field(default_factory=list)

    def compile(self, driver: DbDriver) -> str:
        return driver.primary_key_constraint_template.format(
            constraint=f'CONSTRAINT {self.name} ' if self.name else '',
            columns=', '.join(self.columns) if self.columns else '',
            include=' INCLUDE (%s)' % ', '.join(self.include) if self.include else '',
        )


@dataclasses.dataclass
class ForeignKey(Constraint):
    target_table: str
    target_columns: list[str] | None = None
    self_columns: list[str] | None = None
    on_delete: Action | None = None
    on_update: Action | None = None
    name: str | None = None
    match: MatchType | None = None

    def compile(self, driver: DbDriver) -> str:
        return driver.foreign_key_template.format(
            self_columns='FOREIGN KEY (%s) ' % ', '.join(self.self_columns) if self.self_columns else '',
            constraint=f'CONSTRAINT {self.name} ' if self.name else '',
            references=f'REFERENCES {self.target_table}',
            columns=' (%s)' % ', '.join(self.target_columns) if self.target_columns else '',
            on_delete=f' ON DELETE {self.on_delete}' if self.on_delete else '',
            on_update=f' ON UPDATE {self.on_update}' if self.on_update else '',
            match=f' MATCH {self.match}' if self.match else '',
        )


@dataclasses.dataclass
class Column:
    name: str
    type: types.Type
    null: bool = False
    default: str | None = None
    primary_key: bool = False
    auto_increment: bool = False
    if_not_exists: bool = False
    comment: str | None = None
    unique_constraint: UniqueConstraint | None = None
    check_constraint: CheckConstraint | None = None
    foreign_key: ForeignKey | None = None

    def check(self, expr: str, name: str | None = None) -> Column:
        self.check_constraint = CheckConstraint(expr, name)
        return self

    def unique(self, name: str | None = None) -> Column:
        self.unique_constraint = UniqueConstraint(name)
        return self

    def references(
        self,
        table: str,
        columns: list[str] | None = None,
        on_delete: Action | None = None,
        on_update: Action | None = None,
        match: MatchType | None = None,
    ) -> Column:
        self.foreign_key = ForeignKey(
            target_table=table,
            on_delete=on_delete,
            on_update=on_update,
            target_columns=columns,
            match=match,
        )
        return self


@dataclasses.dataclass
class Index:
    name: str
    table_name: str
    unique: bool
    using: str
    columns: list[IndexExpr]
    include: list[str]
    with_: str
    tablespace: str
    where: str


@dataclasses.dataclass
class Table:
    name: str
    columns: list[Column] = dataclasses.field(default_factory=list)
    constraints: list[Constraint] = dataclasses.field(default_factory=list)
    indices: list[Index] = dataclasses.field(default_factory=list)
