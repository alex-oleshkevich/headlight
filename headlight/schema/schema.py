from __future__ import annotations

import dataclasses

import string
import typing

from headlight.drivers.base import DbDriver
from headlight.exceptions import HeadlightError
from headlight.schema import types

Action = typing.Literal["RESTRICT", "CASCADE", "NO ACTION", "SET NULL", "SET DEFAULT"]
MatchType = typing.Literal["FULL", "PARTIAL", "SIMPLE"]
DropMode = typing.Literal["RESTRICT", "CASCADE"]


class SchemaError(HeadlightError):
    ...


def sanitize_name(name: str) -> str:
    return "".join([c for c in name if c in string.ascii_letters + string.digits])


@dataclasses.dataclass
class IndexExpr:
    column: str
    collation: str = ""
    opclass: str = ""
    opclass_params: str = ""
    sorting: typing.Literal["ASC", "DESC"] | None = None
    nulls: typing.Literal["FIRST", "LAST"] | None = None

    @classmethod
    def from_specs(cls, specs: list[str | IndexExpr]) -> list[IndexExpr]:
        return [cls(column) if isinstance(column, str) else column for column in specs]


@dataclasses.dataclass
class Constraint:
    def compile(self, driver: DbDriver) -> str:
        raise NotImplementedError()


@dataclasses.dataclass
class CheckConstraint(Constraint):
    expr: str
    name: str | None = None

    def compile(self, driver: DbDriver) -> str:
        expr = self.expr.replace("%", "%%")
        return driver.check_constraint_template.format(
            expr=expr,
            constraint=f"CONSTRAINT {self.name} " if self.name else "",
        )

    @classmethod
    def new(cls, spec: CheckConstraint | str | tuple[str, str]) -> CheckConstraint:
        match spec:
            case CheckConstraint():
                return spec
            case expr if isinstance(expr, str):
                return CheckConstraint(expr=expr)
            case (constraint_name, expr):
                return CheckConstraint(expr=expr, name=constraint_name)

        raise ValueError(f"Don't know how to construct check constraint from '{spec}'.")

    @classmethod
    def generate_name(cls, table_name: str, expr: str) -> str:
        return "{table_name}_{columns}_check".format(table_name=table_name, columns="".join(sanitize_name(expr)))


@dataclasses.dataclass
class UniqueConstraint(Constraint):
    columns: list[str] | None = None
    include: list[str] | None = None
    name: str | None = None

    def compile(self, driver: DbDriver) -> str:
        return driver.unique_constraint_template.format(
            constraint=f"CONSTRAINT {self.name} " if self.name else "",
            columns=" (%s)" % ", ".join(self.columns) if self.columns else "",
            include=" INCLUDE (%s)" % ", ".join(self.include) if self.include else "",
        )

    @classmethod
    def new(cls, spec: UniqueConstraint | bool | str) -> UniqueConstraint:
        match spec:
            case UniqueConstraint():
                return spec
            case True:
                return UniqueConstraint()
            case constraint_name if isinstance(constraint_name, str):
                return UniqueConstraint(name=constraint_name)

        raise ValueError(f"Don't know how to construct unique constraint from '{spec}'.")

    @classmethod
    def generate_name(cls, table_name: str, columns: list[str]) -> str:
        return "{table_name}_{columns}_uniq".format(table_name=table_name, columns="_".join(columns))


@dataclasses.dataclass
class PrimaryKeyConstraint(Constraint):
    columns: list[str]
    name: str | None = None
    include: list[str] = dataclasses.field(default_factory=list)

    def compile(self, driver: DbDriver) -> str:
        return driver.primary_key_constraint_template.format(
            constraint=f"CONSTRAINT {self.name} " if self.name else "",
            columns=", ".join(self.columns) if self.columns else "",
            include=" INCLUDE (%s)" % ", ".join(self.include) if self.include else "",
        )

    @classmethod
    def generate_name(cls, table_name: str, columns: list[str]) -> str:
        return "{table_name}_{columns}_pk".format(table_name=table_name, columns="_".join(columns))


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
            self_columns="FOREIGN KEY (%s) " % ", ".join(self.self_columns) if self.self_columns else "",
            constraint=f"CONSTRAINT {self.name} " if self.name else "",
            references=f"REFERENCES {self.target_table}",
            columns=" (%s)" % ", ".join(self.target_columns) if self.target_columns else "",
            on_delete=f" ON DELETE {self.on_delete}" if self.on_delete else "",
            on_update=f" ON UPDATE {self.on_update}" if self.on_update else "",
            match=f" MATCH {self.match}" if self.match else "",
        )

    @classmethod
    def generate_name(cls, from_table: str, to_table: str, from_columns: list[str]) -> str:
        return "{from_table}_{from_cols}_to_{to_table}_fk".format(
            from_table=from_table, to_table=to_table, from_cols="_".join(from_columns)
        )


@dataclasses.dataclass
class GeneratedAs:
    expr: str
    stored: bool = False

    def compile(self, driver: DbDriver) -> str:
        return driver.generated_as_template.format(
            expr=self.expr,
            stored="STORED" if self.stored else "",
        )

    @classmethod
    def new(cls, value: GeneratedAs | str) -> GeneratedAs:
        if isinstance(value, str):
            return cls(expr=value, stored=True)
        return value


@dataclasses.dataclass
class Column:
    name: str
    type: types.Type
    null: bool = False
    default: str | None = None
    primary_key: bool = False
    collate: str | None = None
    foreign_key: ForeignKey | None = None
    generated_as_: GeneratedAs | None = None
    unique_constraint: UniqueConstraint | None = None
    check_constraints: list[CheckConstraint] = dataclasses.field(default_factory=list)

    def __post_init__(self) -> None:
        if self.unique_constraint:
            self.validate_unique_constraint()

        if self.foreign_key:
            self.validate_foreign_key_constraint()

    def check(self, expr: str, name: str | None = None) -> Column:
        self.check_constraints.append(CheckConstraint(expr, name))
        return self

    def unique(self, name: str | None = None) -> Column:
        self.unique_constraint = UniqueConstraint(name=name)
        return self

    def references(
        self,
        target_table: str,
        target_columns: list[str] | None = None,
        on_delete: Action | None = None,
        on_update: Action | None = None,
        match: MatchType | None = None,
    ) -> Column:
        self.foreign_key = ForeignKey(
            target_table=target_table,
            on_delete=on_delete,
            on_update=on_update,
            target_columns=target_columns,
            match=match,
        )
        return self

    def generated_as(self, expr: str, stored: bool = True) -> Column:
        self.generated_as_ = GeneratedAs(expr, stored)
        return self

    def validate_unique_constraint(self) -> None:
        if self.unique_constraint is None:
            return

        if self.unique_constraint.columns:
            raise SchemaError(
                f'Column level unique constraint "{self.unique_constraint.name}" cannot have multiple columns.'
                f'Seen in "{self.name}".'
            )
        if self.unique_constraint.include:
            raise SchemaError(
                f'Column level unique constraint "{self.unique_constraint.name}" cannot have INCLUDE part.'
                f'Seen in "{self.name}".'
            )

    def validate_foreign_key_constraint(self) -> None:
        if self.foreign_key is None:
            return

        if self.foreign_key.self_columns:
            raise SchemaError(
                f'"self_columns" is not allowed for column level foreign key constraint ("{self.foreign_key.name}").'
                f'Seen in "{self.name}".'
            )

    def compile(self, driver: DbDriver) -> str:
        check_sql = ""
        for check in self.check_constraints:
            check_sql += " " + check.compile(driver)

        return driver.column_template.format(
            name=self.name,
            check=check_sql,
            null="" if self.null else " NOT NULL",
            type=driver.get_sql_for_type(self.type),
            pk=" PRIMARY KEY" if self.primary_key else "",
            collate=f' COLLATE "{self.collate}"' if self.collate else "",
            default=f" DEFAULT '{self.default}'" if self.default is not None else "",
            foreign=f" {self.foreign_key.compile(driver)}" if self.foreign_key else "",
            generated_as=f" {self.generated_as_.compile(driver)}" if self.generated_as_ else "",
            unique=f" {self.unique_constraint.compile(driver)}" if self.unique_constraint else "",
        )


@dataclasses.dataclass
class Index:
    name: str
    table_name: str
    columns: list[IndexExpr]
    unique: bool = False
    using: str | None = None
    include: list[str] | None = None
    with_: str | None = None
    tablespace: str | None = None
    where: str | None = None

    @classmethod
    def generate_name(cls, table_name: str, index_expr: list[IndexExpr]) -> str:
        return table_name + "_" + "_".join([sanitize_name(expr.column) for expr in index_expr]) + "_idx"


@dataclasses.dataclass
class Table:
    name: str
    columns: list[Column] = dataclasses.field(default_factory=list)
    constraints: list[Constraint] = dataclasses.field(default_factory=list)
    indices: list[Index] = dataclasses.field(default_factory=list)
