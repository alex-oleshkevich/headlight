from __future__ import annotations

import abc
import typing

from headlight.drivers.base import DbDriver
from headlight.exceptions import HeadlightError
from headlight.schema.schema import (
    Action,
    Column,
    Constraint,
    Default,
    DropMode,
    Index,
    MatchType,
    PrimaryKeyConstraint,
    Table,
)
from headlight.schema.types import Type


class OperationError(HeadlightError):
    pass


class Operation(abc.ABC):
    @abc.abstractmethod
    def to_up_sql(self, driver: DbDriver) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def to_down_sql(self, driver: DbDriver) -> str:
        raise NotImplementedError()


class RunSQLOp(Operation):
    def __init__(self, up_sql: str, down_sql: str) -> None:
        self.up_sql = up_sql
        self.down_sql = down_sql

    def to_up_sql(self, driver: DbDriver) -> str:
        return self.up_sql

    def to_down_sql(self, driver: DbDriver) -> str:
        return self.down_sql


class CreateIndexOp(Operation):
    def __init__(
        self,
        index: Index,
        concurrently: bool = False,
        if_not_exists: bool = False,
        only: bool = False,
    ) -> None:
        self.only = only
        self.index = index
        self.concurrently = concurrently
        self.if_not_exists = if_not_exists

    def to_up_sql(self, driver: DbDriver) -> str:
        return driver.create_index_template.format(
            unique=" UNIQUE" if self.index.unique else "",
            concurrently=" CONCURRENTLY" if self.concurrently else "",
            if_not_exists=" IF NOT EXISTS" if self.if_not_exists else "",
            name=f" {self.index.name}" if self.index.name else "",
            only=" ONLY" if self.only else "",
            table=self.index.table_name,
            using=f" USING {self.index.using}" if self.index.using else "",
            columns=", ".join(
                [
                    driver.index_column_template.format(
                        expr=column.column,
                        collation=f' COLLATE "{column.collation}"' if column.collation else "",
                        opclass=f" {column.opclass}" if column.opclass else "",
                        opclass_params=f"({column.opclass_params})" if column.opclass_params else "",
                        sorting=f" {column.sorting}" if column.sorting else "",
                        nulls=f" NULLS {column.nulls}" if column.nulls else "",
                    )
                    for column in self.index.columns
                ]
            ),
            include=" INCLUDE (%s)" % ", ".join(self.index.include) if self.index.include else "",
            with_=f" WITH ({self.index.with_})" if self.index.with_ else "",
            tablespace=f" TABLESPACE {self.index.tablespace}" if self.index.tablespace else "",
            where=f" WHERE {self.index.where}" if self.index.where else "",
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        return DropIndexOp(name=self.index.name, current_index=self.index).to_up_sql(driver)


class DropIndexOp(Operation):
    def __init__(self, name: str, current_index: Index, mode: DropMode | None = None) -> None:
        self.name = name
        self.mode = mode
        self.old_index = current_index

    def to_up_sql(self, driver: DbDriver) -> str:
        return driver.drop_index_template.format(name=self.name, mode=f" {self.mode}" if self.mode else "")

    def to_down_sql(self, driver: DbDriver) -> str:
        return CreateIndexOp(index=self.old_index).to_up_sql(driver)


class CreateTableOp(Operation):
    def __init__(self, table: Table, if_not_exists: bool = False) -> None:
        self._table = table
        self._if_not_exists = if_not_exists

    def to_up_sql(self, driver: DbDriver) -> str:
        pk_cols = [col for col in self._table.columns if col.primary_key]
        pk_count = len(pk_cols)
        if pk_count > 1:
            self._table.constraints.append(PrimaryKeyConstraint(columns=[col.name for col in pk_cols]))
            for col in self._table.columns:
                col.primary_key = False

        column_stmts = ["    " + column.compile(driver) for column in self._table.columns]

        for constraint in self._table.constraints:
            column_stmts.append("    " + constraint.compile(driver))

        return driver.create_table_template.format(
            name=self._table.name,
            column_sql="\n" + ",\n".join(column_stmts) + "\n",
            if_not_exists=" IF NOT EXISTS " if self._if_not_exists else " ",
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        return DropTableOp(name=self._table.name, current_table=self._table).to_up_sql(driver)


class DropTableOp(Operation):
    def __init__(self, name: str, current_table: Table, mode: DropMode | None = None) -> None:
        self.name = name
        self.mode = mode
        self.old_table = current_table

    def to_up_sql(self, driver: DbDriver) -> str:
        return driver.drop_table_template.format(name=self.name, mode=f" {self.mode}" if self.mode else "")

    def to_down_sql(self, driver: DbDriver) -> str:
        return CreateTableOp(table=self.old_table).to_up_sql(driver)


class AddColumnOp(Operation):
    def __init__(
        self,
        table_name: str,
        column: Column,
        if_column_not_exists: bool = False,
        if_table_exists: bool = False,
        only: bool = False,
    ) -> None:
        self.type = type
        self.only = only
        self.column = column
        self.table_name = table_name
        self.if_table_exists = if_table_exists
        self.if_column_not_exists = if_column_not_exists

    def check(self, expr: str, name: str | None = None) -> AddColumnOp:
        self.column.check(expr, name)
        return self

    def unique(self, name: str | None = None) -> AddColumnOp:
        self.column.unique(name)
        return self

    def references(
        self,
        target_table: str,
        target_columns: list[str] | None = None,
        on_delete: Action | None = None,
        on_update: Action | None = None,
        match: MatchType | None = None,
    ) -> AddColumnOp:
        self.column.references(
            target_table=target_table,
            on_delete=on_delete,
            on_update=on_update,
            target_columns=target_columns,
            match=match,
        )
        return self

    def generated_as(self, expr: str, stored: bool = True) -> AddColumnOp:
        self.column.generated_as(expr, stored)
        return self

    def to_up_sql(self, driver: DbDriver) -> str:
        return driver.add_column_template.format(
            column_spec=self.column.compile(driver),
            table=self.table_name,
            only=" ONLY" if self.only else "",
            if_table_exists=" IF EXISTS" if self.if_table_exists else "",
            if_column_not_exists=" IF NOT EXISTS" if self.if_column_not_exists else "",
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        return DropColumnOp(
            only=self.only,
            if_table_exists=True,
            if_column_exists=True,
            table_name=self.table_name,
            column_name=self.column.name,
            current_column=self.column,
        ).to_up_sql(driver)


class DropColumnOp(Operation):
    def __init__(
        self,
        table_name: str,
        column_name: str,
        current_column: Column,
        if_table_exists: bool = False,
        if_column_exists: bool = False,
        only: bool = False,
        mode: DropMode | None = None,
    ) -> None:
        self.only = only
        self.mode = mode
        self.table_name = table_name
        self.column_name = column_name
        self.old_column = current_column
        self.if_table_exists = if_table_exists
        self.if_column_exists = if_column_exists

    def to_up_sql(self, driver: DbDriver) -> str:
        return driver.drop_column_template.format(
            table=self.table_name,
            name=self.column_name,
            mode=f" {self.mode}" if self.mode else "",
            only=" ONLY" if self.only else "",
            if_table_exists=" IF EXISTS" if self.if_table_exists else "",
            if_column_exists=" IF EXISTS" if self.if_column_exists else "",
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        return AddColumnOp(
            table_name=self.table_name,
            column=self.old_column,
            only=self.only,
        ).to_up_sql(driver)


class SetDefaultOp(Operation):
    def __init__(
        self,
        table_name: str,
        column_name: str,
        new_default: typing.Any,
        current_default: typing.Any = None,
        only: bool = False,
        if_table_exists: bool = False,
    ) -> None:
        self.only = only
        self.table_name = table_name
        self.column_name = column_name
        self.new_default = Default.new(new_default)
        self.old_default = Default.new(current_default)
        self.if_table_exists = if_table_exists

    def to_up_sql(self, driver: DbDriver) -> str:
        return driver.add_column_default_template.format(
            table=self.table_name,
            name=self.column_name,
            only=" ONLY" if self.only else "",
            expr=f"{self.new_default.compile(driver)}",
            if_table_exists=" IF EXISTS" if self.if_table_exists else "",
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        if self.old_default.value is None:
            return DropDefaultOp(
                table_name=self.table_name,
                column_name=self.column_name,
                current_default=self.old_default,
                only=self.only,
                if_table_exists=self.if_table_exists,
            ).to_up_sql(driver)
        return self.__class__(
            table_name=self.table_name,
            column_name=self.column_name,
            new_default=self.old_default,
            current_default=self.new_default,
            only=self.only,
            if_table_exists=self.if_table_exists,
        ).to_up_sql(driver)


class DropDefaultOp(Operation):
    def __init__(
        self,
        table_name: str,
        column_name: str,
        current_default: typing.Any = None,
        only: bool = False,
        if_table_exists: bool = False,
    ) -> None:
        self.only = only
        self.table_name = table_name
        self.column_name = column_name
        self.old_default = Default.new(current_default)
        self.if_table_exists = if_table_exists

    def to_up_sql(self, driver: DbDriver) -> str:
        return driver.drop_column_default_template.format(
            table=self.table_name,
            name=self.column_name,
            only=" ONLY" if self.only else "",
            if_table_exists=" IF EXISTS" if self.if_table_exists else "",
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        if self.old_default.value is None:  # column had no default previously
            return "-- noop, column had no default previously"
        return SetDefaultOp(
            table_name=self.table_name,
            column_name=self.column_name,
            new_default=self.old_default,
            only=self.only,
            if_table_exists=self.if_table_exists,
        ).to_up_sql(driver)


class SetNotNullOp(Operation):
    def __init__(
        self,
        table_name: str,
        column_name: str,
        only: bool = False,
        if_table_exists: bool = False,
    ) -> None:
        self.only = only
        self.table_name = table_name
        self.column_name = column_name
        self.if_table_exists = if_table_exists

    def to_up_sql(self, driver: DbDriver) -> str:
        return driver.add_column_null_template.format(
            table=self.table_name,
            name=self.column_name,
            only=" ONLY" if self.only else "",
            if_table_exists=" IF EXISTS" if self.if_table_exists else "",
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        return DropNotNullOp(
            table_name=self.table_name,
            column_name=self.column_name,
            only=self.only,
            if_table_exists=self.if_table_exists,
        ).to_up_sql(driver)


class DropNotNullOp(Operation):
    def __init__(
        self,
        table_name: str,
        column_name: str,
        only: bool = False,
        if_table_exists: bool = False,
    ) -> None:
        self.only = only
        self.table_name = table_name
        self.column_name = column_name
        self.if_table_exists = if_table_exists

    def to_up_sql(self, driver: DbDriver) -> str:
        return driver.drop_column_null_template.format(
            table=self.table_name,
            name=self.column_name,
            only=" ONLY" if self.only else "",
            if_table_exists=" IF EXISTS" if self.if_table_exists else "",
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        return SetNotNullOp(
            table_name=self.table_name,
            column_name=self.column_name,
            only=self.only,
            if_table_exists=self.if_table_exists,
        ).to_up_sql(driver)


class ChangeTypeOp(Operation):
    def __init__(
        self,
        table_name: str,
        column_name: str,
        new_type: Type,
        current_type: Type,
        only: bool = False,
        if_table_exists: bool = False,
        collation: str | None = None,
        using: str | None = None,
        current_collation: str | None = None,
        current_using: str | None = None,
    ) -> None:
        self.only = only
        self.new_type = new_type
        self.old_type = current_type
        self.table_name = table_name
        self.column_name = column_name
        self.if_table_exists = if_table_exists
        self.collation = collation
        self.using = using
        self.old_collation = current_collation
        self.old_using = current_using

    def to_up_sql(self, driver: DbDriver) -> str:
        return driver.change_column_type.format(
            table=self.table_name,
            name=self.column_name,
            type=driver.get_sql_for_type(self.new_type),
            collate=f" COLLATE {self.collation}" if self.collation else "",
            using=f" USING {self.using}" if self.using else "",
            only=" ONLY" if self.only else "",
            if_table_exists=" IF EXISTS" if self.if_table_exists else "",
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        return driver.change_column_type.format(
            table=self.table_name,
            name=self.column_name,
            type=driver.get_sql_for_type(self.old_type),
            collate=f" COLLATE {self.old_collation}" if self.old_collation else "",
            using=f" USING {self.old_using}" if self.old_using else "",
            only=" ONLY" if self.only else "",
            if_table_exists=" IF EXISTS" if self.if_table_exists else "",
        )


class AddTableConstraintOp(Operation):
    def __init__(
        self,
        constraint: Constraint,
        table_name: str,
        only: bool = False,
        if_table_exists: bool = False,
    ) -> None:
        self.only = only
        self.constraint = constraint
        self.table_name = table_name
        self.if_table_exists = if_table_exists

    def to_up_sql(self, driver: DbDriver) -> str:
        return driver.add_table_check_template.format(
            table=self.table_name,
            constraint=self.constraint.compile(driver),
            only=" ONLY" if self.only else "",
            if_table_exists=" IF EXISTS" if self.if_table_exists else "",
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        name = getattr(self.constraint, "name")
        if not name:
            raise OperationError(f'Constraint "{self.constraint} has no name and therefore cannot be dropped.')

        return DropTableConstraintOp(
            constraint_name=getattr(self.constraint, "name"),
            table_name=self.table_name,
            only=self.only,
            if_table_exists=self.if_table_exists,
            current_constraint=self.constraint,
        ).to_up_sql(driver)


class DropTableConstraintOp(Operation):
    def __init__(
        self,
        constraint_name: str,
        table_name: str,
        current_constraint: Constraint,
        only: bool = False,
        mode: DropMode | None = None,
        if_exists: bool = False,
        if_table_exists: bool = False,
    ) -> None:
        self.only = only
        self.mode = mode
        self.if_exists = if_exists
        self.table_name = table_name
        self.constraint_name = constraint_name
        self.if_table_exists = if_table_exists
        self.current_constraint = current_constraint

    def to_up_sql(self, driver: DbDriver) -> str:
        return driver.drop_table_constraint_template.format(
            table=self.table_name,
            name=self.constraint_name,
            only=" ONLY" if self.only else "",
            mode=f" {self.mode}" if self.mode else "",
            if_exists=" IF EXISTS" if self.if_exists else "",
            if_table_exists=" IF EXISTS" if self.if_table_exists else "",
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        return AddTableConstraintOp(
            constraint=self.current_constraint,
            table_name=self.table_name,
            only=self.only,
            if_table_exists=self.if_table_exists,
        ).to_up_sql(driver)
