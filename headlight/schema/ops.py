from __future__ import annotations

import abc

from headlight.drivers.base import DbDriver
from headlight.schema.schema import (
    Action,
    CheckConstraint,
    Column,
    Constraint,
    ForeignKey,
    Index,
    IndexExpr,
    MatchType,
    PrimaryKeyConstraint,
    UniqueConstraint,
)
from headlight.schema.types import Type


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
        table: str,
        columns: list[IndexExpr],
        name: str,
        unique: bool = False,
        concurrently: bool = False,
        if_not_exists: bool = False,
        only: bool = False,
        using: str | None = None,
        include: list[str] | None = None,
        with_: str | None = None,
        where: str | None = None,
        tablespace: str | None = None,
    ) -> None:
        self.table = table
        self.columns = columns
        self.name = name
        self.unique = unique
        self.concurrently = concurrently
        self.if_not_exists = if_not_exists
        self.only = only
        self.using = using
        self.include = include
        self.with_ = with_
        self.where = where
        self.tablespace = tablespace

    def to_up_sql(self, driver: DbDriver) -> str:
        return driver.create_index_template.format(
            unique=' UNIQUE' if self.unique else '',
            concurrently=' CONCURRENTLY' if self.concurrently else '',
            if_not_exists=' IF NOT EXISTS' if self.if_not_exists else '',
            name=f' {self.name}' if self.name else '',
            only=' ONLY' if self.only else '',
            table=self.table,
            using=f' USING {self.using}' if self.using else '',
            columns=', '.join(
                [
                    driver.index_column_template.format(
                        expr=column.column,
                        collation=f' COLLATE "{column.collation}"' if column.collation else '',
                        opclass=f' {column.opclass}' if column.opclass else '',
                        opclass_params=f'({column.opclass_params})' if column.opclass_params else '',
                        sorting=f' {column.sorting}' if column.sorting else '',
                        nulls=f' NULLS {column.nulls}' if column.nulls else '',
                    )
                    for column in self.columns
                ]
            ),
            include=' INCLUDE (%s)' % ', '.join(self.include) if self.include else '',
            with_=f' WITH ({self.with_})' if self.with_ else '',
            tablespace=f' TABLESPACE {self.tablespace}' if self.tablespace else '',
            where=f' WHERE {self.where}' if self.where else '',
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        return driver.drop_index_template.format(name=self.name)


class DropIndexOp(Operation):
    def __init__(self, name: str, create_index: CreateIndexOp) -> None:
        self.name = name
        self.create_index = create_index

    def to_up_sql(self, driver: DbDriver) -> str:
        return driver.drop_index_template.format(name=self.name)

    def to_down_sql(self, driver: DbDriver) -> str:
        return self.create_index.to_up_sql(driver)


class CreateTableOp(Operation):
    def __init__(
        self,
        table_name: str,
        columns: list[Column],
        constraints: list[Constraint] | None = None,
        indices: list[Index] | None = None,
        if_not_exists: bool = False,
    ) -> None:
        self._table_name = table_name
        self._if_not_exists = if_not_exists
        self._columns = columns
        self._constraints = constraints or []
        self._indices = indices or []

    def to_up_sql(self, driver: DbDriver) -> str:
        pk_cols = [col for col in self._columns if col.primary_key]
        pk_count = len(pk_cols)

        column_stmts = [
            '    '
            + driver.column_template.format(
                name=column.name,
                null='' if column.null else ' NOT NULL',
                check=f' {column.check_constraint.compile(driver)}' if column.check_constraint else '',
                type=driver.get_sql_for_type(column.type),
                default=f" DEFAULT '{column.default}'" if column.default is not None else '',
                primary_key=' PRIMARY KEY' if pk_count == 1 and column.primary_key else '',
                foreign=f' {column.foreign_key.compile(driver)}' if column.foreign_key else '',
                unique=f' {column.unique_constraint.compile(driver)}' if column.unique_constraint else '',
                generated_as=f' {column.generated_as_.compile(driver)}' if column.generated_as_ else '',
            )
            for column in self._columns
        ]

        if pk_count > 1:
            self._constraints.append(PrimaryKeyConstraint(columns=[col.name for col in pk_cols]))

        for constraint in self._constraints:
            column_stmts.append('    ' + constraint.compile(driver))

        return driver.create_table_template.format(
            name=self._table_name,
            column_sql='\n' + ',\n'.join(column_stmts) + '\n',
            if_not_exists=' IF NOT EXISTS ' if self._if_not_exists else ' ',
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        return DropTableOp(name=self._table_name, create_table=self).to_up_sql(driver)


class DropTableOp(Operation):
    def __init__(self, name: str, create_table: CreateTableOp) -> None:
        self.name = name
        self.create_table = create_table

    def to_up_sql(self, driver: DbDriver) -> str:
        return driver.drop_table_template.format(name=self.name)

    def to_down_sql(self, driver: DbDriver) -> str:
        return self.create_table.to_up_sql(driver)


class AddColumnOp(Operation):
    def __init__(
        self,
        table_name: str,
        column_name: str,
        type: Type,
        if_column_not_exists: bool = False,
        if_table_exists: bool = False,
        unique_constraint: UniqueConstraint | None = None,
        check_constraint: CheckConstraint | None = None,
        collate: str | None = None,
        only: bool = False,
        null: bool = False,
        default: str | None = None,
        primary_key: bool | None = None,
        foreign_key: ForeignKey | None = None,
    ) -> None:
        self.type = type
        self.only = only
        self.null = null
        self.default = default
        self.collate = collate
        self.table_name = table_name
        self.column_name = column_name
        self.foreign_key = foreign_key
        self.primary_key = primary_key
        self.if_table_exists = if_table_exists
        self.check_constraint = check_constraint
        self.unique_constraint = unique_constraint
        self.if_column_not_exists = if_column_not_exists

    def check(self, expr: str, name: str | None = None) -> AddColumnOp:
        self.check_constraint = CheckConstraint(expr, name)
        return self

    def unique(self, name: str | None = None) -> AddColumnOp:
        self.unique_constraint = UniqueConstraint(name)
        return self

    def references(
        self,
        table: str,
        columns: list[str] | None = None,
        on_delete: Action | None = None,
        on_update: Action | None = None,
        match: MatchType | None = None,
    ) -> AddColumnOp:
        self.foreign_key = ForeignKey(
            target_table=table,
            on_delete=on_delete,
            on_update=on_update,
            target_columns=columns,
            match=match,
        )
        return self

    def to_up_sql(self, driver: DbDriver) -> str:
        return driver.add_column_template.format(
            table=self.table_name,
            name=self.column_name,
            only=' ONLY' if self.only else '',
            null='' if self.null else ' NOT NULL',
            type=driver.get_sql_for_type(self.type),
            pk=' PRIMARY KEY' if self.primary_key else '',
            collate=f' COLLATE "{self.collate}"' if self.collate else '',
            if_table_exists=' IF EXISTS' if self.if_table_exists else '',
            default=f" DEFAULT '{self.default}'" if self.default is not None else '',
            if_column_not_exists=' IF NOT EXISTS' if self.if_column_not_exists else '',
            foreign=f' {self.foreign_key.compile(driver)}' if self.foreign_key else '',
            check=f' {self.check_constraint.compile(driver)}' if self.check_constraint else '',
            unique=f' {self.unique_constraint.compile(driver)}' if self.unique_constraint else '',
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        return DropColumnOp(
            only=self.only,
            if_table_exists=True,
            if_column_exists=True,
            table_name=self.table_name,
            column_name=self.column_name,
            create_column=self,
        ).to_up_sql(driver)


class DropColumnOp(Operation):
    def __init__(
        self,
        table_name: str,
        column_name: str,
        create_column: AddColumnOp,
        if_table_exists: bool = False,
        if_column_exists: bool = False,
        only: bool = False,
    ) -> None:
        self.only = only
        self.table_name = table_name
        self.column_name = column_name
        self.if_table_exists = if_table_exists
        self.if_column_exists = if_column_exists
        self.create_column = create_column

    def to_up_sql(self, driver: DbDriver) -> str:
        return driver.drop_column_template.format(
            table=self.table_name,
            name=self.column_name,
            only=' ONLY' if self.only else '',
            if_table_exists=' IF EXISTS' if self.if_table_exists else '',
            if_column_exists=' IF EXISTS' if self.if_column_exists else '',
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        return self.create_column.to_up_sql(driver)


class SetDefaultOp(Operation):
    def __init__(
        self,
        table_name: str,
        column_name: str,
        new_default: str,
        current_default: str | None = None,
        only: bool = False,
        if_table_exists: bool = False,
    ) -> None:
        self.only = only
        self.table_name = table_name
        self.column_name = column_name
        self.new_default = new_default
        self.old_default = current_default
        self.if_table_exists = if_table_exists

    def to_up_sql(self, driver: DbDriver) -> str:
        return driver.add_column_default_template.format(
            table=self.table_name,
            name=self.column_name,
            only=' ONLY' if self.only else '',
            expr=f"'{self.new_default}'",
            if_table_exists=' IF EXISTS' if self.if_table_exists else '',
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        if self.old_default is None:
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
        current_default: str | None = None,
        only: bool = False,
        if_table_exists: bool = False,
    ) -> None:
        self.only = only
        self.table_name = table_name
        self.column_name = column_name
        self.old_default = current_default
        self.if_table_exists = if_table_exists

    def to_up_sql(self, driver: DbDriver) -> str:
        return driver.drop_column_default_template.format(
            table=self.table_name,
            name=self.column_name,
            only=' ONLY' if self.only else '',
            if_table_exists=' IF EXISTS' if self.if_table_exists else '',
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        if self.old_default is None:  # column had no default previously
            return '-- noop'
        return SetDefaultOp(
            table_name=self.table_name,
            column_name=self.column_name,
            new_default=self.old_default,
            only=self.only,
            if_table_exists=self.if_table_exists,
        ).to_up_sql(driver)


class SetNullOp(Operation):
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
            only=' ONLY' if self.only else '',
            if_table_exists=' IF EXISTS' if self.if_table_exists else '',
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        return DropNullOp(
            table_name=self.table_name,
            column_name=self.column_name,
            only=self.only,
            if_table_exists=self.if_table_exists,
        ).to_up_sql(driver)


class DropNullOp(Operation):
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
            only=' ONLY' if self.only else '',
            if_table_exists=' IF EXISTS' if self.if_table_exists else '',
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        return SetNullOp(
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
            collate=f" COLLATE {self.collation}" if self.collation else '',
            using=f" USING {self.using}" if self.using else '',
            only=' ONLY' if self.only else '',
            if_table_exists=' IF EXISTS' if self.if_table_exists else '',
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        return driver.change_column_type.format(
            table=self.table_name,
            name=self.column_name,
            type=driver.get_sql_for_type(self.old_type),
            collate=f" COLLATE {self.old_collation}" if self.old_collation else '',
            using=f" USING {self.old_using}" if self.old_using else '',
            only=' ONLY' if self.only else '',
            if_table_exists=' IF EXISTS' if self.if_table_exists else '',
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
            only=' ONLY' if self.only else '',
            if_table_exists=' IF EXISTS' if self.if_table_exists else '',
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        return DropTableConstraintOp(
            constraint_name=getattr(self.constraint, 'name'),
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
        if_exists: bool = False,
        if_table_exists: bool = False,
    ) -> None:
        self.only = only
        self.if_exists = if_exists
        self.table_name = table_name
        self.constraint_name = constraint_name
        self.if_table_exists = if_table_exists
        self.current_constraint = current_constraint

    def to_up_sql(self, driver: DbDriver) -> str:
        return driver.drop_table_constraint_template.format(
            table=self.table_name,
            name=self.constraint_name,
            only=' ONLY' if self.only else '',
            if_exists=' IF EXISTS' if self.if_exists else '',
            if_table_exists=' IF EXISTS' if self.if_table_exists else '',
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        return AddTableConstraintOp(
            constraint=self.current_constraint,
            table_name=self.table_name,
            only=self.only,
            if_table_exists=self.if_table_exists,
        ).to_up_sql(driver)
