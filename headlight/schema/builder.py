from __future__ import annotations

import contextlib
import inspect
import typing

from headlight.schema import ops, types
from headlight.schema.schema import (
    Action,
    CheckConstraint,
    Column,
    Constraint,
    Default,
    DropMode,
    Expr,
    ForeignKey,
    GeneratedAs,
    Index,
    IndexExpr,
    MatchType,
    PrimaryKeyConstraint,
    Table,
    UniqueConstraint,
    expr,
)


class CreateTableBuilder:
    def __init__(
        self,
        table_name: str,
    ) -> None:
        self._table = Table(name=table_name)

    def autoincrements(self, name: str = "id") -> None:
        self.add_column(name=name, type=types.BigIntegerType(auto_increment=True), primary_key=True, null=False)

    def add_timestamps(
        self, created_name: str = "created_at", updated_name: str = "updated_at", tz: bool = True
    ) -> None:
        self.add_column(created_name, types.DateTimeType(tz), null=False, default=expr.now())
        self.add_column(updated_name, types.DateTimeType(tz), null=True)

    def add_created_timestamp(self, created_name: str = "created_at", tz: bool = True) -> None:
        self.add_column(created_name, types.DateTimeType(tz), null=False, default=expr.now())

    def add_column(
        self,
        name: str,
        type: types.Type | typing.Type[types.Type],
        null: bool = False,
        default: str | int | Default | Expr | bool | list | dict | None = None,
        primary_key: bool = False,
        unique: UniqueConstraint | bool | str | None = None,
        checks: list[CheckConstraint | str | tuple[str, str]] | None = None,
        generated_as: GeneratedAs | str | None = None,
    ) -> Column:
        column_type = type() if inspect.isclass(type) else type
        unique_constraint = UniqueConstraint.new(unique) if unique else None
        check_constraints = [CheckConstraint.new(check) for check in checks] if checks else []
        generated_as = GeneratedAs.new(generated_as) if generated_as else None
        column = Column(
            name=name,
            type=column_type,
            null=null,
            default=Default.new(default),
            primary_key=primary_key,
            generated_as_=generated_as,
            check_constraints=check_constraints,
            unique_constraint=unique_constraint,
        )
        self._table.columns.append(column)
        return column

    def add_index(
        self,
        columns: list[str | IndexExpr],
        name: str | None = None,
        unique: bool = False,
        using: str | None = None,
        include: list[str] | None = None,
        with_: str | None = None,
        where: str | None = None,
        tablespace: str | None = None,
    ) -> None:
        index_expr = IndexExpr.from_specs(columns)
        index_name = name or Index.generate_name(self._table.name, index_expr)

        self._table.indices.append(
            Index(
                name=index_name,
                table_name=self._table.name,
                unique=unique,
                using=using,
                columns=index_expr,
                include=include,
                with_=with_,
                tablespace=tablespace,
                where=where,
            )
        )

    def add_check_constraint(self, expr: str, name: str | None = None) -> None:
        name = name or CheckConstraint.generate_name(self._table.name, expr)
        self._table.constraints.append(CheckConstraint(expr, name))

    def add_unique_constraint(
        self,
        columns: list[str],
        name: str | None = None,
        include: list[str] | None = None,
    ) -> None:
        name = name or UniqueConstraint.generate_name(self._table.name, columns)
        self._table.constraints.append(UniqueConstraint(name=name, include=include, columns=columns))

    def add_primary_key(self, columns: list[str], name: str | None = None, include: list[str] | None = None) -> None:
        name = name or PrimaryKeyConstraint.generate_name(self._table.name, columns)
        self._table.constraints.append(PrimaryKeyConstraint(name=name, columns=columns, include=include or []))

    def add_foreign_key(
        self,
        columns: list[str],
        target_table: str,
        target_columns: list[str] | None = None,
        name: str | None = None,
        on_delete: Action | None = None,
        on_update: Action | None = None,
        match: MatchType | None = None,
    ) -> None:
        name = name or ForeignKey.generate_name(self._table.name, target_table, columns)
        self._table.constraints.append(
            ForeignKey(
                name=name,
                match=match,
                on_delete=on_delete,
                on_update=on_update,
                self_columns=columns,
                target_table=target_table,
                target_columns=target_columns,
            )
        )


class ChangeColumn:
    def __init__(
        self,
        ops: list[ops.Operation],
        table_name: str,
        column_name: str,
        only: bool = False,
        if_table_exists: bool = False,
    ) -> None:
        self._only = only
        self._table_name = table_name
        self._column_name = column_name
        self._if_table_exists = if_table_exists
        self._ops = ops

    def set_default(self, new_default: typing.Any, current_default: typing.Any) -> ChangeColumn:
        self._ops.append(
            ops.SetDefaultOp(
                table_name=self._table_name,
                column_name=self._column_name,
                new_default=Default.new(new_default),
                current_default=Default.new(current_default),
                only=self._only,
                if_table_exists=self._if_table_exists,
            )
        )
        return self

    def drop_default(self, current_default: typing.Any) -> ChangeColumn:
        self._ops.append(
            ops.DropDefaultOp(
                table_name=self._table_name,
                column_name=self._column_name,
                current_default=Default.new(current_default),
                only=self._only,
                if_table_exists=self._if_table_exists,
            )
        )
        return self

    def set_nullable(self, flag: bool) -> ChangeColumn:
        if flag:
            self._ops.append(
                ops.DropNotNullOp(
                    table_name=self._table_name,
                    column_name=self._column_name,
                    only=self._only,
                    if_table_exists=self._if_table_exists,
                )
            )
        else:
            self._ops.append(
                ops.SetNotNullOp(
                    table_name=self._table_name,
                    column_name=self._column_name,
                    only=self._only,
                    if_table_exists=self._if_table_exists,
                )
            )
        return self

    def change_type(
        self,
        new_type: types.Type | typing.Type[types.Type],
        current_type: types.Type | typing.Type[types.Type],
        collation: str | None = None,
        current_collation: str | None = None,
        using: str | None = None,
        current_using: str | None = None,
    ) -> ChangeColumn:
        new_type = new_type() if inspect.isclass(new_type) else new_type
        current_type = current_type() if inspect.isclass(current_type) else current_type

        self._ops.append(
            ops.ChangeTypeOp(
                table_name=self._table_name,
                column_name=self._column_name,
                new_type=typing.cast(types.Type, new_type),
                current_type=typing.cast(types.Type, current_type),
                only=self._only,
                if_table_exists=self._if_table_exists,
                collation=collation,
                current_collation=current_collation,
                using=using,
                current_using=current_using,
            )
        )
        return self


class AlterTableBuilder:
    def __init__(self, table_name: str, if_exists: bool = False, only: bool = False) -> None:
        self._table_name = table_name
        self._if_exists = if_exists
        self._only = only
        self.ops: list[ops.Operation] = []

    def add_column(
        self,
        name: str,
        type: types.Type | typing.Type[types.Type],
        null: bool = False,
        primary_key: bool = False,
        default: typing.Any = None,
        unique: bool | UniqueConstraint | None = None,
        checks: list[CheckConstraint | str | tuple[str, str]] | None = None,
        if_table_exists: bool = False,
        if_column_not_exists: bool = False,
        collate: str | None = None,
        generated_as: str | GeneratedAs | None = None,
    ) -> ops.AddColumnOp:
        column_type = type() if inspect.isclass(type) else type
        unique_constraint = UniqueConstraint.new(unique) if unique is not None else None
        check_constraints = [CheckConstraint.new(check) for check in checks] if checks is not None else []
        op = ops.AddColumnOp(
            table_name=self._table_name,
            if_table_exists=if_table_exists,
            if_column_not_exists=if_column_not_exists,
            only=self._only,
            column=Column(
                name=name,
                type=column_type,
                unique_constraint=unique_constraint,
                check_constraints=check_constraints,
                collate=collate,
                null=null,
                default=Default.new(default),
                primary_key=primary_key,
                generated_as_=GeneratedAs.new(generated_as) if generated_as else None,
            ),
        )
        self.ops.append(op)
        return op

    def drop_column(
        self,
        name: str,
        current_column: Column,
        if_column_exists: bool = False,
        mode: DropMode | None = None,
    ) -> None:
        self.ops.append(
            ops.DropColumnOp(
                mode=mode,
                only=self._only,
                column_name=name,
                table_name=self._table_name,
                current_column=current_column,
                if_table_exists=self._if_exists,
                if_column_exists=if_column_exists,
            )
        )

    def alter_column(self, column_name: str) -> ChangeColumn:
        return ChangeColumn(
            ops=self.ops,
            table_name=self._table_name,
            column_name=column_name,
            only=self._only,
            if_table_exists=self._if_exists,
        )

    def add_check_constraint(self, name: str, expr: str) -> None:
        self.ops.append(
            ops.AddTableConstraintOp(
                constraint=CheckConstraint(expr, name),
                table_name=self._table_name,
                only=self._only,
                if_table_exists=self._if_exists,
            )
        )

    def add_unique_constraint(self, name: str, columns: list[str], include: list[str] | None = None) -> None:
        self.ops.append(
            ops.AddTableConstraintOp(
                constraint=UniqueConstraint(name=name, include=include, columns=columns),
                table_name=self._table_name,
                only=self._only,
                if_table_exists=self._if_exists,
            )
        )

    def add_primary_key(self, name: str, columns: list[str], include: list[str] | None = None) -> None:
        self.ops.append(
            ops.AddTableConstraintOp(
                constraint=PrimaryKeyConstraint(name=name, columns=columns, include=include or []),
                table_name=self._table_name,
                only=self._only,
                if_table_exists=self._if_exists,
            )
        )

    def add_foreign_key(
        self,
        name: str,
        target_table: str,
        target_columns: list[str] | None = None,
        self_columns: list[str] | None = None,
        on_delete: Action | None = None,
        on_update: Action | None = None,
        match: MatchType | None = None,
    ) -> None:
        self.ops.append(
            ops.AddTableConstraintOp(
                constraint=ForeignKey(
                    name=name,
                    target_table=target_table,
                    target_columns=target_columns,
                    self_columns=self_columns,
                    on_delete=on_delete,
                    on_update=on_update,
                    match=match,
                ),
                table_name=self._table_name,
                only=self._only,
                if_table_exists=self._if_exists,
            )
        )

    def drop_constraint(
        self,
        constraint_name: str,
        current_constraint: Constraint,
        if_exists: bool = False,
        mode: DropMode | None = None,
    ) -> None:
        self.ops.append(
            ops.DropTableConstraintOp(
                mode=mode,
                if_exists=if_exists,
                table_name=self._table_name,
                constraint_name=constraint_name,
                current_constraint=current_constraint,
                only=self._only,
                if_table_exists=self._if_exists,
            )
        )


class Blueprint:
    def __init__(self) -> None:
        self._ops: list[ops.Operation] = []

    @contextlib.contextmanager  # type: ignore[arg-type]
    def create_table(  # type: ignore[misc]
        self,
        table_name: str,
        if_not_exists: bool = False,
    ) -> typing.ContextManager[CreateTableBuilder]:
        builder = CreateTableBuilder(table_name)
        yield builder
        self._ops.append(ops.CreateTableOp(table=builder._table, if_not_exists=if_not_exists))
        for index in builder._table.indices:
            self._ops.append(ops.CreateIndexOp(index=index))

    @contextlib.contextmanager  # type: ignore[arg-type]
    def alter_table(
        self,
        table_name: str,
        if_exists: bool = False,
        only: bool = False,
    ) -> typing.Generator[AlterTableBuilder, None, None]:
        builder = AlterTableBuilder(table_name=table_name, if_exists=if_exists, only=only)
        yield builder
        self._ops.extend(builder.ops)

    def drop_table(self, table_name: str, current_table: Table, mode: DropMode | None = None) -> None:
        self.add_op(ops.DropTableOp(name=table_name, mode=mode, current_table=current_table))

    def drop_index(self, index_name: str, current_index: Index, mode: DropMode | None = None) -> None:
        self._ops.append(
            ops.DropIndexOp(
                mode=mode,
                name=index_name,
                current_index=current_index,
            )
        )

    def run_sql(self, up_sql: str, down_sql: str) -> None:
        self.add_op(ops.RunSQLOp(up_sql, down_sql))

    def add_op(self, operation: ops.Operation) -> None:
        self._ops.append(operation)

    def get_ops(self) -> list[ops.Operation]:
        return self._ops
