from __future__ import annotations

import contextlib
import typing

from headlight.schema import ops, types
from headlight.schema.schema import IndexExpr


class Schema:
    def __init__(self) -> None:
        self._ops: list[ops.Operation] = []

    @contextlib.contextmanager  # type: ignore[arg-type]
    def create_table(  # type: ignore[misc]
        self,
        table_name: str,
        if_not_exists: bool = False,
    ) -> typing.ContextManager[ops.CreateTableOp]:
        op = ops.CreateTableOp(table_name, if_not_exists=if_not_exists)
        yield op
        self._ops.extend([op, *op.extra_ops])

    @contextlib.contextmanager  # type: ignore[arg-type]
    def alter_table(
        self,
        table_name: str,
        if_exists: bool = False,
        only: bool = False,
    ) -> typing.ContextManager[ops.AlterTableOp]:  # type: ignore[misc]
        op = ops.AlterTableOp(table_name=table_name, if_exists=if_exists, only=only)
        yield op
        self._ops.extend(op.extra_ops)

    def drop_table(self, table_name: str, create_table: ops.CreateTableOp) -> None:
        self.add_op(ops.DropTableOp(name=table_name, create_table=create_table))

    def add_index(
        self,
        table: str,
        columns: list[str | IndexExpr],
        name: str | None = None,
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
        self.add_op(
            ops.CreateIndexOp(
                table=table,
                name=name,
                unique=unique,
                concurrently=concurrently,
                if_not_exists=if_not_exists,
                only=only,
                using=using,
                include=include,
                with_=with_,
                where=where,
                tablespace=tablespace,
                columns=[ops.IndexExpr(column=column) if isinstance(column, str) else column for column in columns],
            )
        )

    def drop_index(self, index_name: str, create_index: ops.CreateIndexOp) -> None:
        self.add_op(ops.DropIndexOp(name=index_name, create_index=create_index))

    def add_column(
        self,
        table_name: str,
        column_name: str,
        type: types.Type,
        null: bool = False,
        if_column_not_exists: bool = False,
        if_table_exists: bool = False,
        unique_constraint: ops.UniqueConstraint | None = None,
        check_constraint: ops.CheckConstraint | None = None,
        collate: str | None = None,
        only: bool = False,
        default: str | None = None,
        primary_key: bool | None = None,
        foreign_key: ops.ForeignKey | None = None,
    ) -> None:
        self.add_op(
            ops.AddColumnOp(
                table_name=table_name,
                column_name=column_name,
                type=type,
                if_column_not_exists=if_column_not_exists,
                if_table_exists=if_table_exists,
                unique_constraint=unique_constraint,
                check_constraint=check_constraint,
                collate=collate,
                only=only,
                null=null,
                default=default,
                primary_key=primary_key,
                foreign_key=foreign_key,
            )
        )

    def drop_column(
        self,
        table_name: str,
        column_name: str,
        create_column: ops.AddColumnOp,
        if_column_exists: bool = False,
        if_table_exists: bool = False,
        only: bool = False,
    ) -> None:
        self.add_op(
            ops.DropColumnOp(
                table_name=table_name,
                column_name=column_name,
                if_table_exists=if_table_exists,
                if_column_exists=if_column_exists,
                only=only,
                create_column=create_column,
            )
        )

    def add_constraint(
        self,
        table_name: str,
        constraint: ops.Constraint,
        only: bool = False,
        if_table_exists: bool = False,
    ) -> None:
        self.add_op(
            ops.AddTableConstraintOp(
                constraint=constraint, table_name=table_name, only=only, if_table_exists=if_table_exists
            )
        )

    def drop_constraint(
        self,
        constraint_name: str,
        table_name: str,
        if_exists: bool = False,
        only: bool = False,
        if_table_exists: bool = False,
    ) -> None:
        self.add_op(
            ops.DropTableConstraintOp(
                constraint_name=constraint_name,
                table_name=table_name,
                only=only,
                if_exists=if_exists,
                if_table_exists=if_table_exists,
            )
        )

    def run_sql(self, up_sql: str, down_sql: str) -> None:
        self.add_op(ops.RunSqlOp(up_sql, down_sql))

    def add_op(self, operation: ops.Operation) -> None:
        self._ops.append(operation)

    def get_ops(self) -> list[ops.Operation]:
        return self._ops
