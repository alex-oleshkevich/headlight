from __future__ import annotations

import contextlib
import typing

from headlight.schema import ops, types
from headlight.schema.schema import Action, CheckConstraint, Column, Constraint, ForeignKey, Index, IndexExpr, \
    MatchType, \
    PrimaryKeyConstraint, \
    UniqueConstraint


class CreateTableBuilder:
    def __init__(
        self,
        table_name: str,
    ) -> None:
        self._table_name = table_name
        self._columns: list[Column] = []
        self._constraints: list[Constraint] = []
        self._indices: list[Index] = []

    def add_column(
        self,
        name: str,
        type: types.Type,
        null: bool = False,
        default: str | None = None,
        primary_key: bool = False,
        auto_increment: bool = False,
        if_not_exists: bool = False,
        comment: str | None = None,
        unique: UniqueConstraint | bool | str | None = None,
        check: CheckConstraint | str | tuple[str, str] | None = None,
    ) -> Column:
        unique_constraint: UniqueConstraint | None = None
        match unique:
            case UniqueConstraint():
                unique_constraint = unique
            case True:
                unique_constraint = UniqueConstraint()
            case constraint_name if isinstance(constraint_name, str):
                unique_constraint = UniqueConstraint(name=constraint_name)

        check_constraint: CheckConstraint | None = None
        match check:
            case CheckConstraint():
                check_constraint = check
            case expr if isinstance(expr, str):
                check_constraint = CheckConstraint(expr=expr)
            case (constraint_name, expr):
                check_constraint = CheckConstraint(expr=expr, name=constraint_name)

        column = Column(
            name=name,
            type=type,
            null=null,
            default=default,
            primary_key=primary_key,
            auto_increment=auto_increment,
            if_not_exists=if_not_exists,
            comment=comment,
            unique_constraint=unique_constraint,
            check_constraint=check_constraint,
        )

        self._columns.append(column)
        return column

    def add_index(
        self,
        columns: list[str],
        name: str | None = None,
        unique: bool = False,
        using: str | None = None,
        include: list[str] | None = None,
        with_: str | None = None,
        where: str | None = None,
        tablespace: str | None = None,
    ) -> None:
        index_exprs = [IndexExpr(column) if isinstance(column, str) else column for column in columns]
        index_name = self._table_name + '_' + '_'.join([expr.column for expr in index_exprs]) + '_idx'
        self._indices.append(Index(
            name=index_name, table_name=self._table_name, unique=unique, using=using, columns=index_exprs,
            include=include, with_=with_, tablespace=tablespace, where=where,
        ))

    def add_check_constraint(self, expr: str, name: str | None = None) -> None:
        self._constraints.append(CheckConstraint(expr, name))

    def add_unique_constraint(
        self,
        columns: list[str],
        name: str | None = None,
        include: list[str] | None = None,
    ) -> None:
        self._constraints.append(UniqueConstraint(name=name, include=include, columns=columns))

    def add_primary_key(self, columns: list[str], name: str | None = None, include: list[str] | None = None) -> None:
        self._constraints.append(PrimaryKeyConstraint(name=name, columns=columns, include=include))

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
        self._constraints.append(
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

    def set_default(self, new_default: str, current_default: str | None) -> ChangeColumn:
        self._ops.append(
            ops.SetDefaultOp(
                table_name=self._table_name,
                column_name=self._column_name,
                new_default=new_default,
                current_default=current_default,
                only=self._only,
                if_table_exists=self._if_table_exists,
            )
        )
        return self

    def drop_default(self, current_default: str | None) -> ChangeColumn:
        self._ops.append(
            ops.DropDefaultOp(
                table_name=self._table_name,
                column_name=self._column_name,
                current_default=current_default,
                only=self._only,
                if_table_exists=self._if_table_exists,
            )
        )
        return self

    def set_nullable(self, flag: bool) -> ChangeColumn:
        if flag:
            self._ops.append(
                ops.DropNullOp(
                    table_name=self._table_name,
                    column_name=self._column_name,
                    only=self._only,
                    if_table_exists=self._if_table_exists,
                )
            )
        else:
            self._ops.append(
                ops.SetNullOp(
                    table_name=self._table_name,
                    column_name=self._column_name,
                    only=self._only,
                    if_table_exists=self._if_table_exists,
                )
            )
        return self

    def change_type(
        self,
        new_type: types.Type,
        current_type: types.Type,
        collation: str | None = None,
        current_collation: str | None = None,
        using: str | None = None,
        current_using: str | None = None,
    ) -> ChangeColumn:
        self._ops.append(
            ops.ChangeTypeOp(
                table_name=self._table_name,
                column_name=self._column_name,
                new_type=new_type,
                current_type=current_type,
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
        type: types.Type,
        null: bool = False,
        primary_key: bool | None = None,
        default: str | None = None,
        unique: bool | UniqueConstraint | None = None,
        check: CheckConstraint | None = None,
        if_table_exists: bool = False,
        if_column_not_exists: bool = False,
        collate: str | None = None,
    ) -> ops.AddColumnOp:
        unique_constraint: UniqueConstraint | None = None
        match unique:
            case UniqueConstraint():
                unique_constraint = unique
            case True:
                unique_constraint = UniqueConstraint()
            case constraint_name if isinstance(constraint_name, str):
                unique_constraint = UniqueConstraint(name=constraint_name)

        check_constraint: CheckConstraint | None = None
        match check:
            case CheckConstraint():
                check_constraint = check
            case expr if isinstance(expr, str):
                check_constraint = CheckConstraint(expr=expr)
            case (constraint_name, expr):
                check_constraint = CheckConstraint(expr=expr, name=constraint_name)

        op = ops.AddColumnOp(
            table_name=self._table_name,
            column_name=name,
            type=type,
            if_table_exists=if_table_exists,
            unique_constraint=unique_constraint,
            check_constraint=check_constraint,
            collate=collate,
            only=self._only,
            null=null,
            default=default,
            primary_key=primary_key,
            if_column_not_exists=if_column_not_exists,
        )
        self.ops.append(op)
        return op

    def drop_column(self, name: str, create_column: ops.AddColumnOp, if_column_exists: bool = False) -> None:
        self.ops.append(
            ops.DropColumnOp(
                only=self._only,
                column_name=name,
                table_name=self._table_name,
                create_column=create_column,
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
                constraint=PrimaryKeyConstraint(name=name, columns=columns, include=include),
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
        self._ops.append(ops.CreateTableOp(
            table_name=table_name,
            columns=builder._columns,
            constraints=builder._constraints,
            indices=builder._indices,
            if_not_exists=if_not_exists,
        ))

    @contextlib.contextmanager  # type: ignore[arg-type]
    def alter_table(
        self,
        table_name: str,
        if_exists: bool = False,
        only: bool = False,
    ) -> typing.ContextManager[AlterTableBuilder]:  # type: ignore[misc]
        builder = AlterTableBuilder(table_name=table_name, if_exists=if_exists, only=only)
        yield builder
        self._ops.extend(builder.ops)

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
        self.add_op(ops.RunSQLOp(up_sql, down_sql))

    def add_op(self, operation: ops.Operation) -> None:
        self._ops.append(operation)

    def get_ops(self) -> list[ops.Operation]:
        return self._ops
