import contextlib
import typing

from headlight.schema.ops import CreateIndexOp, CreateTableOp, IndexExpr, Operation, RunSqlOp


class Schema:
    def __init__(self) -> None:
        self._ops: list[Operation] = []

    @contextlib.contextmanager  # type: ignore[arg-type]
    def create_table(  # type: ignore[misc]
        self,
        table_name: str,
        if_not_exists: bool = False,
    ) -> typing.ContextManager[CreateTableOp]:
        op = CreateTableOp(table_name, if_not_exists=if_not_exists)
        yield op
        self._ops.extend([op, *op.extra_ops])

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
    ) -> CreateIndexOp:
        op = CreateIndexOp(
            table=table, name=name, unique=unique, concurrently=concurrently, if_not_exists=if_not_exists, only=only,
            using=using, include=include, with_=with_, where=where, tablespace=tablespace,
            columns=[IndexExpr(column=column) if isinstance(column, str) else column for column in columns],
        )
        self._ops.append(op)
        return op

    def run_sql(self, up_sql: str, down_sql: str) -> None:
        self.add_op(RunSqlOp(up_sql, down_sql))

    def add_op(self, operation: Operation) -> None:
        self._ops.append(operation)

    def get_ops(self) -> list[Operation]:
        return self._ops
