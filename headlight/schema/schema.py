import contextlib
import typing

from headlight.schema.ops import CreateTableOp, Operation, RunSqlOp


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

    def run_sql(self, up_sql: str, down_sql: str) -> None:
        self.add_op(RunSqlOp(up_sql, down_sql))

    def add_op(self, operation: Operation) -> None:
        self._ops.append(operation)

    def get_ops(self) -> list[Operation]:
        return self._ops
