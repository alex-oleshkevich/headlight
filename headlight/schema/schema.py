from headlight.schema.ops import Operation, RunSqlOp


class Schema:
    def __init__(self) -> None:
        self._ops: list[Operation] = []

    def run_sql(self, up_sql: str, down_sql: str) -> None:
        self.add_op(RunSqlOp(up_sql, down_sql))

    def add_op(self, operation: Operation) -> None:
        self._ops.append(operation)

    def get_upgrade_commands(self) -> list[str]:
        return [op.to_up_sql() for op in self._ops]

    def get_down_commands(self) -> list[str]:
        return [op.to_down_sql() for op in self._ops]
