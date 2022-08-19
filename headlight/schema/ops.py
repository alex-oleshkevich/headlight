import abc


class Operation(abc.ABC):
    @abc.abstractmethod
    def to_up_sql(self) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def to_down_sql(self) -> str:
        raise NotImplementedError()


class RunSqlOp(Operation):
    def __init__(self, up_sql: str, down_sql: str) -> None:
        self.up_sql = up_sql
        self.down_sql = down_sql

    def to_up_sql(self) -> str:
        return self.up_sql

    def to_down_sql(self) -> str:
        return self.down_sql
