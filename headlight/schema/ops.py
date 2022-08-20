import dataclasses

import abc

from headlight import DbDriver
from headlight.schema.types import Type


@dataclasses.dataclass
class Column:
    name: str
    type: Type
    null: bool = False
    default: str | None = None
    unique: bool = False
    primary_key: bool = False
    auto_increment: bool = False
    if_not_exists: bool = False
    comment: str | None = None


class Operation(abc.ABC):
    @abc.abstractmethod
    def to_up_sql(self, driver: DbDriver) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def to_down_sql(self, driver: DbDriver) -> str:
        raise NotImplementedError()


class RunSqlOp(Operation):
    def __init__(self, up_sql: str, down_sql: str) -> None:
        self.up_sql = up_sql
        self.down_sql = down_sql

    def to_up_sql(self, driver: DbDriver) -> str:
        return self.up_sql

    def to_down_sql(self, driver: DbDriver) -> str:
        return self.down_sql


class AddColumnOp(Operation):
    def __init__(
        self,
        column_name: str,
        column_type: Type,
        null: bool = False,
        default: str | None = None,
        unique: bool = False,
        primary_key: bool = False,
        auto_increment: bool = False,
        if_not_exists: bool = False,
        comment: str | None = None,
    ) -> None:
        self._column_name = column_name
        self._column_type = column_type
        self._null = null
        self._default = default
        self._unique = unique
        self._primary_key = primary_key
        self._auto_increment = auto_increment
        self._if_not_exists = if_not_exists
        self._comment = comment


class CreateTableOp(Operation):
    def __init__(
        self,
        table_name: str,
        if_not_exists: bool = False,
    ) -> None:
        self.table_name = table_name
        self.if_not_exists = if_not_exists
        self.columns: list[Column] = []
        self.extra_ops: list[Operation] = []

    def add_column(self, column: Column) -> None:
        self.columns.append(column)

    def to_up_sql(self, driver: DbDriver) -> str:
        pk_cols = [col for col in self.columns if col.primary_key]
        pk_count = len(pk_cols)

        column_stmts = [
            '    '
            + driver.column_template.format(
                name=column.name,
                type=driver.get_sql_for_type(column.type),
                null='' if column.null else ' NOT NULL',
                unique=' UNIQUE' if column.unique else '',
                default=f' DEFAULT {column.default}' if column.default is not None else '',
                primary_key=' PRIMARY KEY' if pk_count == 1 and column.primary_key else '',
            )
            for column in self.columns
        ]

        if pk_count > 1:
            column_stmts.append('    PRIMARY KEY (%s)' % ', '.join([col.name for col in pk_cols]))

        return driver.create_table_template.format(
            name=self.table_name,
            column_sql='\n' + ',\n'.join(column_stmts) + '\n',
            if_not_exists=' IF NOT EXISTS' if self.if_not_exists else ' ',
        )

    def to_down_sql(self, driver: DbDriver) -> str:
        return driver.drop_table_template.format(name=self.table_name)
