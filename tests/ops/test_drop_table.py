from headlight import DbDriver
from headlight.schema import types
from headlight.schema.ops import DropTableOp
from headlight.schema.schema import Column, Table

table = Table(
    name="users",
    columns=[
        Column("id", type=types.BigIntegerType()),
        Column("name", type=types.VarCharType()),
    ],
)


def test_op_forward(postgres: DbDriver) -> None:
    sql = DropTableOp(name="users", current_table=table).to_up_sql(postgres)

    assert sql == "DROP TABLE users"


def test_op_reverse(postgres: DbDriver) -> None:
    sql = DropTableOp(name="users", current_table=table).to_down_sql(postgres)

    assert sql == ("CREATE TABLE users (\n" "    id BIGINT NOT NULL,\n" "    name VARCHAR NOT NULL\n" ")")
