from headlight import DbDriver
from headlight.schema import types
from headlight.schema.ops import CreateTableOp
from headlight.schema.schema import (
    CheckConstraint,
    Column,
    Index,
    IndexExpr,
    PrimaryKeyConstraint,
    Table,
    UniqueConstraint,
)

table = Table(
    name="users",
    columns=[
        Column("id", type=types.BigIntegerType()),
        Column("name", type=types.VarCharType()),
    ],
    constraints=[
        UniqueConstraint(columns=["name"]),
        CheckConstraint(expr="name is not null"),
        PrimaryKeyConstraint(columns=["id"]),
    ],
    indices=[
        Index(name="users_name_idx", table_name="users", columns=[IndexExpr("name")]),
    ],
)


def test_op_forward(postgres: DbDriver) -> None:
    sql = CreateTableOp(table=table, if_not_exists=True).to_up_sql(postgres)

    assert sql == (
        "CREATE TABLE IF NOT EXISTS users (\n"
        "    id BIGINT NOT NULL,\n"
        "    name VARCHAR NOT NULL,\n"
        "    UNIQUE (name),\n"
        "    CHECK (name is not null),\n"
        "    PRIMARY KEY (id)\n"
        ")"
    )


def test_op_with_composite_single_key(postgres: DbDriver) -> None:
    sql = CreateTableOp(
        table=Table(
            name="users",
            columns=[
                Column("id", type=types.BigIntegerType(), primary_key=True),
                Column("name", type=types.VarCharType()),
            ],
        )
    ).to_up_sql(postgres)

    assert sql == ("CREATE TABLE users (\n" "    id BIGINT PRIMARY KEY NOT NULL,\n" "    name VARCHAR NOT NULL\n" ")")


def test_op_with_composite_primary_key(postgres: DbDriver) -> None:
    sql = CreateTableOp(
        table=Table(
            name="users",
            columns=[
                Column("id", type=types.BigIntegerType(), primary_key=True),
                Column("name", type=types.VarCharType(), primary_key=True),
            ],
        )
    ).to_up_sql(postgres)

    assert sql == (
        "CREATE TABLE users (\n"
        "    id BIGINT NOT NULL,\n"
        "    name VARCHAR NOT NULL,\n"
        "    PRIMARY KEY (id, name)\n"
        ")"
    )


def test_op_reverse(postgres: DbDriver) -> None:
    sql = CreateTableOp(
        table=Table(
            name="users",
            columns=[
                Column("id", type=types.BigIntegerType(), primary_key=True),
                Column("name", type=types.VarCharType(), primary_key=True),
            ],
        )
    ).to_down_sql(postgres)

    assert sql == "DROP TABLE users"
