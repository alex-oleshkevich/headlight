from headlight import DbDriver
from headlight.schema import types
from headlight.schema.ops import ChangeTypeOp


def test_op_forward(postgres: DbDriver) -> None:
    sql = ChangeTypeOp(
        table_name="users",
        column_name="amount",
        new_type=types.BigIntegerType(),
        current_type=types.VarCharType(512),
        only=True,
        if_table_exists=True,
        collation="german",
        using="amount::bigint",
        current_collation="none",
        current_using="amount::varchar",
    ).to_up_sql(postgres)

    assert sql == "ALTER TABLE IF EXISTS ONLY users ALTER amount TYPE BIGINT COLLATE german USING amount::bigint"


def test_op_reverse(postgres: DbDriver) -> None:
    sql = ChangeTypeOp(
        table_name="users",
        column_name="amount",
        new_type=types.BigIntegerType(),
        current_type=types.VarCharType(512),
        only=True,
        if_table_exists=True,
        collation="german",
        using="amount::bigint",
        current_collation="none",
        current_using="amount::varchar",
    ).to_down_sql(postgres)

    assert sql == "ALTER TABLE IF EXISTS ONLY users ALTER amount TYPE VARCHAR(512) COLLATE none USING amount::varchar"
