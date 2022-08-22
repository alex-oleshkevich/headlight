from headlight import DbDriver
from headlight.schema.ops import DropNotNullOp


def test_op_forward(postgres: DbDriver) -> None:
    sql = DropNotNullOp(
        table_name="users",
        column_name="name",
        only=True,
        if_table_exists=True,
    ).to_up_sql(postgres)

    assert sql == ("ALTER TABLE IF EXISTS ONLY users " "ALTER name DROP NOT NULL")


def test_op_reverse(postgres: DbDriver) -> None:
    sql = DropNotNullOp(
        table_name="users",
        column_name="name",
        only=True,
        if_table_exists=True,
    ).to_down_sql(postgres)

    assert sql == ("ALTER TABLE IF EXISTS ONLY users " "ALTER name SET NOT NULL")
