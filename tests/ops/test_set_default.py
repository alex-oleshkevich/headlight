from headlight import DbDriver
from headlight.schema.ops import SetDefaultOp


def test_op_forward(postgres: DbDriver) -> None:
    sql = SetDefaultOp(
        table_name="users",
        column_name="name",
        new_default="'root'",
        current_default="'user'",
        only=True,
        if_table_exists=True,
    ).to_up_sql(postgres)

    assert sql == ("ALTER TABLE IF EXISTS ONLY users " "ALTER name SET DEFAULT 'root'")


def test_op_reverse(postgres: DbDriver) -> None:
    sql = SetDefaultOp(
        table_name="users",
        column_name="name",
        new_default="'root'",
        current_default="'user'",
        only=True,
        if_table_exists=True,
    ).to_down_sql(postgres)

    assert sql == ("ALTER TABLE IF EXISTS ONLY users " "ALTER name SET DEFAULT 'user'")


def test_op_when_current_default_is_unset(postgres: DbDriver) -> None:
    forward_sql = SetDefaultOp(table_name="users", column_name="name", new_default="'root'").to_up_sql(postgres)
    assert forward_sql == "ALTER TABLE users ALTER name SET DEFAULT 'root'"

    reverse_sql = SetDefaultOp(table_name="users", column_name="name", new_default="'root'").to_down_sql(postgres)
    assert reverse_sql == "ALTER TABLE users ALTER name DROP DEFAULT"
