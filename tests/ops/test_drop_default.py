from headlight import DbDriver
from headlight.schema.ops import DropDefaultOp
from headlight.schema.schema import Default


def test_op_forward(postgres: DbDriver) -> None:
    sql = DropDefaultOp(
        table_name="users",
        column_name="name",
        current_default="'user'",
        only=True,
        if_table_exists=True,
    ).to_up_sql(postgres)

    assert sql == "ALTER TABLE IF EXISTS ONLY users ALTER name DROP DEFAULT"


def test_op_reverse(postgres: DbDriver) -> None:
    sql = DropDefaultOp(
        table_name="users",
        column_name="name",
        current_default="root",
        only=True,
        if_table_exists=True,
    ).to_down_sql(postgres)

    assert sql == "ALTER TABLE IF EXISTS ONLY users ALTER name SET DEFAULT 'root'"


def test_op_when_old_default_is_unset(postgres: DbDriver) -> None:
    forward_sql = DropDefaultOp(table_name="users", column_name="name", current_default=Default(None)).to_up_sql(
        postgres
    )
    assert forward_sql == "ALTER TABLE users ALTER name DROP DEFAULT"

    reverse_sql = DropDefaultOp(table_name="users", column_name="name", current_default=Default(None)).to_down_sql(
        postgres
    )
    assert reverse_sql == "-- noop, column had no default previously"


def test_op_quotes_empty_string(postgres: DbDriver) -> None:
    reverse_sql = DropDefaultOp(table_name="users", column_name="name", current_default="").to_down_sql(postgres)
    assert reverse_sql == "ALTER TABLE users ALTER name SET DEFAULT ''"
