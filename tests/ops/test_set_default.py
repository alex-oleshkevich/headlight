from headlight import DbDriver
from headlight.schema.ops import SetDefaultOp
from headlight.schema.schema import Default, NowExpr


def test_op_forward(postgres: DbDriver) -> None:
    sql = SetDefaultOp(
        table_name="users",
        column_name="name",
        new_default="root",
        current_default="user",
        only=True,
        if_table_exists=True,
    ).to_up_sql(postgres)

    assert sql == "ALTER TABLE IF EXISTS ONLY users ALTER name SET DEFAULT 'root'"


def test_op_reverse(postgres: DbDriver) -> None:
    sql = SetDefaultOp(
        table_name="users",
        column_name="name",
        new_default="root",
        current_default="user",
        only=True,
        if_table_exists=True,
    ).to_down_sql(postgres)

    assert sql == "ALTER TABLE IF EXISTS ONLY users ALTER name SET DEFAULT 'user'"


def test_op_when_current_default_is_unset(postgres: DbDriver) -> None:
    forward_sql = SetDefaultOp(table_name="users", column_name="name", new_default="root").to_up_sql(postgres)
    assert forward_sql == "ALTER TABLE users ALTER name SET DEFAULT 'root'"

    reverse_sql = SetDefaultOp(table_name="users", column_name="name", new_default="root").to_down_sql(postgres)
    assert reverse_sql == "ALTER TABLE users ALTER name DROP DEFAULT"


def test_op_uses_quotes_empty_string(postgres: DbDriver) -> None:
    forward_sql = SetDefaultOp(table_name="users", column_name="name", new_default="").to_up_sql(postgres)
    assert forward_sql == "ALTER TABLE users ALTER name SET DEFAULT ''"


def test_op_for_list(postgres: DbDriver) -> None:
    forward_sql = SetDefaultOp(table_name="users", column_name="name", new_default=[]).to_up_sql(postgres)
    assert forward_sql == "ALTER TABLE users ALTER name SET DEFAULT '[]'"


def test_op_for_dict(postgres: DbDriver) -> None:
    forward_sql = SetDefaultOp(table_name="users", column_name="name", new_default={}).to_up_sql(postgres)
    assert forward_sql == "ALTER TABLE users ALTER name SET DEFAULT '{}'"


def test_op_for_bool(postgres: DbDriver) -> None:
    forward_sql = SetDefaultOp(table_name="users", column_name="name", new_default=True).to_up_sql(postgres)
    assert forward_sql == "ALTER TABLE users ALTER name SET DEFAULT 't'"


def test_op_for_default(postgres: DbDriver) -> None:
    default = Default("value")
    forward_sql = SetDefaultOp(table_name="users", column_name="name", new_default=default).to_up_sql(postgres)
    assert forward_sql == "ALTER TABLE users ALTER name SET DEFAULT 'value'"


def test_op_for_expr(postgres: DbDriver) -> None:
    expr = NowExpr()
    forward_sql = SetDefaultOp(table_name="users", column_name="name", new_default=expr).to_up_sql(postgres)
    assert forward_sql == "ALTER TABLE users ALTER name SET DEFAULT CURRENT_TIMESTAMP"
