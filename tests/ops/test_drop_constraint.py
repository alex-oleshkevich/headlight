from headlight import CheckConstraint, DbDriver
from headlight.schema.ops import DropTableConstraintOp


def test_op_forward(postgres: DbDriver) -> None:
    sql = DropTableConstraintOp(
        constraint_name="email_check",
        current_constraint=CheckConstraint(expr="email is not null", name="email_check"),
        mode="CASCADE",
        table_name="users",
        only=True,
        if_exists=True,
        if_table_exists=True,
    ).to_up_sql(postgres)

    assert sql == "ALTER TABLE IF EXISTS ONLY users DROP CONSTRAINT IF EXISTS email_check CASCADE"


def test_op_reverse(postgres: DbDriver) -> None:
    sql = DropTableConstraintOp(
        constraint_name="email_check",
        current_constraint=CheckConstraint(expr="email is not null", name="email_check"),
        mode="CASCADE",
        table_name="users",
        only=True,
        if_exists=True,
        if_table_exists=True,
    ).to_down_sql(postgres)

    assert sql == "ALTER TABLE IF EXISTS ONLY users ADD CONSTRAINT email_check CHECK (email is not null)"
