import pytest

from headlight import CheckConstraint, DbDriver
from headlight.schema.ops import AddTableConstraintOp, OperationError


def test_op_forward(postgres: DbDriver) -> None:
    sql = AddTableConstraintOp(
        constraint=CheckConstraint(expr="email is not null"),
        table_name="users",
        only=True,
        if_table_exists=True,
    ).to_up_sql(postgres)

    assert sql == "ALTER TABLE IF EXISTS ONLY users ADD CHECK (email is not null)"


def test_op_reverse(postgres: DbDriver) -> None:
    sql = AddTableConstraintOp(
        constraint=CheckConstraint(expr="email is not null", name="email_check"),
        table_name="users",
        only=True,
        if_table_exists=True,
    ).to_down_sql(postgres)

    assert sql == "ALTER TABLE IF EXISTS ONLY users DROP CONSTRAINT email_check"


def test_op_reverse_requires_constraint_name(postgres: DbDriver) -> None:
    with pytest.raises(OperationError):
        AddTableConstraintOp(
            constraint=CheckConstraint(expr="email is not null"),
            table_name="users",
            only=True,
            if_table_exists=True,
        ).to_down_sql(postgres)
