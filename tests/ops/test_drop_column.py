from headlight import DbDriver
from headlight.schema import types
from headlight.schema.ops import DropColumnOp
from headlight.schema.schema import CheckConstraint, Column, Default, ForeignKey, GeneratedAs, UniqueConstraint

column = Column(
    name="email",
    type=types.VarCharType(),
    collate="german",
    null=True,
    default=Default("root@localhost"),
    primary_key=True,
    unique_constraint=UniqueConstraint(name="email_udx"),
    check_constraints=[
        CheckConstraint(expr="email is not null", name="email_check"),
    ],
    foreign_key=ForeignKey(
        target_table="profiles",
        target_columns=["id"],
        on_delete="CASCADE",
        on_update="CASCADE",
        name="profiles_fk",
        match="FULL",
    ),
    generated_as_=GeneratedAs(expr="first_name || '-new'", stored=True),
)


def test_op_forward(postgres: DbDriver) -> None:
    sql = DropColumnOp(
        table_name="users",
        column_name="email",
        current_column=Column(
            name="email",
            type=types.VarCharType(),
        ),
        only=True,
        mode="CASCADE",
        if_table_exists=True,
        if_column_exists=True,
    ).to_up_sql(postgres)

    assert sql == "ALTER TABLE IF EXISTS ONLY users DROP IF EXISTS email CASCADE"


def test_op_reverse(postgres: DbDriver) -> None:
    sql = DropColumnOp(
        table_name="users",
        column_name="email",
        current_column=Column(
            name="email",
            type=types.VarCharType(),
        ),
        only=True,
        mode="CASCADE",
        if_table_exists=True,
        if_column_exists=True,
    ).to_down_sql(postgres)

    assert sql == "ALTER TABLE ONLY users ADD email VARCHAR NOT NULL"
