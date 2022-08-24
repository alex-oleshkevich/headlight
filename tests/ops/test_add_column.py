from headlight import DbDriver
from headlight.schema import types
from headlight.schema.ops import AddColumnOp
from headlight.schema.schema import CheckConstraint, Column, ForeignKey, GeneratedAs, UniqueConstraint

column = Column(
    name="email",
    type=types.VarCharType(),
    collate="german",
    null=True,
    default="'root@localhost'",
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
    sql = AddColumnOp(
        table_name="users",
        column=column,
        only=True,
        if_table_exists=True,
        if_column_not_exists=True,
    ).to_up_sql(postgres)

    assert sql == (
        "ALTER TABLE IF EXISTS ONLY users "
        "ADD IF NOT EXISTS email VARCHAR "
        "PRIMARY KEY "
        "DEFAULT 'root@localhost' COLLATE \"german\" "
        "CONSTRAINT email_check CHECK (email is not null) "
        "CONSTRAINT email_udx UNIQUE "
        "CONSTRAINT profiles_fk REFERENCES profiles (id) "
        "MATCH FULL ON DELETE CASCADE ON UPDATE CASCADE "
        "GENERATED ALWAYS AS (first_name || '-new') STORED"
    )


def test_op_reverse(postgres: DbDriver) -> None:
    sql = AddColumnOp(
        table_name="users",
        column=column,
        only=True,
        if_table_exists=True,
        if_column_not_exists=True,
    ).to_down_sql(postgres)

    assert sql == "ALTER TABLE IF EXISTS ONLY users DROP IF EXISTS email"


def test_op_with_multiple_checks(postgres: DbDriver) -> None:
    sql = AddColumnOp(
        table_name="users",
        column=Column(
            name="email",
            type=types.VarCharType(),
            check_constraints=[
                CheckConstraint(expr="email is not null", name="check1"),
                CheckConstraint(expr="length(email) > 0", name="check2"),
            ],
        ),
    ).to_up_sql(postgres)

    assert sql == (
        "ALTER TABLE users "
        "ADD email VARCHAR NOT NULL "
        "CONSTRAINT check1 CHECK (email is not null) "
        "CONSTRAINT check2 CHECK (length(email) > 0)"
    )


def test_op_check_helper(postgres: DbDriver) -> None:
    sql = (
        AddColumnOp(
            table_name="users",
            column=Column(
                name="email",
                type=types.VarCharType(),
            ),
        )
        .check(expr="email is not null", name="check1")
        .to_up_sql(postgres)
    )

    assert sql == ("ALTER TABLE users " "ADD email VARCHAR NOT NULL " "CONSTRAINT check1 CHECK (email is not null)")


def test_op_unique_helper(postgres: DbDriver) -> None:
    sql = (
        AddColumnOp(
            table_name="users",
            column=Column(
                name="email",
                type=types.VarCharType(),
            ),
        )
        .unique()
        .to_up_sql(postgres)
    )

    assert sql == ("ALTER TABLE users " "ADD email VARCHAR NOT NULL UNIQUE")


def test_op_references_helper(postgres: DbDriver) -> None:
    sql = (
        AddColumnOp(
            table_name="users",
            column=Column(
                name="email",
                type=types.VarCharType(),
            ),
        )
        .references("profiles", ["id"])
        .to_up_sql(postgres)
    )

    assert sql == ("ALTER TABLE users " "ADD email VARCHAR NOT NULL REFERENCES profiles (id)")


def test_op_generated_as_helper(postgres: DbDriver) -> None:
    sql = (
        AddColumnOp(
            table_name="users",
            column=Column(
                name="email",
                type=types.VarCharType(),
            ),
        )
        .generated_as(expr="first_name || '-new'", stored=True)
        .to_up_sql(postgres)
    )

    assert sql == ("ALTER TABLE users " "ADD email VARCHAR NOT NULL GENERATED ALWAYS AS (first_name || '-new') STORED")
