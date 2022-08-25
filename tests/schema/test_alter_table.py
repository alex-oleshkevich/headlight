from headlight.schema import ops, types
from headlight.schema.builder import AlterTableBuilder, ChangeColumn
from headlight.schema.schema import (
    CheckConstraint,
    Column,
    Default,
    ForeignKey,
    GeneratedAs,
    PrimaryKeyConstraint,
    UniqueConstraint,
)


def test_add_column() -> None:
    builder = AlterTableBuilder(table_name="users")
    builder.add_column(
        "id",
        types.BigIntegerType,
        null=True,
        primary_key=True,
        default="default",
        unique=True,
        checks=["id > 0"],
        if_table_exists=True,
        if_column_not_exists=True,
        collate="belarusian",
        generated_as="1",
    )
    match builder.ops[0]:
        case ops.AddColumnOp(
            table_name="users",
            if_table_exists=True,
            if_column_not_exists=True,
            column=Column(
                name="id",
                null=True,
                primary_key=True,
                default=Default(value="default"),
                collate="belarusian",
                unique_constraint=UniqueConstraint(),
                check_constraints=[CheckConstraint(expr="id > 0")],
                generated_as_=GeneratedAs(expr="1", stored=True),
            ),
        ):
            assert True
        case _:
            assert False


def test_drop_column() -> None:
    builder = AlterTableBuilder(table_name="users")
    builder.drop_column(
        name="id",
        current_column=Column("id", types.BigIntegerType()),
        if_column_exists=True,
        mode="CASCADE",
    )
    match builder.ops[0]:
        case ops.DropColumnOp(
            mode="CASCADE",
            column_name="id",
            table_name="users",
            if_column_exists=True,
            old_column=Column("id", types.BigIntegerType()),
        ):
            assert True
        case _:
            assert False


def test_alter_column() -> None:
    builder = AlterTableBuilder(table_name="users", only=True, if_exists=True)
    op = builder.alter_column("id")
    match op:
        case ChangeColumn(_table_name="users", _column_name="id", _only=True, _if_table_exists=True):
            assert True
        case _:
            assert False


def test_add_check_constraint() -> None:
    builder = AlterTableBuilder(table_name="users", only=True, if_exists=True)
    builder.add_check_constraint("name", expr="expr")
    match builder.ops[0]:
        case ops.AddTableConstraintOp(
            constraint=CheckConstraint(name="name", expr="expr"),
            table_name="users",
            only=True,
            if_table_exists=True,
        ):
            assert True
        case _:
            assert False


def test_add_unique_constraint() -> None:
    builder = AlterTableBuilder(table_name="users", only=True, if_exists=True)
    builder.add_unique_constraint("name", columns=["id"], include=["email"])
    match builder.ops[0]:
        case ops.AddTableConstraintOp(
            constraint=UniqueConstraint(name="name", columns=["id"], include=["email"]),
            table_name="users",
            only=True,
            if_table_exists=True,
        ):
            assert True
        case _:
            assert False


def test_add_primary_key() -> None:
    builder = AlterTableBuilder(table_name="users", only=True, if_exists=True)
    builder.add_primary_key("name", columns=["id"], include=["email"])
    match builder.ops[0]:
        case ops.AddTableConstraintOp(
            constraint=PrimaryKeyConstraint(name="name", columns=["id"], include=["email"]),
            table_name="users",
            only=True,
            if_table_exists=True,
        ):
            assert True
        case _:
            assert False


def test_add_foreign_key() -> None:
    builder = AlterTableBuilder(table_name="users", only=True, if_exists=True)
    builder.add_foreign_key(
        name="fk",
        target_table="profiles",
        target_columns=["id"],
        self_columns=["profile_id"],
        on_update="CASCADE",
        on_delete="CASCADE",
        match="FULL",
    )
    match builder.ops[0]:
        case ops.AddTableConstraintOp(
            constraint=ForeignKey(
                name="fk",
                target_table="profiles",
                target_columns=["id"],
                self_columns=["profile_id"],
                on_update="CASCADE",
                on_delete="CASCADE",
                match="FULL",
            ),
            table_name="users",
            only=True,
            if_table_exists=True,
        ):
            assert True
        case _:
            assert False


def test_drop_constraint() -> None:
    builder = AlterTableBuilder(table_name="users", only=True, if_exists=True)
    builder.drop_constraint(
        constraint_name="uniq", current_constraint=UniqueConstraint(), if_exists=True, mode="CASCADE"
    )
    match builder.ops[0]:
        case ops.DropTableConstraintOp(
            constraint_name="uniq",
            current_constraint=UniqueConstraint(),
            if_exists=True,
            mode="CASCADE",
            table_name="users",
            only=True,
            if_table_exists=True,
        ):
            assert True
        case _:
            assert False
