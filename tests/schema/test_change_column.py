from headlight.schema import ops, types
from headlight.schema.builder import ChangeColumn


def test_set_null() -> None:
    builder = ChangeColumn(ops=[], table_name="users", column_name="id", only=True, if_table_exists=True)
    builder.set_nullable(True)

    match builder._ops[0]:
        case ops.DropNotNullOp(table_name="users", column_name="id", only=True, if_table_exists=True):
            assert True
        case _:
            assert False


def test_set_not_null() -> None:
    builder = ChangeColumn(ops=[], table_name="users", column_name="id", only=True, if_table_exists=True)
    builder.set_nullable(False)

    match builder._ops[0]:
        case ops.SetNotNullOp(table_name="users", column_name="id", only=True, if_table_exists=True):
            assert True
        case _:
            assert False


def test_set_default() -> None:
    builder = ChangeColumn(ops=[], table_name="users", column_name="id", only=True, if_table_exists=True)
    builder.set_default(new_default="new", current_default="old")

    match builder._ops[0]:
        case ops.SetDefaultOp(
            new_default="new", old_default="old", table_name="users", column_name="id", only=True, if_table_exists=True
        ):
            assert True
        case _:
            assert False


def test_drop_default() -> None:
    builder = ChangeColumn(ops=[], table_name="users", column_name="id", only=True, if_table_exists=True)
    builder.drop_default(current_default="old")

    match builder._ops[0]:
        case ops.DropDefaultOp(
            old_default="old", table_name="users", column_name="id", only=True, if_table_exists=True
        ):
            assert True
        case _:
            assert False


def test_change_type() -> None:
    builder = ChangeColumn(ops=[], table_name="users", column_name="id", only=True, if_table_exists=True)
    builder.change_type(
        new_type=types.VarCharType(),
        current_type=types.BigIntegerType(),
        collation="german",
        current_collation="bel",
        using="using",
        current_using="current",
    )

    match builder._ops[0]:
        case ops.ChangeTypeOp(
            new_type=types.VarCharType(),
            old_type=types.BigIntegerType(),
            collation="german",
            old_collation="bel",
            using="using",
            old_using="current",
            table_name="users",
            column_name="id",
            only=True,
            if_table_exists=True,
        ):
            assert True
        case _:
            assert False
