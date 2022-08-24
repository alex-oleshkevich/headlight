from headlight.schema import types
from headlight.schema.builder import CreateTableBuilder
from headlight.schema.schema import (
    CheckConstraint,
    Column,
    ForeignKey,
    GeneratedAs,
    Index,
    PrimaryKeyConstraint,
    UniqueConstraint,
)


def test_add_column() -> None:
    builder = CreateTableBuilder(table_name="users")
    builder.add_column(
        name="id",
        type=types.BigIntegerType(),
        null=True,
        default="1",
        primary_key=True,
        unique=True,
        checks=["id > 0"],
        generated_as="1",
    )
    match builder._table.columns[0]:
        case Column(
            name="id",
            type=types.BigIntegerType(),
            null=True,
            default="1",
            primary_key=True,
            unique_constraint=UniqueConstraint(),
            check_constraints=[CheckConstraint(expr="id > 0")],
            generated_as_=GeneratedAs(expr="1"),
        ):
            assert True
        case _:
            assert False


def test_autoincrements() -> None:
    builder = CreateTableBuilder(table_name="users")
    builder.autoincrements()
    match builder._table.columns:
        case [Column(type=types.BigIntegerType(auto_increment=True), primary_key=True, name="id")]:
            assert True
        case _:
            assert False


def test_add_timestamps() -> None:
    builder = CreateTableBuilder(table_name="users")
    builder.add_timestamps()
    match builder._table.columns:
        case [
            Column(type=types.DateTimeType(tz=True), name="created_at", null=False, default="current_timestamp()"),
            Column(type=types.DateTimeType(tz=True), name="updated_at", null=True),
        ]:
            assert True
        case _:
            assert False


def test_add_created_timestamp() -> None:
    builder = CreateTableBuilder(table_name="users")
    builder.add_created_timestamp()
    match builder._table.columns:
        case [
            Column(type=types.DateTimeType(tz=True), name="created_at", null=False, default="current_timestamp()"),
        ]:
            assert True
        case _:
            assert False


def test_add_index() -> None:
    builder = CreateTableBuilder(table_name="users")
    builder.add_index(["id"], name="id_idx")
    match builder._table.indices[0]:
        case Index(table_name="users", name="id_idx"):
            assert True
        case _:
            assert False


def test_add_index_generates_name() -> None:
    builder = CreateTableBuilder(table_name="users")
    builder.add_index(["id"])
    match builder._table.indices[0]:
        case Index(table_name="users", name="users_id_idx"):
            assert True
        case _:
            assert False


def test_add_check_constraint() -> None:
    builder = CreateTableBuilder(table_name="users")
    builder.add_check_constraint("id is not null", "not_null")
    match builder._table.constraints[0]:
        case CheckConstraint(expr="id is not null", name="not_null"):
            assert True
        case _:
            assert False


def test_add_check_constraint_generates_name() -> None:
    builder = CreateTableBuilder(table_name="users")
    builder.add_check_constraint("length(id) > 0")
    match builder._table.constraints[0]:
        case CheckConstraint(expr="length(id) > 0", name="users_lengthid0_check"):
            assert True
        case _:
            assert False


def test_add_unique_constraint() -> None:
    builder = CreateTableBuilder(table_name="users")
    builder.add_unique_constraint(["id"])
    match builder._table.constraints[0]:
        case UniqueConstraint(columns=["id"]):
            assert True
        case _:
            assert False


def test_add_unique_constraint_generates_name() -> None:
    builder = CreateTableBuilder(table_name="users")
    builder.add_unique_constraint(["id"])
    match builder._table.constraints[0]:
        case UniqueConstraint(columns=["id"], name="users_id_uniq"):
            assert True
        case _:
            assert False


def test_add_primary_key() -> None:
    builder = CreateTableBuilder(table_name="users")
    builder.add_primary_key(["id"])
    match builder._table.constraints[0]:
        case PrimaryKeyConstraint(columns=["id"]):
            assert True
        case _:
            assert False


def test_add_primary_key_generates_name() -> None:
    builder = CreateTableBuilder(table_name="users")
    builder.add_primary_key(["id"])
    match builder._table.constraints[0]:
        case PrimaryKeyConstraint(columns=["id"], name="users_id_pk"):
            assert True
        case _:
            assert False


def test_add_foreign_key() -> None:
    builder = CreateTableBuilder(table_name="users")
    builder.add_foreign_key(["id"], "profiles")
    match builder._table.constraints[0]:
        case ForeignKey(target_table="profiles", self_columns=["id"]):
            assert True
        case _:
            assert False


def test_add_foreign_key_generates_name() -> None:
    builder = CreateTableBuilder(table_name="users")
    builder.add_foreign_key(["id"], "profiles")
    match builder._table.constraints[0]:
        case ForeignKey(target_table="profiles", self_columns=["id"], name="users_id_to_profiles_fk"):
            assert True
        case _:
            assert False
