from headlight import DbDriver
from headlight.schema.ops import DropIndexOp
from headlight.schema.schema import Index, IndexExpr

index = Index(
    name="perf_idx",
    table_name="users",
    columns=[
        IndexExpr(column="first_name"),
    ],
)


def test_op_forward(postgres: DbDriver) -> None:
    sql = DropIndexOp(name="perf_idx", current_index=index).to_up_sql(postgres)

    assert sql == "DROP INDEX perf_idx"


def test_op_reverse(postgres: DbDriver) -> None:
    sql = DropIndexOp(name="perf_idx", current_index=index).to_down_sql(postgres)

    assert sql == "CREATE INDEX perf_idx ON users (first_name)"
