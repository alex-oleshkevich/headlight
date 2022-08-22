from headlight import DbDriver
from headlight.schema.ops import RunSQLOp


def test_op_forward(postgres: DbDriver) -> None:
    sql = RunSQLOp(up_sql="select 1", down_sql="select 2").to_up_sql(postgres)
    assert sql == "select 1"


def test_op_reverse(postgres: DbDriver) -> None:
    sql = RunSQLOp(up_sql="select 1", down_sql="select 2").to_down_sql(postgres)
    assert sql == "select 2"
