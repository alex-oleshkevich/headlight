from headlight import DbDriver
from headlight.schema.ops import CreateIndexOp
from headlight.schema.schema import Index, IndexExpr

index = Index(
    unique=True,
    using="gist",
    name="perf_idx",
    table_name="users",
    tablespace="indexspace",
    include=["email", "id"],
    where="email is not null",
    with_="deduplicate_items = off",
    columns=[
        IndexExpr(column="first_name"),
        IndexExpr(
            column="last_name",
            collation="german",
            opclass="gist__intbig_ops",
            opclass_params="siglen = 32",
            sorting="DESC",
            nulls="LAST",
        ),
    ],
)


def test_op_forward(postgres: DbDriver) -> None:
    sql = CreateIndexOp(index=index, concurrently=True, if_not_exists=True, only=True).to_up_sql(postgres)

    assert sql == (
        "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS perf_idx "
        "ON ONLY users USING gist ("
        'first_name, last_name COLLATE "german" gist__intbig_ops(siglen = 32) DESC NULLS LAST) '
        "INCLUDE (email, id) "
        "WITH (deduplicate_items = off) "
        "TABLESPACE indexspace "
        "WHERE email is not null"
    )


def test_op_reverse(postgres: DbDriver) -> None:
    sql = CreateIndexOp(index=index, concurrently=True, if_not_exists=True, only=True).to_down_sql(postgres)

    assert sql == "DROP INDEX perf_idx"
