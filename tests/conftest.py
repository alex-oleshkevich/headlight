import os
import pytest

from headlight.drivers.postgresql import PgDriver


@pytest.fixture(scope="session")
def postgres() -> PgDriver:
    url = os.environ.get("POSTGRES_URL", "postgresql://postgres:postgres@localhost")
    return PgDriver(url)


@pytest.fixture(autouse=True, scope="session")
def setup_dbs(postgres) -> None:
    # postgres.execute("rollback")  # exit current transaction
    # postgres.execute("create database headlight_test")
    # yield
    # postgres.execute("drop database headlight_test")
    pass
