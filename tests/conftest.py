import os
import pytest

from headlight.drivers.postgresql import PgDriver


@pytest.fixture(scope="session")
def postgres() -> PgDriver:
    url = os.environ.get("POSTGRES_URL", "postgresql://postgres:postgres@localhost")
    return PgDriver(url)
