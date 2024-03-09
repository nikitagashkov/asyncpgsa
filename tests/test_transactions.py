import pytest
import sqlalchemy as sa

import asyncpgsa
from tests.conftest import RecreateTableFixture


async def test_rollbacks_upon_exception(
    connection: asyncpgsa.connection.SAConnection,
    recreate_table: RecreateTableFixture,
) -> None:
    # Given
    table = sa.Table(
        "guinea_pigs",
        sa.MetaData(),
        sa.Column("value", sa.String),
    )

    await recreate_table(table)
    await connection.execute(table.insert().values(value="should exist"))

    with pytest.raises(ValueError, match="Oops"):
        async with connection.transaction():
            await connection.execute(table.insert().values(value="should not exist"))
            raise ValueError("Oops")

    assert [dict(r) for r in await connection.fetch("SELECT * FROM guinea_pigs")] == [
        {"value": "should exist"}
    ]
