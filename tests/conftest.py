import asyncio
from typing import Awaitable, Callable

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

import asyncpgsa.connection


def pytest_pycollect_makeitem(collector, name, obj):
    """
    Fix pytest collecting for coroutines.
    """
    if collector.funcnamefilter(name) and asyncio.iscoroutinefunction(obj):
        obj = pytest.mark.asyncio(obj)
        return list(collector._genfunctions(name, obj))


@pytest.fixture(scope="function")
def pool(event_loop):
    from asyncpgsa import create_pool

    from . import DB_NAME, HOST, PASS, PORT, USER

    pool = create_pool(
        min_size=1,
        max_size=3,
        host=HOST,
        port=PORT,
        user=USER,
        password=PASS,
        database=DB_NAME,
        timeout=1,
        loop=event_loop,
    )

    event_loop.run_until_complete(pool)

    try:
        yield pool
    finally:
        event_loop.run_until_complete(pool.close())


@pytest.fixture(scope="function")
def connection(pool, event_loop):
    conn = event_loop.run_until_complete(pool.acquire(timeout=2))

    try:
        yield conn
    finally:
        event_loop.run_until_complete(pool.release(conn))


RecreateTableFixture = Callable[[sa.Table], Awaitable[None]]


@pytest.fixture
def recreate_table(
    connection: asyncpgsa.connection.SAConnection,
) -> RecreateTableFixture:
    async def fixture_value(table: sa.Table) -> None:
        await connection.execute(sa.sql.ddl.DropTable(table, if_exists=True))
        await connection.execute(sa.sql.ddl.CreateTable(table))

    return fixture_value


RecreateSequenceFixture = Callable[[sa.Sequence], Awaitable[None]]


@pytest.fixture
def recreate_sequence(
    connection: asyncpgsa.connection.SAConnection,
) -> RecreateSequenceFixture:
    async def fixture_value(sequence: sa.Sequence) -> None:
        await connection.execute(sa.sql.ddl.DropSequence(sequence, if_exists=True))
        await connection.execute(sa.sql.ddl.CreateSequence(sequence))

    return fixture_value


RecreateEnumFixture = Callable[[sa.Enum], Awaitable[None]]


@pytest.fixture
def recreate_enum(
    connection: asyncpgsa.connection.SAConnection,
) -> RecreateEnumFixture:
    async def fixture_value(enum: postgresql.ENUM) -> None:
        # `postgresql.DropEnumType` does not support `IF EXISTS` clause, so we
        # need to homebrew it.
        pg_type = sa.Table(
            "pg_type",
            sa.MetaData(),
            sa.Column("typname", sa.String),
        )

        exists = await connection.fetchval(
            sa.select(sa.exists().where(pg_type.c.typname == enum.name))
        )

        if exists:
            await connection.execute(postgresql.DropEnumType(enum))

        await connection.execute(postgresql.CreateEnumType(enum))

    return fixture_value
