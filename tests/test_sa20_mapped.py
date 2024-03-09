import pytest
import sqlalchemy as sa

if sa.__version__ < "2.0.0":
    pytest.skip("These tests is for SQLAlchemy 2.0+ only", allow_module_level=True)

from enum import Enum
from typing import Any, Optional, Type

import asyncpg
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from typing_extensions import Annotated

import asyncpgsa


class WrappedEnum:
    def __init__(self, enum_cls: Type[Enum], name: str) -> None:
        self.enum_cls = enum_cls
        self.name = name

    def map_to_sa_orm(self) -> Mapped[Any]:
        return mapped_column(
            postgresql.ENUM(
                self.enum_cls,
                name=self.name,
                # By default, SQLAlchemy uses `.name` as values for ENUMs, but we
                # usually use `.value` everywhere.
                values_callable=lambda obj: [e.value for e in obj],
            )
        )

    async def register_type_codec(self, conn: asyncpg.Connection) -> None:
        # XXX: Should crash upon override? Does it already?
        await conn.set_type_codec(
            self.name,
            encoder=lambda x: x,
            # TODO: `compiled._bind_processors` does this part of the job for
            # us, why? Did we miss doing another part of the job?
            # encoder=lambda x: x.value,
            decoder=lambda x: self.enum_cls(x),
        )


class GuineaPigStatus(str, Enum):
    NEW_AS_NAME = "NEW_AS_VALUE"
    OLD_AS_NAME = "OLD_AS_VALUE"


GuineaPigStatusWrapped = WrappedEnum(GuineaPigStatus, name="guinea_pig_status")


class Base(DeclarativeBase):
    metadata = sa.MetaData()


bigintpk = Annotated[int, mapped_column(sa.BigInteger(), primary_key=True)]


class GuineaPig(Base):
    __tablename__ = "guinea_pigs"

    id: Mapped[bigintpk]
    status: Mapped[GuineaPigStatus] = GuineaPigStatusWrapped.map_to_sa_orm()
    value: Mapped[Optional[str]]

    @classmethod
    def table(cls) -> sa.Table:
        assert isinstance(cls.__table__, sa.Table)
        return cls.__table__


async def test_crud(connection: asyncpgsa.connection.SAConnection) -> None:
    # Prepare database
    await connection.execute(sa.sql.ddl.DropTable(GuineaPig.table(), if_exists=True))
    await connection.execute(
        sa.text(f"DROP TYPE IF EXISTS {GuineaPigStatusWrapped.name}")
    )

    await connection.execute(
        sa.text(
            f"CREATE TYPE {GuineaPigStatusWrapped.name} "
            f"AS ENUM "
            f"({', '.join(repr(e.value) for e in GuineaPigStatusWrapped.enum_cls)})"
        ),
    )
    await connection.execute(sa.sql.ddl.CreateTable(GuineaPig.table()))

    await GuineaPigStatusWrapped.register_type_codec(connection)

    # Create
    record = await connection.fetchrow(
        sa.insert(GuineaPig)
        .values(
            id=1,
            value="test",
            status=GuineaPigStatus.NEW_AS_NAME,
        )
        .returning(GuineaPig),
    )
    guinea_pig1 = GuineaPig(**record)

    assert guinea_pig1.id == 1
    assert guinea_pig1.value == "test"
    assert guinea_pig1.status is GuineaPigStatus.NEW_AS_NAME  # to check type conversion
    assert guinea_pig1.status == "NEW_AS_VALUE"  # to check `.value` access

    # Read
    [record] = await connection.fetch(sa.select(GuineaPig))
    guinea_pig2 = GuineaPig(**record)

    assert guinea_pig2.id == guinea_pig1.id
    assert guinea_pig2.value == guinea_pig1.value
    assert guinea_pig2.status is guinea_pig1.status  # to check type conversion

    # Update
    record = await connection.fetchrow(
        sa.update(GuineaPig)
        .where(GuineaPig.id == 1)
        .values(
            value="test2",
            status=GuineaPigStatus.OLD_AS_NAME,
        )
        .returning(GuineaPig),
    )
    guinea_pig3 = GuineaPig(**record)

    assert guinea_pig3.id == guinea_pig2.id
    assert guinea_pig3.value == "test2"
    assert guinea_pig3.status is GuineaPigStatus.OLD_AS_NAME  # to check type conversion
    assert guinea_pig3.status == "OLD_AS_VALUE"  # to check `.value` access

    # Upsert
    psql_insert = postgresql.insert(GuineaPig)
    record = await connection.fetchrow(
        psql_insert.values(
            id=1,
            value="test3",
            status=GuineaPigStatus.NEW_AS_NAME,
        )
        .on_conflict_do_update(
            index_elements=[GuineaPig.id],
            set_={
                GuineaPig.value: psql_insert.excluded.value,
                GuineaPig.status: psql_insert.excluded.status,
            },
        )
        .returning(GuineaPig),
    )
    guinea_pig4 = GuineaPig(**record)

    assert guinea_pig4.id == guinea_pig3.id
    assert guinea_pig4.value == "test3"
    assert guinea_pig4.status is GuineaPigStatus.NEW_AS_NAME  # to check type conversion
    assert guinea_pig4.status == "NEW_AS_VALUE"  # to check `.value` access

    # Delete
    assert await connection.fetchval(sa.select(sa.func.count(GuineaPig.id))) == 1
    result = await connection.execute(
        sa.insert(GuineaPig).values(
            id=2,
            value="howdy",
            status=GuineaPigStatus.NEW_AS_NAME,
        ),
    )
    assert result == "INSERT 0 1"
    assert await connection.fetchval(sa.select(sa.func.count(GuineaPig.id))) == 2

    await connection.execute(sa.delete(GuineaPig).where(GuineaPig.id == 1))
    assert await connection.fetchval(sa.select(sa.func.count(GuineaPig.id))) == 1
    assert await connection.fetch(sa.select(GuineaPig.id)) == [(2,)]
