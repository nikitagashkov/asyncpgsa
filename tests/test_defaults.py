import enum
from datetime import date, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Column, MetaData, Sequence, Table, types
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from asyncpgsa import connection
from tests.compat import strip_bind_casts_if_lt_sa20

metadata = MetaData()


class MyEnum(enum.Enum):
    ITEM_1 = "item_1"
    ITEM_2 = "item_2"


class MyIntEnum(enum.IntEnum):
    ITEM_1 = 1
    ITEM_2 = 2


serial_default = Sequence("serial_seq")
name_default = "default"
t_list_default = ["foo", "bar"]
t_enum_default = MyEnum.ITEM_2.name  # TODO: Was it `.name` before?
t_int_enum_default = MyIntEnum.ITEM_1.name  # TODO: Was it `.name` before?
t_datetime_default = datetime(2017, 1, 1)
t_date_default = date(2017, 1, 1)
t_interval_default = timedelta(seconds=60)
t_boolean_default = True


def t_date_default_func():
    return date(2017, 2, 1)


users = Table(
    "users",
    metadata,
    Column("id", PG_UUID(as_uuid=True), unique=True, default=uuid4),
    Column("serial", types.Integer, serial_default),
    Column("name", types.String(60), nullable=False, default=name_default),
    Column(
        "t_list",
        types.ARRAY(types.String(60)),
        nullable=False,
        default=t_list_default,
    ),
    Column("t_enum", types.Enum(MyEnum), nullable=False, default=t_enum_default),
    Column(
        "t_int_enum",
        types.Enum(MyIntEnum),
        nullable=False,
        default=t_int_enum_default,
    ),
    Column("t_datetime", types.DateTime(), nullable=False, default=t_datetime_default),
    Column("t_date", types.DateTime(), nullable=False, default=t_date_default),
    Column("t_date_2", types.DateTime(), nullable=False, default=t_date_default_func),
    Column("t_interval", types.Interval(), nullable=False, default=t_interval_default),
    Column("t_boolean", types.Boolean(), nullable=False, default=True),
    Column("version", PG_UUID(as_uuid=True), default=uuid4, onupdate=uuid4),
)


class AnyUUID:
    def __eq__(self, other):
        return isinstance(other, UUID)


# FIXME: Use smaller per-test tables for easier testing.


def test_insert_query_defaults():
    # FIXME: If `.values()` are omitted, the defaults are not applied.
    query = users.insert().values()
    new_query, new_params = connection.compile_query(query)

    assert (
        new_query
        == strip_bind_casts_if_lt_sa20(
            "INSERT INTO users ("
            "id, "
            "serial, "
            "name, "
            "t_list, "
            "t_enum, "
            "t_int_enum, "
            "t_datetime, "
            "t_date, "
            "t_date_2, "
            "t_interval, "
            "t_boolean, "
            "version) "
            "VALUES ("
            "$1::UUID, "
            "nextval('serial_seq'), "  # XXX: Why is it inlined?
            "$2::VARCHAR, "
            "$3::VARCHAR(60)[], "
            "$4::myenum, "
            "$5::myintenum, "
            "$6::TIMESTAMP WITHOUT TIME ZONE, "
            "$7::TIMESTAMP WITHOUT TIME ZONE, "
            "$8::TIMESTAMP WITHOUT TIME ZONE, "
            "$9::INTERVAL, "
            "$10::BOOLEAN, "
            "$11::UUID)"
        )
    )
    assert new_params == [
        AnyUUID(),  # FIXME: Need predetermined UUIDs.
        name_default,
        t_list_default,
        t_enum_default,
        t_int_enum_default,
        t_datetime_default,
        t_date_default,
        t_date_default_func(),
        t_interval_default,
        t_boolean_default,
        AnyUUID(),  # FIXME: Need predetermined UUIDs.
    ]


def test_insert_query_defaults_override():
    query = users.insert()
    query = query.values(
        name="username",
        serial=4444,
        t_list=["l1", "l2"],
        t_enum=MyEnum.ITEM_1,
        t_int_enum=MyIntEnum.ITEM_2,
        t_datetime=datetime(2020, 1, 1),
        t_date=date(2020, 1, 1),
        t_date_2=date(2020, 1, 1),
        t_interval=timedelta(seconds=120),
        t_boolean=False,
    )
    new_query, new_params = connection.compile_query(query)

    assert new_query == strip_bind_casts_if_lt_sa20(
        "INSERT INTO users ("
        "id, "
        "serial, "
        "name, "
        "t_list, "
        "t_enum, "
        "t_int_enum, "
        "t_datetime, "
        "t_date, "
        "t_date_2, "
        "t_interval, "
        "t_boolean, "
        "version) "
        "VALUES ("
        "$1::UUID, "
        "$2::INTEGER, "
        "$3::VARCHAR, "
        "$4::VARCHAR(60)[], "
        "$5::myenum, "
        "$6::myintenum, "
        "$7::TIMESTAMP WITHOUT TIME ZONE, "
        "$8::TIMESTAMP WITHOUT TIME ZONE, "
        "$9::TIMESTAMP WITHOUT TIME ZONE, "
        "$10::INTERVAL, "
        "$11::BOOLEAN, "
        "$12::UUID)"
    )
    assert new_params == [
        AnyUUID(),  # FIXME: Need predetermined UUIDs.
        4444,
        "username",
        ["l1", "l2"],
        MyEnum.ITEM_1.name,  # FIXME: Why is it `.name`?
        MyIntEnum.ITEM_2.name,  # FIXME: Why is it `.name`?
        datetime(2020, 1, 1),
        date(2020, 1, 1),
        date(2020, 1, 1),
        timedelta(seconds=120),
        False,  # FIXME: Not strict enough because of `__bool__`.
        AnyUUID(),  # FIXME: Need predetermined UUIDs.
    ]


@pytest.mark.skip("Bug? `onupdate` should be fired, not `default`")
def test_update_query_defaults():
    # FIXME: If `.values()` are omitted, the defaults are not applied.
    query = users.update().values()
    new_query, new_params = connection.compile_query(query)
    assert query.parameters.get("version")
    assert query.parameters.get("serial") == 4444
    assert query.parameters.get("name") == "username"
    assert query.parameters.get("t_list") == ["l1", "l2"]
    assert query.parameters.get("t_enum") == MyEnum.ITEM_1
    assert query.parameters.get("t_int_enum") == MyIntEnum.ITEM_2
    assert query.parameters.get("t_datetime") == datetime(2020, 1, 1)
    assert query.parameters.get("t_date") == date(2020, 1, 1)
    assert query.parameters.get("t_date_2") == date(2020, 1, 1)
    assert query.parameters.get("t_interval") == timedelta(seconds=120)
    assert query.parameters.get("t_boolean") is False
    assert isinstance(query.parameters.get("version"), UUID)


def test_update_query():
    query = users.update().where(users.c.name == "default")
    query = query.values(
        name="newname",
        serial=5555,
        t_list=["l3", "l4"],
        t_enum=MyEnum.ITEM_1,
        t_int_enum=MyIntEnum.ITEM_2,
        t_datetime=datetime(2030, 1, 1),
        t_date=date(2030, 1, 1),
        t_date_2=date(2030, 1, 1),
        t_interval=timedelta(seconds=180),
        t_boolean=False,
    )
    new_query, new_params = connection.compile_query(query)

    assert new_query == strip_bind_casts_if_lt_sa20(
        "UPDATE users SET "
        "serial=$1::INTEGER, "
        "name=$2::VARCHAR, "
        "t_list=$3::VARCHAR(60)[], "
        "t_enum=$4::myenum, "
        "t_int_enum=$5::myintenum, "
        "t_datetime=$6::TIMESTAMP WITHOUT TIME ZONE, "
        "t_date=$7::TIMESTAMP WITHOUT TIME ZONE, "
        "t_date_2=$8::TIMESTAMP WITHOUT TIME ZONE, "
        "t_interval=$9::INTERVAL, "
        "t_boolean=$10::BOOLEAN, "
        "version=$11::UUID "
        "WHERE users.name = $12::VARCHAR"
    )
    assert new_params == [
        5555,
        "newname",
        ["l3", "l4"],
        MyEnum.ITEM_1.name,  # FIXME: Why is it `.name`?
        MyIntEnum.ITEM_2.name,  # FIXME: Why is it `.name`?
        datetime(2030, 1, 1),
        date(2030, 1, 1),
        date(2030, 1, 1),
        timedelta(seconds=180),
        False,  # FIXME: Not strict enough because of `__bool__`.
        AnyUUID(),  # FIXME: Need predetermined UUIDs.
        "default",
    ]
