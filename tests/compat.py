import re

import sqlalchemy as sa


def strip_bind_casts_if_lt_sa20(query: str) -> str:
    # SQLAlchemy 2.0 aggressively type casts bind parameters while 1.4 does not.
    # TODO: Drop this function when we drop support for SQLAlchemy 1.4.
    if sa.__version__ > "2.0.0":
        return query

    # FIXME: Disgusting hack
    query = query.replace("TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ")
    query = query.replace("TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP")
    query = query.replace("::VARCHAR(60)[]", "")
    query = re.sub(r"::[a-zA-Z_]+", "", query)

    return query
