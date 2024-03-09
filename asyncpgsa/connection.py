from asyncpg import connection
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import asyncpg
from sqlalchemy.sql.ddl import DDLElement

from .log import query_logger


def get_dialect(**kwargs):
    # TODO: Migrate to `paramstyle="numeric_dollar"` after dropping support for
    # SQLAlchemy 1.4.
    dialect = asyncpg.dialect(paramstyle="pyformat", **kwargs)
    return dialect


_dialect = get_dialect()


def _exec_default(default, dialect):
    # Adapted from https://github.com/sqlalchemy/sqlalchemy/blob/rel_2_0_25/lib/sqlalchemy/engine/default.py#L2113-L2126
    # FIXME: Clause elements are not supported.
    # FIXME: Insert sentinels are not supported.
    if default.is_sequence:
        return func.nextval(default.name).compile(dialect=dialect)
    if default.is_callable:
        # XXX: Execution context is not provided since we don't have it.
        return default.arg({})
    else:
        return default.arg


def execute_defaults(compiled, dialect=_dialect):
    # We can't do it in-place because `compiled.params` is a property that
    # returns a new dict every time it's accessed.
    params = compiled.params.copy()

    for column in compiled.insert_prefetch:
        params[column.key] = _exec_default(column.default, dialect)
    for column in compiled.update_prefetch:
        params[column.key] = _exec_default(column.onupdate, dialect)

    return params


def compile_query(query, dialect=_dialect, inline=False):
    if isinstance(query, str):
        query_logger.debug(query)
        return query, ()

    compiled = query.compile(
        dialect=dialect,
        # https://docs.sqlalchemy.org/en/20/faq/sqlexpressions.html#rendering-postcompile-parameters-as-bound-parameters
        compile_kwargs={"render_postcompile": True},
    )

    # TODO: Check via `compiled.is_ddl` after dropping support for SQLAlchemy
    # 1.4.
    if isinstance(query, DDLElement):
        query_logger.debug(compiled.string)
        return compiled.string, ()

    params = execute_defaults(compiled, dialect)  # Default values for Insert/Update.

    # TODO: Remove this after dropping support for SQLAlchemy 1.4 and using
    # `paramstyle="numeric_dollar"`.
    pyformat_to_numeric_dollar = {
        key: f"${idx}" for idx, key in enumerate(params.keys(), start=1)
    }
    new_query = compiled.string % pyformat_to_numeric_dollar

    # Respect custom `TypeAdapters`. XXX: Why `query.compile` doesn't do this?
    # XXX: `_bind_processors` does not resemble public API. Is there a better
    # way?
    # FIXME: `processors[key]` might be tuple of processors.
    processors = compiled._bind_processors
    new_params = [
        processors[key](val) if key in processors else val
        # Python dicts are insertion ordered since 3.7+, so we can rely on the
        # order from `query.compile`. XXX: We can mess up ordering while
        # executing defaults?
        for key, val in params.items()
    ]

    query_logger.debug(new_query)

    if inline:
        return new_query

    # FIXME: Should return `tuple`, not `list`.
    return new_query, new_params


class SAConnection(connection.Connection):
    def __init__(self, *args, dialect=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._dialect = dialect or _dialect

    # FIXME: Update type hints for public methods (asyncpg supports `query: str`
    # only, we support `sa.sql.expression.ClauseElement`.
    def _execute(
        self,
        query,
        args,
        limit,
        timeout,
        return_status=False,
        record_class=None,
        ignore_custom_codec=False,
    ):
        query, compiled_args = compile_query(query, dialect=self._dialect)
        args = compiled_args or args
        return super()._execute(
            query,
            args,
            limit,
            timeout,
            return_status=return_status,
            record_class=record_class,
            ignore_custom_codec=ignore_custom_codec,
        )

    async def execute(self, script, *args, **kwargs) -> str:
        script, params = compile_query(script, dialect=self._dialect)
        args = params or args
        result = await super().execute(script, *args, **kwargs)
        return result

    def cursor(self, query, *args, prefetch=None, timeout=None):
        query, compiled_args = compile_query(query, dialect=self._dialect)
        args = compiled_args or args
        return super().cursor(query, *args, prefetch=prefetch, timeout=timeout)
