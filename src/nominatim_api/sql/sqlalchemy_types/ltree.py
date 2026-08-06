# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Custom type for the ltree[] categories column.
"""
from typing import Any, Callable, Optional

import sqlalchemy as sa
from sqlalchemy.ext.compiler import compiles

from ...typing import SaColumn


class CategoryArray(sa.types.UserDefinedType):  # type: ignore[type-arg]
    """ Array of ltree categories (``osm.<class>.<type>``). Maps to a native
        ``ltree[]`` column on PostgreSQL and to comma-separated text on other
        dialects (SQLite), where the categories column is not queried.
    """
    cache_ok = True

    def get_col_spec(self, **_: Any) -> str:
        return 'ltree[]'

    def bind_processor(self, dialect: 'sa.Dialect') -> Optional[Callable[[Any], Any]]:
        if dialect.name == 'postgresql':
            return None

        def process(value: Any) -> Optional[str]:
            return ','.join(value) if value is not None else None
        return process

    def result_processor(self, dialect: 'sa.Dialect',
                         coltype: object) -> Optional[Callable[[Any], Any]]:
        if dialect.name == 'postgresql':
            return None

        def process(value: Any) -> Optional[list[str]]:
            return value.split(',') if value else None
        return process

    def bind_expression(self, bindvalue: SaColumn) -> SaColumn:
        return _LtreeArrayCast(bindvalue)


class _LtreeArrayCast(sa.sql.expression.FunctionElement[Any]):
    """ Cast a bound text array to ``ltree[]`` on PostgreSQL. A no-op on
        other dialects, where categories are stored as plain text.
    """
    inherit_cache = True


@compiles(_LtreeArrayCast)
def _default_ltree_array_cast(element: _LtreeArrayCast,
                              compiler: 'sa.Compiled', **kw: Any) -> str:
    return "%s::ltree[]" % compiler.process(element.clauses, **kw)


@compiles(_LtreeArrayCast, 'sqlite')
def _sqlite_ltree_array_cast(element: _LtreeArrayCast,
                             compiler: 'sa.Compiled', **kw: Any) -> str:
    return compiler.process(element.clauses, **kw)


@compiles(CategoryArray, 'sqlite')
def _sqlite_category_col_spec(*args: Any, **kwargs: Any) -> str:
    return 'TEXT'
