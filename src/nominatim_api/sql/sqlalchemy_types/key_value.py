# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2024 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
A custom type that implements a simple key-value store of strings.
"""
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import HSTORE
from sqlalchemy.dialects.sqlite import JSON as sqlite_json

from ...typing import SaDialect, SaColumn


class KeyValueStore(sa.types.TypeDecorator[Any]):
    """ Dialect-independent type of a simple key-value store of strings.
    """
    impl = HSTORE
    cache_ok = True

    def load_dialect_impl(self, dialect: SaDialect) -> sa.types.TypeEngine[Any]:
        if dialect.name == 'postgresql':
            self.comparator_factory = self.hstore_comparator_factory  # type: ignore
            return dialect.type_descriptor(HSTORE())  # type: ignore[no-untyped-call]

        return dialect.type_descriptor(sqlite_json(none_as_null=True))

    class hstore_comparator_factory(HSTORE.Comparator):

        def merge(self, other: SaColumn) -> 'sa.Operators':
            """ Merge the values from the given KeyValueStore into this
                one, overwriting values where necessary. When the argument
                is null, nothing happens.
            """
            return self.expr.op('||')(sa.func.coalesce(other, sa.cast('', HSTORE)))

    class comparator_factory(sqlite_json.Comparator):  # type: ignore[type-arg]

        def merge(self, other: SaColumn) -> 'sa.Operators':
            """ Merge the values from the given KeyValueStore into this
                one, overwriting values where necessary. When the argument
                is null, nothing happens.
            """
            return sa.func.json_patch(self.expr, sa.func.coalesce(other, '{}'), type_=sqlite_json)
