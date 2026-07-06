# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2025 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Implementation of category search.
"""
import sqlalchemy as sa

from . import base
from ..db_search_fields import SearchData
from ... import results as nres
from ...typing import SaBind, SaRow, SaSelect
from ...sql.sqlalchemy_types import Geometry
from ...connection import SearchConnection
from ...types import SearchDetails, Bbox


LIMIT_PARAM: SaBind = sa.bindparam('limit')
VIEWBOX_PARAM: SaBind = sa.bindparam('viewbox', type_=Geometry)
NEAR_PARAM: SaBind = sa.bindparam('near', type_=Geometry)
NEAR_RADIUS_PARAM: SaBind = sa.bindparam('near_radius')


class PoiSearch(base.AbstractSearch):
    """ Category search in a geographic area.
    """
    def __init__(self, sdata: SearchData) -> None:
        super().__init__(sdata.penalty)
        self.qualifiers = sdata.qualifiers
        self.countries = sdata.countries

    async def lookup(self, conn: SearchConnection,
                     details: SearchDetails) -> nres.SearchResults:
        """ Find results for the search in the database.
        """
        bind_params = {
            'limit': details.max_results,
            'viewbox': details.viewbox,
            'near': details.near,
            'near_radius': details.near_radius,
            'excluded': details.excluded_place_ids
        }

        t = conn.t.placex

        # Rows tagged with the category they were matched on. A placex row may
        # carry several categories, so the matched one is not necessarily its
        # class/type and must be tracked explicitly.
        rows: list[tuple[SaRow, tuple[str, str]]] = []

        if details.near and details.near_radius is not None and details.near_radius < 0.2:
            # simply search in placex table
            def _base_query() -> SaSelect:
                return base.select_placex(t) \
                           .add_columns((-t.c.centroid.ST_Distance(NEAR_PARAM))
                                        .label('importance'))\
                           .where(t.c.linked_place_id == None) \
                           .where(t.c.geometry.within_distance(NEAR_PARAM, NEAR_RADIUS_PARAM)) \
                           .order_by(t.c.centroid.ST_Distance(NEAR_PARAM)) \
                           .limit(LIMIT_PARAM)

            for category in self.qualifiers.values:
                sql = _base_query().where(base.category_filter(t, *category))

                if self.countries:
                    sql = sql.where(t.c.country_code.in_(self.countries.values))

                if details.viewbox is not None and details.bounded_viewbox:
                    sql = sql.where(t.c.geometry.intersects(VIEWBOX_PARAM))

                if details.excluded:
                    sql = sql.where(base.exclude_places(t))

                rows.extend((r, category) for r in await conn.execute(sql, bind_params))
        else:
            # use the categories column, backed by the ltree GiST index
            for category in self.qualifiers.values:
                sql = base.select_placex(t)\
                           .add_columns(t.c.importance)\
                           .where(base.category_filter(t, *category))

                if details.viewbox is not None and details.bounded_viewbox:
                    sql = sql.where(t.c.geometry.intersects(VIEWBOX_PARAM))

                if details.near and details.near_radius is not None:
                    sql = sql.order_by(t.c.centroid.ST_Distance(NEAR_PARAM))\
                             .where(t.c.geometry.within_distance(NEAR_PARAM,
                                                                 NEAR_RADIUS_PARAM))

                if self.countries:
                    sql = sql.where(t.c.country_code.in_(self.countries.values))

                if details.excluded:
                    sql = sql.where(base.exclude_places(t))

                sql = sql.limit(LIMIT_PARAM)
                rows.extend((r, category) for r in await conn.execute(sql, bind_params))

        results = nres.SearchResults()
        for row, category in rows:
            result = nres.create_from_placex_row(row, nres.SearchResult)
            result.category = category
            result.accuracy = self.penalty + self.qualifiers.get_penalty(category)
            result.bbox = Bbox.from_wkb(row.bbox)
            results.append(result)

        return results
