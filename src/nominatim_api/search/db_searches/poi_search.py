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
from ...typing import SaBind
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

        # All searches go through the categories column, which is backed by the
        # combined centroid/categories GiST index. Filter and order on the
        # centroid, so that the index can serve both conditions at once.
        sql = base.select_placex(t)\
                  .where(sa.or_(*(base.category_filter(t, *category)
                                  for category in self.qualifiers.values)))\
                  .where(t.c.linked_place_id == None)

        if details.near is not None and details.near_radius is not None:
            sql = sql.add_columns((-t.c.centroid.ST_Distance(NEAR_PARAM))
                                  .label('importance'))\
                     .where(t.c.centroid.within_distance(NEAR_PARAM, NEAR_RADIUS_PARAM))\
                     .order_by(t.c.centroid.ST_Distance(NEAR_PARAM))
        else:
            sql = sql.add_columns(t.c.importance)

        if details.viewbox is not None and details.bounded_viewbox:
            sql = sql.where(t.c.centroid.intersects(VIEWBOX_PARAM))

        if self.countries:
            sql = sql.where(t.c.country_code.in_(self.countries.values))

        if details.excluded:
            sql = sql.where(base.exclude_places(t))

        rows = await conn.execute(sql.limit(LIMIT_PARAM), bind_params)

        results = nres.SearchResults()
        for row in rows:
            result = nres.create_from_placex_row(row, nres.SearchResult)
            result.accuracy = self.penalty + self.qualifiers.get_penalty((row.class_, row.type))
            result.bbox = Bbox.from_wkb(row.bbox)
            results.append(result)

        return results
