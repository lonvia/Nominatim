# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Custom mocks for testing.
"""
import itertools

from nominatim_db.db import properties


class MockPlacexTable:
    """ A placex table for testing.
    """
    def __init__(self, conn, sql_preprocessor):
        self.idseq = itertools.count(10000)
        self.conn = conn
        sql_preprocessor.run_sql_file(conn, 'tables/placex.sql')
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_place START 1")
        conn.commit()

    def add(self, place_id=None, osm_type='N', osm_id=None, cls='amenity', typ='cafe', names=None,
            admin_level=None, address=None, extratags=None, geom='POINT(10 4)',
            country=None, housenumber=None, rank_search=30, centroid='POINT(10 4)',
            indexed_status=0, indexed_date=None):
        with self.conn.cursor() as cur:
            cur.execute("""INSERT INTO placex (place_id, osm_type, osm_id, class,
                                               type, name, admin_level, address,
                                               housenumber, rank_search, rank_address,
                                               extratags, centroid, geometry, country_code,
                                               geometry_sector, partition,
                                               indexed_status, indexed_date)
                            VALUES(coalesce(%s, nextval('seq_place')),
                                   %s, %s, %s, %s, %s, %s,
                                   %s, %s, %s, %s, %s, %s, %s, %s, 1, 0, %s, %s)
                            RETURNING place_id""",
                        (place_id, osm_type, osm_id or next(self.idseq), cls, typ, names,
                         admin_level, address, housenumber, rank_search,
                         rank_search, extratags, centroid, 'SRID=4326;' + geom,
                         country, indexed_status, indexed_date))
            place_id = cur.fetchone()[0]
        self.conn.commit()
        return place_id


class MockPropertyTable:
    """ A property table for testing.
    """
    def __init__(self, conn):
        self.conn = conn

    def set(self, name, value):
        """ Set a property in the table to the given value.
        """
        properties.set_property(self.conn, name, value)

    def get(self, name):
        """ Set a property in the table to the given value.
        """
        return properties.get_property(self.conn, name)
