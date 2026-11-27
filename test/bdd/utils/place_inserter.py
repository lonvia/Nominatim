# SPDX-License-Identifier: GPL-2.0-only
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Helper classes for filling the place table.
"""
import ast
import random
import string

import psycopg.sql as pysql

from .geometry_alias import ALIASES
from .grid import Grid


class PlaceColumn:
    """ Helper class to collect contents from a BDD table row and
        insert it into the place table.
    """
    def __init__(self, grid=None, has_address_field=True):
        self.columns = {}
        self.grid = grid or Grid()
        self.has_address_field = has_address_field

    def add_row(self, headings, row, with_geometry=True):
        """ Parse the content from the given behave row as place column data.
        """
        for name, value in zip(headings, row):
            self.add(name, value)

        if with_geometry and 'geometry' not in self.columns:
            assert self.columns['osm_type'] == 'N', 'Geometry missing'

            pt = self.grid.get(str(self.columns['osm_id']))
            if pt is None:
                pt = (random.uniform(-180, 180), random.uniform(-90, 90))

            self.columns['geometry'] = pysql.SQL("ST_SetSRID(ST_Point({}, {}), 4326)".format(*pt))

        return self

    def add(self, key, value):
        if hasattr(self, '_set_key_' + key):
            getattr(self, '_set_key_' + key)(value)
        elif key.startswith('name+'):
            self._add_hstore('name', key[5:], value)
        elif key.startswith('extra+'):
            self._add_hstore('extratags', key[6:], value)
        elif key.startswith('addr+'):
            self._add_hstore('address', key[5:], value)
        elif key in ('name', 'address', 'extratags'):
            self.columns[key] = ast.literal_eval('{' + value + '}')
        elif key == 'categories':
            self.columns[key] = list(map(str.strip, value.split(','))) if value else []
        else:
            self.columns[key] = None if value == '' else value

        return self

    def _set_key_name(self, value):
        self._add_hstore('name', 'name', value)

    def _set_key_osm(self, value):
        assert value[0] in 'NRW' and value[1:].isdigit(), \
               "OSM id needs to be of format <NRW><id>."

        self.columns['osm_type'] = value[0]
        self.columns['osm_id'] = int(value[1:])

    def _set_key_admin(self, value):
        self.columns['admin_level'] = int(value)

    def _set_key_nodes(self, value):
        self.columns['nodes'] = [int(x) for x in value.split(',')]

    def _set_key_housenr(self, value):
        if value:
            self._add_hstore('address', 'housenumber', value)

    def _set_key_postcode(self, value):
        if value:
            if self.has_address_field:
                self._add_hstore('address', 'postcode', value)
            else:
                self.columns['postcode'] = value

    def _set_key_street(self, value):
        if value:
            self._add_hstore('address', 'street', value)

    def _set_key_addr_place(self, value):
        if value:
            self._add_hstore('address', 'place', value)

    def _set_key_country(self, value):
        if value:
            self._add_hstore('address', 'country', value)

    def _add_hstore(self, column, key, value):
        if column in self.columns:
            self.columns[column][key] = value
        else:
            self.columns[column] = {key: value}

    def _set_key_geometry(self, value):
        self.columns['geometry'] = pysql.SQL(self._value_to_wkt(value))

    def _set_key_centroid(self, value):
        self.columns['centroid'] = pysql.SQL(self._value_to_wkt(value))

    def _value_to_wkt(self, value):
        if value.startswith('country:'):
            ccode = value[8:].upper()
            return "ST_SetSRID(ST_Point({}, {}), 4326)".format(*ALIASES[ccode])

        wkt = self.grid.geometry_to_wkt(value)
        return f"'srid=4326;{wkt}'::geometry"

    def assert_osm_type(self, otype=None):
        if otype is None:
            assert 'osm_type' in self.columns, "'osm' column is required"
        else:
            assert self.columns.pop('osm_type') == otype, f"Expect osm type '{otype}'."

        return self

    def with_categories(self):
        if 'categories' not in self.columns:
            cls = self.columns.get('class')
            typ = self.columns.get('type')
            if cls and typ:
                self.columns['categories'] = [f"osm.{cls}.{typ}"]
        return self

    def with_name(self):
        if 'name' not in self.columns:
            self.columns['name'] = {
                'name':
                ''.join(random.choices(string.ascii_uppercase)
                        + random.choices(string.printable, k=random.randrange(30))),
            }
        return self

    def db_delete(self, cursor):
        """ Issue a delete for the given OSM object.
        """
        cursor.execute('DELETE FROM place WHERE osm_type = %s and osm_id = %s',
                       (self.columns['osm_type'], self.columns['osm_id']))
