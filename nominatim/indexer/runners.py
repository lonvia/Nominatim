"""
Mix-ins that provide the actual commands for the indexer for various indexing
tasks.
"""

import psycopg2.extras

class AbstractPlacexRunner:
    """ Base class for runners that work with the placex table.
    """
    FIELDS = "(placex_prepare_update(placex)).*"

    def __init__(self, rank, tokenizer):
        self.tokenizer = tokenizer.get_name_analyzer()
        self.rank = rank

    def close(self):
        if self.tokenizer:
            self.tokenizer.close()
            self.tokenizer = None


    def sql_get_object_info(self, place_ids):
        return """SELECT place_id, (placex_prepare_update(placex)).*
                  FROM placex WHERE place_id IN %s""", (tuple(place_ids), )


    def sql_index_places(self, places):
        values = []
        for place in places:
            values.extend((place[x] for x in ('place_id', 'address')))
            values.append(psycopg2.extras.Json(self.tokenizer.tokenize(place)))

        return """UPDATE placex
                  SET indexed_status = 0, address = v.addr, token_info = v.ti
                  FROM (VALUES {}) as v(id, addr, ti)
                  WHERE place_id = v.id"""\
               .format(','.join(["(%s, %s::hstore, %s::jsonb)"]  * len(places))), values


class RankRunner(AbstractPlacexRunner):
    """ Returns SQL commands for indexing one rank within the placex table.
    """

    def name(self):
        return "rank {}".format(self.rank)

    def sql_count_objects(self):
        return """SELECT count(*) FROM placex
                  WHERE rank_address = {} and indexed_status > 0
               """.format(self.rank)

    def sql_get_objects(self):
        return """SELECT place_id FROM placex
                  WHERE indexed_status > 0 and rank_address = {}
                  ORDER BY geometry_sector""".format(self.rank)


class BoundaryRunner(AbstractPlacexRunner):
    """ Returns SQL commands for indexing the administrative boundaries
        of a certain rank.
    """

    def name(self):
        return "boundaries rank {}".format(self.rank)

    def sql_count_objects(self):
        return """SELECT count(*) FROM placex
                  WHERE indexed_status > 0
                    AND rank_search = {}
                    AND class = 'boundary' and type = 'administrative'
               """.format(self.rank)

    def sql_get_objects(self):
        return """SELECT place_id FROM placex
                  WHERE indexed_status > 0 and rank_search = {}
                        and class = 'boundary' and type = 'administrative'
                  ORDER BY partition, admin_level
               """.format(self.rank)


class InterpolationRunner:
    """ Returns SQL commands for indexing the address interpolation table
        location_property_osmline.
    """

    def __init__(self, tokenizer):
        self.analyzer = tokenizer.get_name_analyzer()

    def close(self):
        if self.analyzer:
            self.analyzer.close()
            self.analyzer = None

    @staticmethod
    def name():
        return "interpolation lines (location_property_osmline)"

    @staticmethod
    def sql_count_objects():
        return """SELECT count(*) FROM location_property_osmline
                  WHERE indexed_status > 0"""

    @staticmethod
    def sql_get_objects():
        return """SELECT place_id FROM location_property_osmline
                  WHERE indexed_status > 0
                  ORDER BY geometry_sector"""


    def sql_get_object_info(self, place_ids):
        return """SELECT place_id, get_interpolation_address(address, osm_id) as address
                  FROM location_property_osmline
                  WHERE place_id IN %s""", (tuple(place_ids), )

    def sql_index_places(self, places):
        values = []
        for place in places:
            values.append(place['place_id'])
            values.append(place['address'])
            values.append(psycopg2.extras.Json(self.analyzer.tokenize(place)))

        return """UPDATE location_property_osmline
                  SET indexed_status = 0, address = v.addr, token_info = v.ti
                  FROM (VALUES {}) as v(id, addr, ti)
                  WHERE place_id = v.id"""\
               .format(','.join(["(%s, %s::hstore, %s::jsonb)"]  * len(places))), values


class PostcodeRunner:
    """ Provides the SQL commands for indexing the location_postcode table.
    """

    @staticmethod
    def close():
        pass

    @staticmethod
    def name():
        return "postcodes (location_postcode)"

    @staticmethod
    def sql_count_objects():
        return 'SELECT count(*) FROM location_postcode WHERE indexed_status > 0'

    @staticmethod
    def sql_get_objects():
        return """SELECT place_id FROM location_postcode
                  WHERE indexed_status > 0
                  ORDER BY country_code, postcode"""

    @staticmethod
    def sql_index_places(ids):
        return """UPDATE location_postcode SET indexed_status = 0
                  WHERE place_id IN %s
               """, (tuple(ids), )
