"""
Main work horse for indexing (computing addresses) the database.
"""
# pylint: disable=C0111
import logging
import select
import time

import psycopg2
import psycopg2.extras

from .progress import ProgressLogger
from ..db.async_connection import DBConnection
from ..tokenizer import tokenizer

LOG = logging.getLogger()

class AbstractPlacexRunner:
    """ Base class for runners that work with the placex table.
    """
    FIELDS = 'place_id, name'

    def preprocess_places(self, cursor, places):
        terms = tokenizer.get_search_terms(cursor, {p[0] : p[1] for p in places if p[1] is not None})
        for place in places:
            if place[1] is not None:
                place[1]['_'] = "{%s}" % (','.join([str(x) for x in terms.get(place[0], [])]))
        return cursor.mogrify(','.join(["(%s, %s::hstore)"]  * len(places)),
                           [val for sublist in places for val in sublist]).decode('utf-8')

    @staticmethod
    def sql_index_place(places):
        return """UPDATE placex SET
                     indexed_status = 0,
                     name = v.name
                  FROM (VALUES {}) as v(id, name)
                  WHERE place_id = v.id"""\
               .format(places)


class RankRunner(AbstractPlacexRunner):
    """ Returns SQL commands for indexing one rank within the placex table.
    """

    def __init__(self, rank):
        self.rank = rank

    def name(self):
        return "rank {}".format(self.rank)

    def sql_count_objects(self):
        return """SELECT count(*) FROM placex
                  WHERE rank_address = {} and indexed_status > 0
               """.format(self.rank)

    def sql_get_objects(self):
        return """SELECT {} FROM placex
                  WHERE indexed_status > 0 and rank_address = {}
                  ORDER BY geometry_sector""".format(self.FIELDS, self.rank)


class BoundaryRunner(AbstractPlacexRunner):
    """ Returns SQL commands for indexing the administrative boundaries
        of a certain rank.
    """

    def __init__(self, rank):
        self.rank = rank

    def name(self):
        return "boundaries rank {}".format(self.rank)

    def sql_count_objects(self):
        return """SELECT count(*) FROM placex
                  WHERE indexed_status > 0
                    AND rank_search = {}
                    AND class = 'boundary' and type = 'administrative'
               """.format(self.rank)

    def sql_get_objects(self):
        return """SELECT {} FROM placex
                  WHERE indexed_status > 0 and rank_search = {}
                        and class = 'boundary' and type = 'administrative'
                  ORDER BY partition, admin_level
               """.format(self.FIELDS, self.rank)


class InterpolationRunner:
    """ Returns SQL commands for indexing the address interpolation table
        location_property_osmline.
    """

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

    def preprocess_places(self, cursor, places):
        return [r[0] for r in places]

    @staticmethod
    def sql_index_place(ids):
        return """UPDATE location_property_osmline
                  SET indexed_status = 0 WHERE place_id IN ({})
               """.format(','.join((str(i) for i in ids)))

class PostcodeRunner:
    """ Provides the SQL commands for indexing the location_postcode table.
    """

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

    def preprocess_places(self, cursor, places):
        return [r[0] for r in places]

    @staticmethod
    def sql_index_place(ids):
        return """UPDATE location_postcode SET indexed_status = 0
                  WHERE place_id IN ({})
               """.format(','.join((str(i) for i in ids)))


def _analyse_db_if(conn, condition):
    if condition:
        with conn.cursor() as cur:
            cur.execute('ANALYSE')


class Indexer:
    """ Main indexing routine.
    """

    def __init__(self, dsn, num_threads):
        self.dsn = dsn
        self.num_threads = num_threads
        self.conn = None
        self.threads = []


    def _setup_connections(self):
        self.conn = psycopg2.connect(self.dsn)
        self.threads = [DBConnection(self.dsn) for _ in range(self.num_threads)]


    def _close_connections(self):
        if self.conn:
            self.conn.close()
            self.conn = None

        for thread in self.threads:
            thread.close()
        self.threads = []


    def index_full(self, analyse=True):
        """ Index the complete database. This will first index boudnaries
            followed by all other objects. When `analyse` is True, then the
            database will be analysed at the appropriate places to
            ensure that database statistics are updated.
        """
        conn = psycopg2.connect(self.dsn)
        conn.autocommit = True

        try:
            self.index_by_rank(0, 4)
            _analyse_db_if(conn, analyse)

            self.index_boundaries(0, 30)
            _analyse_db_if(conn, analyse)

            self.index_by_rank(5, 25)
            _analyse_db_if(conn, analyse)

            self.index_by_rank(26, 30)
            _analyse_db_if(conn, analyse)

            self.index_postcodes()
            _analyse_db_if(conn, analyse)
        finally:
            conn.close()


    def index_boundaries(self, minrank, maxrank):
        """ Index only administrative boundaries within the given rank range.
        """
        LOG.warning("Starting indexing boundaries using %s threads",
                    self.num_threads)

        self._setup_connections()

        try:
            for rank in range(max(minrank, 4), min(maxrank, 26)):
                self.index(BoundaryRunner(rank))
        finally:
            self._close_connections()

    def index_by_rank(self, minrank, maxrank):
        """ Index all entries of placex in the given rank range (inclusive)
            in order of their address rank.

            When rank 30 is requested then also interpolations and
            places with address rank 0 will be indexed.
        """
        maxrank = min(maxrank, 30)
        LOG.warning("Starting indexing rank (%i to %i) using %i threads",
                    minrank, maxrank, self.num_threads)

        self._setup_connections()

        try:
            for rank in range(max(1, minrank), maxrank):
                self.index(RankRunner(rank))

            if maxrank == 30:
                self.index(RankRunner(0))
                self.index(InterpolationRunner(), 20)
                self.index(RankRunner(30), 20)
            else:
                self.index(RankRunner(maxrank))
        finally:
            self._close_connections()


    def index_postcodes(self):
        """Index the entries ofthe location_postcode table.
        """
        LOG.warning("Starting indexing postcodes using %s threads", self.num_threads)

        self._setup_connections()

        try:
            self.index(PostcodeRunner(), 20)
        finally:
            self._close_connections()

    def update_status_table(self):
        """ Update the status in the status table to 'indexed'.
        """
        conn = psycopg2.connect(self.dsn)

        try:
            with conn.cursor() as cur:
                cur.execute('UPDATE import_status SET indexed = true')

            conn.commit()
        finally:
            conn.close()

    def index(self, obj, batch=1):
        """ Index a single rank or table. `obj` describes the SQL to use
            for indexing. `batch` describes the number of objects that
            should be processed with a single SQL statement
        """
        LOG.warning("Starting %s (using batch size %s)", obj.name(), batch)

        with self.conn.cursor() as cur:
            cur.execute(obj.sql_count_objects())

            total_tuples = cur.fetchone()[0]
            LOG.debug("Total number of rows: %i", total_tuples)

        self.conn.commit()

        progress = ProgressLogger(obj.name(), total_tuples)
        timing_find_thread = 0

        if total_tuples > 0:
            # Connection for queries during preprocessing. Use for SELECT only.
            ro_conn = psycopg2.connect(self.dsn)
            ro_conn.autocommit = True
            ro_cur = ro_conn.cursor()
            psycopg2.extras.register_hstore(ro_cur, globally=True)

            try:
                with self.conn.cursor(name='places') as cur:
                    cur.execute(obj.sql_get_objects())

                    next_thread = self.find_free_thread()
                    while True:
                        places = cur.fetchmany(batch)
                        if not places:
                            break
                        progress.add(len(places))

                        places = obj.preprocess_places(ro_cur, places)

                        LOG.debug("Processing places: %s", str(places))
                        t0 = time.time()
                        thread = next(next_thread)
                        timing_find_thread += time.time() - t0

                        thread.perform(obj.sql_index_place(places))
            finally:
                ro_cur.close()
                ro_conn.close()

            self.conn.commit()

            for thread in self.threads:
                thread.wait()

        total_time = progress.done()
        LOG.warning("Time waiting for Postgresql: {:.2f}s ({:.2f}%)".format(
            timing_find_thread, timing_find_thread*100/total_time))

    def find_free_thread(self):
        """ Generator that returns the next connection that is free for
            sending a query.
        """
        ready = self.threads
        command_stat = 0

        while True:
            for thread in ready:
                if thread.is_done():
                    command_stat += 1
                    yield thread

            # refresh the connections occasionaly to avoid potential
            # memory leaks in Postgresql.
            if command_stat > 100000:
                for thread in self.threads:
                    while not thread.is_done():
                        thread.wait()
                    thread.connect()
                command_stat = 0
                ready = self.threads
            else:
                ready, _, _ = select.select(self.threads, [], [])

        assert False, "Unreachable code"
