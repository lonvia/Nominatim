"""
Main work horse for indexing (computing addresses) the database.
"""
# pylint: disable=C0111
import logging
import selectors
import time

import psycopg2.extras

from .progress import ProgressLogger
from .worker import IndexWorker
from ..db.connection import connect
from . import runners

LOG = logging.getLogger()


class WorkerPool:

    def __init__(self, dsn, runner, pool_size):
        self.selectors = selectors.DefaultSelector()
        self.threads = []

        for _ in range(pool_size):
            thread = IndexWorker(dsn, runner)
            self.selectors.register(thread, selectors.EVENT_WRITE, thread)
            self.threads.append(thread)

        self.free_workers = self._yield_free_worker()


    def close(self):
        for thread in self.threads:
            self.selectors.unregister(thread)
            thread.close()
        self.threads = []
        self.selectors.close()


    def has_workers(self):
        return len(self.threads) > 0


    def shutdown_worker(self, worker):
        self.selectors.unregister(worker)
        worker.close()
        self.threads.remove(worker)


    def next_free_worker(self):
        return next(self.free_workers)

    def _yield_free_worker(self):
        while True:
            for key, _ in self.selectors.select():
                yield key.data

    def __enter__(self):
        return self


    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class Indexer:
    """ Main indexing routine.
    """

    def __init__(self, dsn, tokenizer, num_threads):
        self.dsn = dsn
        self.tokenizer = tokenizer
        self.num_threads = num_threads


    def index_full(self, analyse=True):
        """ Index the complete database. This will first index boudnaries
            followed by all other objects. When `analyse` is True, then the
            database will be analysed at the appropriate places to
            ensure that database statistics are updated.
        """
        with connect(self.dsn) as conn:
            conn.autocommit = True

            if analyse:
                def _analyse():
                    with conn.cursor() as cur:
                        cur.execute('ANALYSE')
            else:
                def _analyse():
                    pass

            self.index_by_rank(0, 4)
            _analyse()

            self.index_boundaries(0, 30)
            _analyse()

            self.index_by_rank(5, 25)
            _analyse()

            self.index_by_rank(26, 30)
            _analyse()

            self.index_postcodes()
            _analyse()


    def index_boundaries(self, minrank, maxrank):
        """ Index only administrative boundaries within the given rank range.
        """
        LOG.warning("Starting indexing boundaries using %s threads",
                    self.num_threads)

        for rank in range(max(minrank, 4), min(maxrank, 26)):
            self._index(runners.BoundaryRunner(rank, self.tokenizer))

    def index_by_rank(self, minrank, maxrank):
        """ Index all entries of placex in the given rank range (inclusive)
            in order of their address rank.

            When rank 30 is requested then also interpolations and
            places with address rank 0 will be indexed.
        """
        maxrank = min(maxrank, 30)
        LOG.warning("Starting indexing rank (%i to %i) using %i threads",
                    minrank, maxrank, self.num_threads)

        for rank in range(max(1, minrank), maxrank):
            self._index(runners.RankRunner(rank, self.tokenizer))

        if maxrank == 30:
            self._index(runners.RankRunner(0, self.tokenizer))
            self._index(runners.InterpolationRunner(self.tokenizer), 20)
            self._index(runners.RankRunner(30, self.tokenizer), 20)
        else:
            self._index(runners.RankRunner(maxrank, self.tokenizer))


    def index_postcodes(self):
        """Index the entries ofthe location_postcode table.
        """
        LOG.warning("Starting indexing postcodes using %s threads", self.num_threads)
        self._index(runners.PostcodeRunner(), 20)


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


    def _index(self, runner, batch=1):
        """ New style indexing.
        """
        LOG.warning("Starting %s (using batch size %s)", runner.name(), batch)

        with WorkerPool(self.dsn, runner, self.num_threads) as pool:
            total_tuples = 0
            slices = []

            with connect(self.dsn) as conn:
                psycopg2.extras.register_hstore(conn, globally=True) #argh global
                with conn.cursor() as cur:
                    total_tuples = cur.scalar(runner.sql_count_objects())
                    LOG.debug("Total number of rows: %i", total_tuples)

                conn.commit()

                progress = ProgressLogger(runner.name(), total_tuples)
                timing_find_thread = 0

                with conn.cursor(name="placeids") as cur:
                    cur.execute(runner.sql_get_objects())

                    worker = pool.next_free_worker()
                    while True:
                        done_items = worker.continue_slice()

                        if done_items < 0:
                            ids = [row[0] for row in cur.fetchmany(300)]
                            if not ids:
                                break

                            worker.start_slice(ids, batch)
                        else:
                            progress.add(done_items)

                            t0 = time.time()
                            worker = pool.next_free_worker()
                            timing_find_thread += time.time() - t0

                conn.commit()

            # let all workers finish
            while pool.has_workers():
                worker = pool.next_free_worker()
                done_items = worker.continue_slice()
                if done_items < 0:
                    pool.shutdown_worker(worker)
                else:
                    progress.add(done_items)

        total_time = progress.done()
        LOG.warning("Time waiting for Postgresql: {:.2f}s ({:.2f}%)".format(
                     timing_find_thread, timing_find_thread*100/total_time))
