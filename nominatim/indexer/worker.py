"""
Implementation of a worker that indexes over an asynchronous connection.
"""

import psycopg2.extras

from ..db.async_connection import DBConnection

class IndexWorker:

    def __init__(self, dsn, runner):
        self.conn = DBConnection(dsn, cursor_factory=psycopg2.extras.DictCursor)
        self.runner = runner
        self.in_progress = None

        self._place_count = 0


    def close(self):
        if self.conn:
          self.conn.close()
          self.conn = None


    def fileno(self):
        return self.conn.fileno()


    def start_slice(self, ids, batch_size):
        if self._place_count > 10000:
            self.conn.connect(cursor_factory=psycopg2.extras.DictCursor)
            self._place_count = 0
        self.in_progress = self._process_slice(ids, batch_size)


    def continue_slice(self):
        if self.in_progress is None:
            return -1

        done = next(self.in_progress)
        if done < 0:
            self.in_progress = None

        return done


    def _process_slice(self, ids, batch_size):
        if hasattr(self.runner, 'sql_get_object_info'):
            self.conn.perform(*self.runner.sql_get_object_info(ids))
            while not self.conn.is_done():
                yield 0

            ids = self.conn.fetchall()

        idx = 0
        done = 0
        while idx < len(ids):
            end_idx = idx + batch_size
            todo = ids[idx:end_idx]
            self.conn.perform(*self.runner.sql_index_places(todo))
            while not self.conn.is_done():
                yield done
                done = 0
            done = len(todo)
            idx = end_idx

        self._place_count += len(ids)
        yield done
        yield -1
