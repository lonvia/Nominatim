"""
Tokenizer implementing nromalisation as used before Nominatim 4.
"""
import functools
import re

import psycopg2.extras

from nominatim.db.connection import connect
from nominatim.db.utils import execute_file

def create(dsn, data_dir):
    return LegacyTokenizer(dsn, data_dir)

class LegacyTokenizer:
    """ The legacy tokenizer uses a special Postgresql module to normalize
        names and SQL functions to split them into tokens.
    """

    def __init__(self, dsn, data_dir):
        self.dsn = dsn
        self.data_dir = data_dir


    def init_new_db(self):
        """ Set up the tokenizer for a new import.

            This copies all necessary data in the project directory to make
            sure the tokenizer remains stable even over updates.

            This function is called after the placex table has been loaded
            with the unindexed data and before indexing. The function may
            use the content of the placex table to initialise its data
            structures.
        """
        self.update_sql_functions()
        with connect(self.dsn) as conn:
            self._compute_word_frequencies(conn)



    def init_from_project(self):
        """ Initialise the tokenizer from the project directory.
        """
        pass


    def update_sql_functions(self):
        """ Reimport the SQL functions for this tokenizer.
        """
        execute_file(self.dsn, self.data_dir / 'tokenizer.sql')


    def get_name_analyzer(self):
        """ Create a new analyzer for tokenizing names from OpenStreetMap
            using this tokinzer.

            Analyzers are not thread-safe. You need to instantiate one per thread.
        """
        return LegacyNameAnalyzer(self.dsn)


    def _compute_word_frequencies(self, conn):
        """ Compute the frequencies of words.

            They are used to decide if partial words are handled as stop words.
            Stop words are never added to the search_name table. Therefore they
            need to be known before indexing creates the search terms.
        """
        with conn.cursor() as cur:
            cur.execute("""CREATE TEMP TABLE word_frequencies AS
                          (SELECT unnest(make_keywords(v)) as id, sum(count) as count
                           FROM (select svals(name) as v, count(*)from place group by v) cnt
                            WHERE v is not null
                             GROUP BY id)""")

            # copy the word frequencies
            cur.execute("""update word set search_name_count = count from word_frequencies wf where wf.id = word.word_id""")

            # and drop the temporary frequency table again
            cur.execute("drop table word_frequencies");
        conn.commit()



class LegacyNameAnalyzer:
    """ The legacy analyzer uses the special Postgresql module for
        splitting names.

        Each instance opens a connection to the database to request the
        normalization.
    """

    def __init__(self, dsn):
        self.conn = connect(dsn).connection
        self.conn.autocommit = True
        psycopg2.extras.register_hstore(self.conn)

        self._precompute_housenumbers()


    def close(self):
        """ Shut down the analyzer and free all resources.
        """
        if self.conn:
            self.conn.close()
            self.conn = None

    def tokenize(self, place):
        """ Tokenize the given names. `places` is a dictionary of
            properties of the object to get the name tokens for. The place
            must have a property `name` with a dictionary with
            key/value pairs of OSM tags that should be tokenized. Other
            properties depend on the software version used. The tokenizer must
            ignore unknown ones.

            Returned is a JSON-serializable data structure with the
            information that the SQL part of the tokenizer requires.
        """
        token_info = {}

        names = place.get('name')
        address = place.get('address')

        if names:
            with self.conn.cursor() as cur:
                # create the token IDs for all names
                cur.execute("SELECT make_keywords(%s)::text", (names, ))
                token_info['names'] = cur.fetchone()[0]

        if address:
            # add housenumber tokens to word table
            hnrs = tuple((v for k, v in address.items()
                    if k in ('housenumber', 'streetnumber', 'conscriptionnumber')))
            if hnrs:
                token_info['hnr'] = self._get_housenumber_ids(hnrs)

            # add postcode token to word table
            if 'postcode' in address:
                self._create_postcode_id(address['postcode'])

            # terms for matching up streets and places
            for atype in ('street', 'place'):
                if atype in address:
                    token_info[atype + '_match'], token_info[atype + '_search'] = \
                        self._get_street_place_terms(address[atype])

            # terms for other address parts
            token_info['addr'] = {k : self._get_addr_terms(v) for k, v in address.items()
                                  if k not in ('country', 'street', 'place', 'postcode',
                                               'housenumber', 'streetnumber', 'conscriptionnumber')}

        return token_info

    @functools.lru_cache(maxsize=1024)
    def _get_addr_terms(self, name):
        with self.conn.cursor() as cur:
            cur.execute("""SELECT addr_ids_from_name(%s)::text,
                                  word_ids_from_name(%s)::text""",
                        (name, name))
            return cur.fetchone()


    @functools.lru_cache(maxsize=256)
    def _get_street_place_terms(self, name):
        with self.conn.cursor() as cur:
            cur.execute("""SELECT word_ids_from_name(%s)::text,
                                  ARRAY[getorcreate_name_id(make_standard_name(%s), '')]::text""",
                        (name, name))
            return cur.fetchone()

    @functools.lru_cache(maxsize=32)
    def _create_postcode_id(self, postcode):
        if re.search(r'[:,;]', postcode) is None:
            with self.conn.cursor() as cur:
                cur.execute('SELECT getorcreate_postcode_id(%s)', (postcode, ))

    def _get_housenumber_ids(self, hnrs):
        if hnrs in self._cached_housenumbers:
            return self._cached_housenumbers[hnrs]

        with self.conn.cursor() as cur:
            cur.execute("""SELECT array_agg(getorcreate_housenumber_id(make_standard_name(hnr.name)))::text
                           FROM (VALUES {}) as hnr(name)
                        """.format(','.join(['(%s)']  * len(hnrs))),
                        hnrs)
            return cur.fetchone()[0]

    def _precompute_housenumbers(self):
        with self.conn.cursor() as cur:
            cur.execute("""SELECT i, ARRAY[getorcreate_housenumber_id(make_standard_name(i::text))]::text
                           FROM generate_series(1, 100) as i""")
            self._cached_housenumbers = {(str(r[0]), ) : r[1] for r in cur}
