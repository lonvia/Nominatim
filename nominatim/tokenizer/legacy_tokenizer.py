"""
Tokenizer implementing nromalisation as used before Nominatim 4.
"""
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

            cur.execute("""select count(getorcreate_postcode_id(v)) from (select distinct address->'postcode' as v from place where address ? 'postcode') as w where v is not null""")
            cur.execute("""select count(getorcreate_housenumber_id(make_standard_name(v))) from (select distinct address->'housenumber' as v from place where address ? 'housenumber') as w""")

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

        if names:
            with self.conn.cursor() as cur:
                cur.execute("SELECT make_keywords(%s)::text", (names, ))
                token_info['names'] = cur.fetchone()[0]

        return token_info


