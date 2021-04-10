"""
Tokenizer implementing nromalisation as used before Nominatim 4.
"""
import functools
import re
from textwrap import dedent

from icu import Transliterator
import psycopg2.extras

from nominatim.db.connection import connect
from nominatim.db.utils import execute_file
from nominatim.db import properties

def create(dsn, data_dir):
    return LegacyTokenizer(dsn, data_dir)

class LegacyTokenizer:
    """ The legacy tokenizer uses a special Postgresql module to normalize
        names and SQL functions to split them into tokens.
    """

    def __init__(self, dsn, data_dir):
        self.dsn = dsn
        self.data_dir = data_dir
        self.normalization = None


    def init_new_db(self, config, sqllib_dir, phplib_dir):
        """ Set up the tokenizer for a new import.

            This copies all necessary data in the project directory to make
            sure the tokenizer remains stable even over updates.

            This function is called after the placex table has been loaded
            with the unindexed data and before indexing. The function may
            use the content of the placex table to initialise its data
            structures.
        """
        with connect(self.dsn) as conn:
            with conn.cursor() as cur:
                # Used by getorcreate_word_id to ignore frequent partial words.
                # Must be set to a fixed number on import and then never changed.
                cur.execute("""CREATE OR REPLACE FUNCTION get_maxwordfreq()
                               RETURNS integer AS
                               $$ SELECT %s as maxwordfreq $$ LANGUAGE SQL IMMUTABLE
                            """, (int(config.MAX_WORD_FREQUENCY), ))

            self.update_sql_functions(sqllib_dir)
            self._compute_word_frequencies(conn)

            properties.set_property(conn, "tokenizer_normalization", config.TERM_NORMALIZATION)

        self.normalization = config.TERM_NORMALIZATION

        php_file = self.data_dir / "tokenizer.php"
        php_file.write_text(dedent("""\
            <?php

            @define('CONST_Max_Word_Frequency', {0.MAX_WORD_FREQUENCY});
            @define('CONST_Term_Normalization_Rules', "{0.TERM_NORMALIZATION}");

            require_once('{1}/tokenizers/legacy_tokenizer.php');
            """.format(config, str(phplib_dir))))


    def init_from_project(self, config):
        """ Initialise the tokenizer from the project directory.
        """
        with connect(self.dsn) as conn:
            self.normalization = properties.get_property(conn, "tokenizer_normalization")


    def update_sql_functions(self, sqllib_dir):
        """ Reimport the SQL functions for this tokenizer.
        """
        execute_file(self.dsn, sqllib_dir / 'tokenizers' / 'legacy_tokenizer.sql')


    def get_name_analyzer(self):
        """ Create a new analyzer for tokenizing names from OpenStreetMap
            using this tokinzer.

            Analyzers are not thread-safe. You need to instantiate one per thread.
        """
        return LegacyNameAnalyzer(self.dsn,
                                  Transliterator.createFromRules("special-phrases normalizer",
                                                                 self.normalization))


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


    def _setup_normalizer(self, config):
        self.transliterator = Transliterator.createFromRules("special-phrases normalizer",
                                                             config.TERM_NORMALIZATION)


class LegacyNameAnalyzer:
    """ The legacy analyzer uses the special Postgresql module for
        splitting names.

        Each instance opens a connection to the database to request the
        normalization.
    """

    def __init__(self, dsn, normalizer):
        self.normalizer = normalizer
        self.conn = connect(dsn).connection
        self.conn.autocommit = True
        psycopg2.extras.register_hstore(self.conn)

        self._precompute_housenumbers()


    def close(self):
        """ Shut down the analyzer and free all resources.
        """
        print("CACHE INFO postcode", self._create_postcode_id.cache_info())
        print("CACHE INFO street/place", self._get_street_place_terms.cache_info())
        print("CACHE INFO addr", self._get_addr_terms.cache_info())
        if self.conn:
            self.conn.close()
            self.conn = None


    def add_country_words(self, names):
        """ Add the given country names. 'names' is an iterable of tuple pairs
            with country_code and the name.
        """
        with self.conn.cursor() as cur:
            cur.execute("""SELECT getorcreate_country(make_standard_name(v.name), v.cc)
                           FROM (VALUES {}) as v(cc, name)
                        """.format(','.join(["(%s, %s)"]  * len(names))),
                        [val for sublist in names for val in sublist])


    def add_special_phrase(self, name, osm_key, osm_value, operator):
        normalized = self.normalizer.transliterate(name)

        with self.conn.cursor() as cur:
            if operator in ('near', 'in'):
                cur.execute("""SELECT getorcreate_amenityoperator(
                                 make_standard_name(%s), %s, %s, %s, %s)""",
                            (name, normalized, osm_key, osm_value, operator))
            else:
                cur.execute("""SELECT getorcreate_amenity(
                                 make_standard_name(%s), %s, %s, %s)""",
                            (name, normalized, osm_key, osm_value))


    def normalize_postcode(self, postcode):
        """ Get the normalized version of the postcode.

            This function is currently unused but mat be put into use later.
            It must return exactly the same normalized form as the SQL
            function 'token_normalized_postcode()'.
        """
        if postcode is not None and re.search(r'[:,;]', postcode) is None:
          return postcode.strip().upper()


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
            country_feature = place.get('country_feature')

            with self.conn.cursor() as cur:
                # create the token IDs for all names
                cur.execute("SELECT make_keywords(%s)::text", (names, ))
                token_info['names'] = cur.fetchone()[0]

                # also add country tokens, if applicable
                if country_feature and re.fullmatch(r'[A-Za-z][A-Za-z]', country_feature):
                    cur.execute("SELECT create_country(%s, %s)",
                                (names, country_feature.lower()))

        if address:
            # add housenumber tokens to word table
            hnrs = tuple((v for k, v in address.items()
                    if k in ('housenumber', 'streetnumber', 'conscriptionnumber')))
            if hnrs:
                token_info['hnr_search'], token_info['hnr_match'] = self._get_housenumber_ids(hnrs)

            # add postcode token to word table
            postcode = self.normalize_postcode(address.get('postcode'))
            if postcode:
                self._create_postcode_id(postcode)

            # terms for matching up streets and places
            for atype in ('street', 'place'):
                if atype in address:
                    token_info[atype + '_match'], token_info[atype + '_search'] = \
                        self._get_street_place_terms(address[atype])

            # terms for other address parts
            token_info['addr'] = {k : self._get_addr_terms(v) for k, v in address.items()
                                  if k not in ('country', 'street', 'place', 'postcode', 'full',
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
        with self.conn.cursor() as cur:
            cur.execute('SELECT create_postcode_id(%s)', (postcode, ))

    def _get_housenumber_ids(self, hnrs):
        if hnrs in self._cached_housenumbers:
            return self._cached_housenumbers[hnrs], hnrs[0]

        # split numbers if necessary
        simple_set = set()
        for hnr in hnrs:
            simple_set.update((x.strip() for x in re.split(r'[;,]', hnr)))

        with self.conn.cursor() as cur:
            cur.execute("SELECT (create_housenumbers(%s)).* ", (list(simple_set), ))
            return cur.fetchone()

    def _precompute_housenumbers(self):
        with self.conn.cursor() as cur:
            # shortcut here because integer housenumbers are already normalised
            cur.execute("""SELECT i, ARRAY[getorcreate_housenumber_id(i::text)]::text
                           FROM generate_series(1, 100) as i""")
            self._cached_housenumbers = {(str(r[0]), ) : r[1] for r in cur}
