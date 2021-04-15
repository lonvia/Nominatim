"""
Tokenizer implementing simple normalisation as used before Nominatim 4
but using libICU to latinise the words.
"""
import io
import functools
import re
import json
from textwrap import dedent
from pathlib import Path

from icu import Transliterator
import psycopg2.extras
from collections import Counter

from nominatim.db.connection import connect
from nominatim.db.utils import execute_file
from nominatim.db.sql_preprocessor import SQLPreprocessor
from nominatim.db import properties

def create(dsn, data_dir):
    return LegacyICUTokenizer(dsn, data_dir)

class LegacyICUTokenizer:
    """ The legacy tokenizer uses a special Postgresql module to normalize
        names and SQL functions to split them into tokens.
    """

    def __init__(self, dsn, data_dir):
        self.dsn = dsn
        self.data_dir = data_dir
        self.normalization = None
        self.transliterator = None
        self.abbreviations = None


    def init_new_db(self, config, sqllib_dir, phplib_dir, module_dir, config_dir):
        """ Set up the tokenizer for a new import.

            This copies all necessary data in the project directory to make
            sure the tokenizer remains stable even over updates.

            This function is called after the placex table has been loaded
            with the unindexed data and before indexing. The function may
            use the content of the placex table to initialise its data
            structures.
        """
        if config.TOKENIZER_CONFIG:
            cfgfile = Path(config.TOKENIZER_CONFIG)
        else:
            cfgfile = config_dir / 'legacy_icu_tokenizer.json'

        rules = json.loads(cfgfile.read_text())
        self.transliterator = ';'.join(rules['normalization']) + ';'
        self.abbreviations = rules["abbreviations"]
        self.normalization = config.TERM_NORMALIZATION

        with connect(self.dsn) as conn:
            # create the word table
            sql_processor = SQLPreprocessor(conn, config, sqllib_dir)
            sql_processor.run_sql_file(conn, 'tokenizers/legacy_tokenizer_tables.sql')

            self.update_sql_functions(config, sqllib_dir)
            self._compute_word_frequencies(conn)

            properties.set_property(conn, "tokenizer_normalization",
                                    config.TERM_NORMALIZATION)
            properties.set_property(conn, "tokenizer_transliterator",
                                    self.transliterator)
            properties.set_property(conn, "tokenizer_abbreviations",
                                    json.dumps(self.abbreviations))


        php_file = self.data_dir / "tokenizer.php"
        php_file.write_text(dedent("""\
            <?php

            @define('CONST_Max_Word_Frequency', {0.MAX_WORD_FREQUENCY});
            @define('CONST_Term_Normalization_Rules', "{0.TERM_NORMALIZATION}");

            require_once('{1}/tokenizers/legacy_icu_tokenizer.php');
            """.format(config, str(phplib_dir))))


    def init_from_project(self, config):
        """ Initialise the tokenizer from the project directory.
        """
        with connect(self.dsn) as conn:
            self.normalization = properties.get_property(conn, "tokenizer_normalization")
            self.transliterator = properties.get_property(conn, "tokenizer_transliterator")
            self.abbreviations = json.loads(properties.get_property(conn, "tokenizer_abbreviations"))


    def update_sql_functions(self, config, sqllib_dir):
        """ Reimport the SQL functions for this tokenizer.
        """
        with connect(self.dsn) as conn:
            sqlp = SQLPreprocessor(conn, config, sqllib_dir)
            sqlp.run_sql_file(conn, 'tokenizers/legacy_icu_tokenizer.sql')


    def get_name_analyzer(self):
        """ Create a new analyzer for tokenizing names from OpenStreetMap
            using this tokinzer.

            Analyzers are not thread-safe. You need to instantiate one per thread.
        """
        norm = Transliterator.createFromRules("normalizer", self.normalization)
        trans = Transliterator.createFromRules("transliterator", self.transliterator)
        return LegacyIcuNameAnalyzer(self.dsn, norm, trans, self.abbreviations)


    def _compute_word_frequencies(self, conn):
        """ Compute the frequencies of partial words.

            They are used to decide if partial words are handled as stop words.
            Stop words are never added to the search_name table. Therefore they
            need to be known before indexing creates the search terms.
        """
        words = Counter()
        analyzer = self.get_name_analyzer()

        # get partial words and their frequencies
        with conn.cursor(name="words") as cur:
            cur.execute("SELECT svals(name) as v, count(*) FROM place GROUP BY v")

            for name, cnt in cur:
                for word in analyzer.make_standard_word(name).split():
                    words[word] += cnt

        # copy them back into the word table
        copystr = io.StringIO(''.join(('{}\t{}\n'.format(*args) for args in words.items())))

        with conn.cursor() as cur:
            cur.copy_from(copystr, 'word', columns=['word_token', 'search_name_count'])
            cur.execute("""UPDATE word SET word_id = nextval('seq_word')
                           WHERE word_id is null""")

        conn.commit()


    def _setup_normalizer(self, config):
        self.transliterator = Transliterator.createFromRules("special-phrases normalizer",
                                                             config.TERM_NORMALIZATION)


class LegacyIcuNameAnalyzer:
    """ The legacy analyzer uses the special Postgresql module for
        splitting names.

        Each instance opens a connection to the database to request the
        normalization.
    """

    def __init__(self, dsn, normalizer, transliterator, abbreviations):
        self.normalizer = normalizer
        self.transliterator = transliterator
        self.abbreviations = abbreviations
        self.conn = connect(dsn).connection
        self.conn.autocommit = True
        psycopg2.extras.register_hstore(self.conn)

        self._precompute_housenumbers()

    def make_standard_word(self, name):
        """ Create the normalised version of the name.
        """
        norm = ' ' + self.transliterator.transliterate(name) + ' '
        for full, abbr in self.abbreviations:
            if full in norm:
                norm = norm.replace(full, abbr)

        return norm.strip()

    def close(self):
        """ Shut down the analyzer and free all resources.
        """
        print("CACHE INFO postcode", self._create_postcode_id.cache_info())
        print("CACHE INFO street/place", self._get_street_place_terms.cache_info())
        print("CACHE INFO addr", self._get_addr_terms.cache_info())
        if self.conn:
            self.conn.close()
            self.conn = None


    def add_country_names(self, country_code, names):
        """ Add the names 'names' for the country with the given country_code.
        """
        if not re.fullmatch(r'[a-z][a-z]', country_code):
            return

        self._add_normalized_country_names(country_code,
                                           set((self.make_standard_word(n) for n in names)))


    def _add_normalized_country_names(self, country_code, normalized_names):
        filtered = [(n, ) for n in normalized_names if n]
        with self.conn.cursor() as cur:
            sql = """INSERT INTO word (word_id, word_token, country_code, search_name_count)
                     (SELECT nextval('seq_word'), ' ' || name, '{0}', 0 FROM (VALUES %s) AS v (name)
                      WHERE NOT EXISTS (SELECT * FROM word
                                        WHERE word_token = ' ' || name and country_code = '{0}'))
                  """.format(country_code)
            psycopg2.extras.execute_values(cur, sql, filtered)


    def add_special_phrase(self, name, osm_key, osm_value, operator):
        normalized = self.normalizer.transliterate(name)
        token = self.make_standard_word(name)

        if operator in ('near', 'in'):
            sql = """INSERT INTO word (word_id, word_token, word, class, type, operator, search_name_count)
                     (SELECT nextval('seq_word'), ' ' || token, name, cls, typ, op, 0
                      FROM (VALUES (%s, %s, %s, %s, %s)) AS v (token, name, cls, typ, op)
                      WHERE NOT EXISTS (SELECT * FROM word
                                        WHERE word_token = ' ' || token
                                              and word = name
                                              and country_code = cc
                                              and cls = class and type = typ
                                              and operator = op))
                  """
            values = (token, normalized, osm_key, osm_value, operator)
        else:
            sql = """INSERT INTO word (word_id, word_token, word, class, type, search_name_count)
                     (SELECT nextval('seq_word'), ' ' || token, name, cls, typ, 0
                      FROM (VALUES (%s, %s, %s, %s)) AS v (token, name, cls, typ)
                      WHERE NOT EXISTS (SELECT * FROM word
                                        WHERE word_token = ' ' || token
                                              and word = name
                                              and country_code = cc
                                              and cls = class and type = typ))
                  """
            values = (token, normalized, osm_key, osm_value)

        with self.conn.cursor() as cur:
            cur.execute(cur, sql, values)


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

            norm_names = set((self.make_standard_word(name) for name in names.values()))
            partials = set((part for names in norm_names for part in names.split()))

            with self.conn.cursor() as cur:
                # create the token IDs for all names
                cur.execute("""SELECT array_remove(array_agg(wid), null)::TEXT FROM
                               (SELECT getorcreate_name_id(token, '') as wid
                                  FROM unnest(%s) as token
                                 UNION ALL
                                SELECT getorcreate_word_id(token) as wid
                                  FROM unnest(%s) as token)y
                            """,
                             ([n for n in norm_names if n], [p for p in partials if p]))
                token_info['names'] = cur.fetchone()[0]

                # also add country tokens, if applicable
                if country_feature and re.fullmatch(r'[A-Za-z][A-Za-z]', country_feature):
                    self._add_normalized_country_names(country_feature.lower(), norm_names)

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
        norm = self.make_standard_word(name)
        if not norm:
            return '{}', '{}'

        with self.conn.cursor() as cur:
            cur.execute("""SELECT addr_ids_from_name(%s)::text,
                                  word_ids_from_name(%s)::text""",
                        (norm, norm))
            return cur.fetchone()


    @functools.lru_cache(maxsize=256)
    def _get_street_place_terms(self, name):
        norm = self.make_standard_word(name)
        if not norm:
            return '{}', '{}'

        with self.conn.cursor() as cur:
            cur.execute("""SELECT word_ids_from_name(%s)::text,
                                  ARRAY[getorcreate_name_id(%s, '')]::text""",
                        (norm, norm))
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
        normalised = [self.make_standard_word(w) for w in simple_set]

        with self.conn.cursor() as cur:
            cur.execute(""" SELECT array_agg(getorcreate_housenumber_id(hnr))::TEXT
                            FROM unnest(%s) AS hnr
                        """, (normalised, ))

            return cur.fetchone()[0], ';'.join(normalised)

    def _precompute_housenumbers(self):
        with self.conn.cursor() as cur:
            # shortcut here because integer housenumbers are already normalised
            cur.execute("""SELECT i, ARRAY[getorcreate_housenumber_id(i::text)]::text
                           FROM generate_series(1, 100) as i""")
            self._cached_housenumbers = {(str(r[0]), ) : r[1] for r in cur}
