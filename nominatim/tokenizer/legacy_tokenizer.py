"""
Tokenizer implementing normalisation as used before Nominatim 4.
"""
from collections import OrderedDict
import logging
import re
import shutil

from icu import Transliterator
import psycopg2
import psycopg2.extras

from nominatim.db.connection import connect
from nominatim.db import properties
from nominatim.db import utils as db_utils
from nominatim.db.sql_preprocessor import SQLPreprocessor
from nominatim.errors import UsageError

DBCFG_NORMALIZATION = "tokenizer_normalization"
DBCFG_MAXWORDFREQ = "tokenizer_maxwordfreq"

LOG = logging.getLogger()

def create(dsn, data_dir):
    """ Create a new instance of the tokenizer provided by this module.
    """
    return LegacyTokenizer(dsn, data_dir)


def _install_module(config_module_path, src_dir, module_dir):
    """ Copies the PostgreSQL normalisation module into the project
        directory if necessary. For historical reasons the module is
        saved in the '/module' subdirectory and not with the other tokenizer
        data.

        The function detects when the installation is run from the
        build directory. It doesn't touch the module in that case.
    """
    # Custom module locations are simply used as is.
    if config_module_path:
        LOG.info("Using custom path for database module at '%s'", config_module_path)
        return config_module_path

    # Compatibility mode for builddir installations.
    if module_dir.exists() and src_dir.samefile(module_dir):
        LOG.info('Running from build directory. Leaving database module as is.')
        return module_dir

    # In any other case install the module in the project directory.
    if not module_dir.exists():
        module_dir.mkdir()

    destfile = module_dir / 'nominatim.so'
    shutil.copy(str(src_dir / 'nominatim.so'), str(destfile))
    destfile.chmod(0o755)

    LOG.info('Database module installed at %s', str(destfile))

    return module_dir


def _check_module(module_dir, conn):
    """ Try to use the PostgreSQL module to confirm that it is correctly
        installed and accessible from PostgreSQL.
    """
    with conn.cursor() as cur:
        try:
            cur.execute("""CREATE FUNCTION nominatim_test_import_func(text)
                           RETURNS text AS '{}/nominatim.so', 'transliteration'
                           LANGUAGE c IMMUTABLE STRICT;
                           DROP FUNCTION nominatim_test_import_func(text)
                        """.format(module_dir))
        except psycopg2.DatabaseError as err:
            LOG.fatal("Error accessing database module: %s", err)
            raise UsageError("Database module cannot be accessed.") from err


class LegacyTokenizer:
    """ The legacy tokenizer uses a special PostgreSQL module to normalize
        names and queries. The tokenizer thus implements normalization through
        calls to the database.
    """

    def __init__(self, dsn, data_dir):
        self.dsn = dsn
        self.data_dir = data_dir
        self.normalization = None


    def init_new_db(self, config):
        """ Set up a new tokenizer for the database.

            This copies all necessary data in the project directory to make
            sure the tokenizer remains stable even over updates.
        """
        module_dir = _install_module(config.DATABASE_MODULE_PATH,
                                     config.lib_dir.module,
                                     config.project_dir / 'module')

        self.normalization = config.TERM_NORMALIZATION

        with connect(self.dsn) as conn:
            _check_module(module_dir, conn)
            self._save_config(conn, config)
            conn.commit()

        self.update_sql_functions(config)
        self._init_db_tables(config)


    def init_from_project(self):
        """ Initialise the tokenizer from the project directory.
        """
        with connect(self.dsn) as conn:
            self.normalization = properties.get_property(conn, DBCFG_NORMALIZATION)


    def update_sql_functions(self, config):
        """ Reimport the SQL functions for this tokenizer.
        """
        with connect(self.dsn) as conn:
            max_word_freq = properties.get_property(conn, DBCFG_MAXWORDFREQ)
            modulepath = config.DATABASE_MODULE_PATH or \
                         str((config.project_dir / 'module').resolve())
            sqlp = SQLPreprocessor(conn, config)
            sqlp.run_sql_file(conn, 'tokenizer/legacy_tokenizer.sql',
                              max_word_freq=max_word_freq,
                              modulepath=modulepath)


    def migrate_database(self, config):
        """ Initialise the project directory of an existing database for
            use with this tokenizer.

            This is a special migration function for updating existing databases
            to new software versions.
        """
        module_dir = _install_module(config.DATABASE_MODULE_PATH,
                                     config.lib_dir.module,
                                     config.project_dir / 'module')

        with connect(self.dsn) as conn:
            _check_module(module_dir, conn)
            self._save_config(conn, config)


    def name_analyzer(self):
        """ Create a new analyzer for tokenizing names and queries
            using this tokinzer. Analyzers are context managers and should
            be used accordingly:

            ```
            with tokenizer.name_analyzer() as analyzer:
                analyser.tokenize()
            ```

            When used outside the with construct, the caller must ensure to
            call the close() function before destructing the analyzer.

            Analyzers are not thread-safe. You need to instantiate one per thread.
        """
        normalizer = Transliterator.createFromRules("phrase normalizer",
                                                    self.normalization)
        return LegacyNameAnalyzer(self.dsn, normalizer)


    def _init_db_tables(self, config):
        """ Set up the word table and fill it with pre-computed word
            frequencies.
        """
        with connect(self.dsn) as conn:
            sqlp = SQLPreprocessor(conn, config)
            sqlp.run_sql_file(conn, 'tokenizer/legacy_tokenizer_tables.sql')
            conn.commit()

        LOG.warning("Precomputing word tokens")
        db_utils.execute_file(self.dsn, config.lib_dir.data / 'words.sql')


    def _save_config(self, conn, config):
        """ Save the configuration that needs to remain stable for the given
            database as database properties.
        """
        properties.set_property(conn, DBCFG_NORMALIZATION, self.normalization)
        properties.set_property(conn, DBCFG_MAXWORDFREQ, config.MAX_WORD_FREQUENCY)


class LegacyNameAnalyzer:
    """ The legacy analyzer uses the special Postgresql module for
        splitting names.

        Each instance opens a connection to the database to request the
        normalization.
    """

    def __init__(self, dsn, normalizer):
        self.conn = connect(dsn).connection
        self.conn.autocommit = True
        self.normalizer = normalizer
        psycopg2.extras.register_hstore(self.conn)

        self._cache = _TokenCache(self.conn)


    def __enter__(self):
        return self


    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


    def close(self):
        """ Free all resources used by the analyzer.
        """
        if self.conn:
            self.conn.close()
            self.conn = None


    def normalize(self, phrase):
        """ Normalize the given phrase, i.e. remove all properties that
            are irrelevant for search.
        """
        return self.normalizer.transliterate(phrase)


    def add_postcodes_from_db(self):
        """ Add postcodes from the location_postcode table to the word table.
        """
        with self.conn.cursor() as cur:
            cur.execute("""SELECT count(create_postcode_id(pc))
                           FROM (SELECT distinct(postcode) as pc
                                 FROM location_postcode) x""")


    def update_special_phrases(self, phrases):
        """ Replace the search index for special phrases with the new phrases.
        """
        norm_phrases = set(((self.normalize(p[0]), p[1], p[2], p[3])
                            for p in phrases))

        with self.conn.cursor() as cur:
            # Get the old phrases.
            existing_phrases = set()
            cur.execute("""SELECT word, class, type, operator FROM word
                           WHERE class != 'place'
                                 OR (type != 'house' AND type != 'postcode')""")
            for label, cls, typ, oper in cur:
                existing_phrases.add((label, cls, typ, oper or '-'))

            to_add = norm_phrases - existing_phrases
            to_delete = existing_phrases - norm_phrases

            if to_add:
                psycopg2.extras.execute_values(
                    cur,
                    """ INSERT INTO word (word_id, word_token, word, class, type,
                                          search_name_count, operator)
                        (SELECT nextval('seq_word'), make_standard_name(name), name,
                                class, type, 0,
                                CASE WHEN op in ('in', 'near') THEN op ELSE null END
                           FROM (VALUES %s) as v(name, class, type, op))""",
                    to_add)

            if to_delete:
                psycopg2.extras.execute_values(
                    cur,
                    """ DELETE FROM word USING (VALUES %s) as v(name, in_class, in_type, op)
                        WHERE word = name and class = in_class and type = in_type
                              and ((op = '-' and operator is null) or op = operator)""",
                    to_delete)

        LOG.info("Total phrases: %s. Added: %s. Deleted: %s",
                 len(norm_phrases), len(to_add), len(to_delete))


    def add_country_names(self, country_code, names):
        """ Add names for the given country to the search index.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO word (word_id, word_token, country_code)
                   (SELECT nextval('seq_word'), lookup_token, %s
                      FROM (SELECT ' ' || make_standard_name(n) as lookup_token
                            FROM unnest(%s)n) y
                      WHERE NOT EXISTS(SELECT * FROM word
                                       WHERE word_token = lookup_token and country_code = %s))
                """, (country_code, names, country_code))


    def process_place(self, place):
        """ Determine tokenizer information about the given place.

            Returns a JSON-serialisable structure that will be handed into
            the database via the token_info field.
        """
        token_info = _TokenInfo(self._cache)

        names = place.get('name')

        if names:
            token_info.add_names(self.conn, names)

            country_feature = place.get('country_feature')
            if country_feature and re.fullmatch(r'[A-Za-z][A-Za-z]', country_feature):
                self.add_country_names(country_feature.lower(), list(names.values()))

        address = place.get('address')

        if address:
            hnrs = []
            addr_terms = []
            for key, value in address.items():
                if key == 'postcode':
                    self._add_postcode(value)
                elif key in ('housenumber', 'streetnumber', 'conscriptionnumber'):
                    hnrs.append(value)
                elif key == 'street':
                    token_info.add_street(self.conn, value)
                elif key == 'place':
                    token_info.add_place(self.conn, value)
                elif not key.startswith('_') and \
                     key not in ('country', 'full'):
                    addr_terms.append((key, value))

            if hnrs:
                token_info.add_housenumbers(self.conn, hnrs)

            if addr_terms:
                token_info.add_address_terms(self.conn, addr_terms)

        return token_info.data


    def _add_postcode(self, postcode):
        """ Make sure the normalized postcode is present in the word table.
        """
        def _create_postcode_from_db(pcode):
            with self.conn.cursor() as cur:
                cur.execute('SELECT create_postcode_id(%s)', (pcode, ))

        if re.search(r'[:,;]', postcode) is None:
            self._cache.postcodes.get(postcode.strip().upper(), _create_postcode_from_db)


class _TokenInfo:
    """ Collect token information to be sent back to the database.
    """
    def __init__(self, cache):
        self.cache = cache
        self.data = {}


    def add_names(self, conn, names):
        """ Add token information for the names of the place.
        """
        with conn.cursor() as cur:
            # Create the token IDs for all names.
            self.data['names'] = cur.scalar("SELECT make_keywords(%s)::text",
                                            (names, ))


    def add_housenumbers(self, conn, hnrs):
        """ Extract housenumber information from the address.
        """
        if len(hnrs) == 1:
            token = self.cache.get_housenumber(hnrs[0])
            if token is not None:
                self.data['hnr_tokens'] = token
                self.data['hnr'] = hnrs[0]
                return

        # split numbers if necessary
        simple_list = []
        for hnr in hnrs:
            simple_list.extend((x.strip() for x in re.split(r'[;,]', hnr)))

        if len(simple_list) > 1:
            simple_list = list(set(simple_list))

        with conn.cursor() as cur:
            cur.execute("SELECT (create_housenumbers(%s)).* ", (simple_list, ))
            self.data['hnr_tokens'], self.data['hnr'] = cur.fetchone()


    def add_street(self, conn, street):
        """ Add addr:street match terms.
        """
        def _get_street(name):
            with conn.cursor() as cur:
                return cur.scalar("SELECT word_ids_from_name(%s)::text", (name, ))

        self.data['street'] = self.cache.streets.get(street, _get_street)


    def add_place(self, conn, place):
        """ Add addr:place search and match terms.
        """
        def _get_place(name):
            with conn.cursor() as cur:
                cur.execute("""SELECT (addr_ids_from_name(%s)
                                       || getorcreate_name_id(make_standard_name(%s), ''))::text,
                                      word_ids_from_name(%s)::text""",
                            (name, name, name))
                return cur.fetchone()

        self.data['place_search'], self.data['place_match'] = \
            self.cache.places.get(place, _get_place)


    def add_address_terms(self, conn, terms):
        """ Add additional address terms.
        """
        def _get_address_term(name):
            with conn.cursor() as cur:
                cur.execute("""SELECT addr_ids_from_name(%s)::text,
                                      word_ids_from_name(%s)::text""",
                            (name, name))
                return cur.fetchone()

        tokens = {}
        for key, value in terms:
            tokens[key] = self.cache.address_terms.get(value, _get_address_term)

        self.data['addr'] = tokens


class _LRU:
    """ Least recently used cache that accepts a generator function to
        produce the item when there is a cache miss.
    """

    def __init__(self, maxsize=128, init_data=None):
        self.data = init_data or OrderedDict()
        self.maxsize = maxsize
        if init_data is not None and len(init_data) > maxsize:
            self.maxsize = len(init_data)

    def get(self, key, generator):
        """ Get the item with the given key from the cache. If nothing
            is found in the cache, generate the value through the
            generator function and store it in the cache.
        """
        value = self.data.get(key)
        if value is not None:
            self.data.move_to_end(key)
        else:
            value = generator(key)
            if len(self.data) >= self.maxsize:
                self.data.popitem(last=False)
            self.data[key] = value

        return value


class _TokenCache:
    """ Cache for token information to avoid repeated database queries.

        This cache is not thread-safe and needs to be instantiated per
        analyzer.
    """
    def __init__(self, conn):
        # various LRU caches
        self.streets = _LRU(maxsize=256)
        self.places = _LRU(maxsize=128)
        self.address_terms = _LRU(maxsize=1024)

        # Lookup houseunumbers up to 100 and cache them
        with conn.cursor() as cur:
            cur.execute("""SELECT i, ARRAY[getorcreate_housenumber_id(i::text)]::text
                           FROM generate_series(1, 100) as i""")
            self._cached_housenumbers = {str(r[0]) : r[1] for r in cur}

        # Get postcodes that are already saved
        postcodes = OrderedDict()
        with conn.cursor() as cur:
            cur.execute("""SELECT word FROM word
                           WHERE class ='place' and type = 'postcode'""")
            for row in cur:
                postcodes[row[0]] = None
        self.postcodes = _LRU(maxsize=32, init_data=postcodes)

    def get_housenumber(self, number):
        """ Get a housenumber token from the cache.
        """
        return self._cached_housenumbers.get(number)
