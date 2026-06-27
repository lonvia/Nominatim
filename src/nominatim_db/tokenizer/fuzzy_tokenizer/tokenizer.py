# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Tokenizer using pg_search to allow for fuzzy search.
"""
from typing import Optional, Iterable, Any
import logging

from psycopg import sql as pysql

from ...db.connection import connect, Connection, drop_tables, table_exists
from ...db.sql_preprocessor import SQLPreprocessor
from ..base import AbstractAnalyzer, AbstractTokenizer
from ...config import Configuration
from ...data.place_info import PlaceInfo
from .analyzer import FuzzyAnalyzer
from .config import FuzzyTokenizerConfig
from  .name_processor import FuzzyNameProcessor
from . import types as ttyp

LOG = logging.getLogger()


def create(dsn: str) -> AbstractTokenizer:
    """ Create a new instance of the tokenizer provided by this module.
    """
    return FuzzyTokenizer(dsn)


class FuzzyTokenizer(AbstractTokenizer):

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.config: Optional[FuzzyTokenizerConfig] = None

    def init_new_db(self, config: Configuration, init_db: bool = True) -> None:
        self.config = FuzzyTokenizerConfig(config)

        with connect(self.dsn) as conn:
            #conn.execute('CREATE EXTENSION IF NOT EXISTS pg_search')
            self.update_sql_functions(config)
            self._setup_db_tables(conn, config)
            self._create_base_indices(config, 'word')
            self.config.save_to_db(conn)

    def init_from_project(self, config: Configuration) -> None:
        with connect(self.dsn) as conn:
            self.config = FuzzyTokenizerConfig(config, conn)

    def finalize_import(self, config: Configuration, threads: int = 1) -> None:
        """ Finalize the word table by creating lookup indexes
            and computing word statistics if necessary.
        """
        self._create_lookup_indices(config)
        self.update_statistics(config, threads)

    def update_sql_functions(self, config: Configuration) -> None:
        """ Reimport the SQL functions for this tokenizer.
        """
        with connect(self.dsn) as conn:
            sqlp = SQLPreprocessor(conn, config)
            sqlp.run_sql_file(conn, 'tokenizer/fuzzy_tokenizer.sql')

    def check_database(self, config: Configuration) -> Optional[str]:
        pass # no other checks at the moment

    def update_statistics(self, config: Configuration, threads: int = 1) -> None:
        """ Recompute frequencies of full words.
        """
        with connect(self.dsn) as conn:
            if not table_exists(conn, 'search_name'):
                return

            conn.execute('ANALYSE search_name')
            if threads > 1:
                    conn.execute(pysql.SQL('SET max_parallel_workers_per_gather TO {}')
                                     .format(pysql.Literal(min(threads, 6),)))

            LOG.info('Computing word frequencies')
            drop_tables(conn, 'word_frequencies')
            conn.execute("""
                CREATE TEMP TABLE word_frequencies AS
                WITH word_freq AS MATERIALIZED (
                       SELECT unnest(name_vector) as id, count(*)
                             FROM search_name GROUP BY id),
                   addr_freq AS MATERIALIZED (
                       SELECT unnest(nameaddress_vector) as id, count(*)
                             FROM search_name GROUP BY id)
                SELECT coalesce(a.id, w.id) as id,
                       w.count as name_count,
                       a.count as addr_count
                FROM word_freq w FULL JOIN addr_freq a ON a.id = w.id;
                """)
            conn.execute('CREATE UNIQUE INDEX ON word_frequencies(id)')
            drop_tables(conn, 'tmp_word')
            conn.execute("CREATE TABLE tmp_word (LIKE word INCLUDING DEFAULTS)")
            conn.execute("""
                INSERT INTO tmp_word (word_id, type, word, src, name_count, address_count)
                  SELECT word_id, type, word, src,
                         COALESCE(wf.name_count, 1),
                         COALESCE(wf.addr_count, 1)
                  FROM word LEFT JOIN word_frequencies wf ON word_id = wf.id
                  """)
            drop_tables(conn, 'word_frequencies')
            conn.execute('SET max_parallel_workers_per_gather TO 0')

            sqlp = SQLPreprocessor(conn, config)
            sqlp.run_string(conn, """
                GRANT SELECT ON tmp_word TO "{{config.DATABASE_WEBUSER}}";
                GRANT SELECT ON token_source TO "{{config.DATABASE_WEBUSER}}"
                """)

            conn.commit()

        self._create_base_indices(config, 'tmp_word')
        self._move_temporary_word_table('tmp_word')

    def update_word_tokens(self) -> None:
        """ Remove unused tokens.
        """
        #TODO: clean housenumbers at least

    def name_analyzer(self) -> AbstractAnalyzer:
        assert self.config is not None
        return FuzzyAnalyzer(self.dsn, FuzzyNameProcessor(self.config))

    def most_frequent_words(self, conn: Connection, num: int) -> list[str]:
        # TODO: implement
        return []

    def _setup_db_tables(self, conn: Connection, config: Configuration) -> None:
        drop_tables(conn, 'word', 'token_source')
        sqlp = SQLPreprocessor(conn, config)
        # token_source contains meta-info on the token.
        # Token IDs up to 9999 are reserved for numeric housenumbers.
        sqlp.run_string(conn, """
            CREATE TABLE token_source (
                id INTEGER GENERATED ALWAYS AS IDENTITY (MINVALUE 10000),
                type VARCHAR NOT NULL,
                token TEXT NOT NULL,
                attributes HSTORE NOT NULL,
                variants TEXT[],
                info JSONB);
            CREATE UNIQUE INDEX idx_token_source_token
                                 ON token_source
                                 USING btree(type, token, attributes);

            CREATE TABLE word (
                word_id INTEGER NOT NULL,
                type VARCHAR NOT NULL,
                word TEXT NOT NULL,
                src TEXT NOT NULL,
                name_count INTEGER NOT NULL DEFAULT 1,
                address_count INTEGER NOT NULL DEFAULT 1);
        """)

    def _create_base_indices(self, config: Configuration, table_name: str) -> None:
        with connect(self.dsn) as conn:
            sqlp = SQLPreprocessor(conn, config)
            sqlp.run_string(conn,
                            """CREATE INDEX idx_{{tbl}}_word
                               ON {{tbl}} USING BTREE (word) {{db.tablespace.search_index}};

                               GRANT SELECT ON {{tbl}} TO "{{config.DATABASE_WEBUSER}}"
                            """, tbl=table_name)
            for token_type, type_name in ttyp.TOKEN_LABELS.items():
                sqlp.run_string(conn,
                                """CREATE INDEX idx_{{tbl}}_{{sub}}
                                     ON {{tbl}} USING BTREE (src) {{db.tablespace.address_index}}
                                     WHERE type = '{{ttype}}'""",
                                tbl=table_name, sub=type_name, ttype=token_type)

    def _create_lookup_indices(self, config: Configuration) -> None:
        with connect(self.dsn) as conn:
            sqlp = SQLPreprocessor(conn, config)
            sqlp.run_string(conn,
                            """CREATE UNIQUE INDEX idx_token_source_id
                               ON token_source USING BTREE (id)
                               {{db.tablespace.search_index}}""")

    def _move_temporary_word_table(self, old: str) -> None:
        """ Rename all tables and indexes used by the tokenizer.
        """
        with connect(self.dsn) as conn:
            drop_tables(conn, 'word')
            with conn.cursor() as cur:
                cur.execute(pysql.SQL("ALTER TABLE {} RENAME TO word")
                                 .format(pysql.Identifier(old)))
                for idx in ['word'] + list(ttyp.TOKEN_LABELS.values()):
                    cur.execute(pysql.SQL("ALTER INDEX {} RENAME TO {}")
                                     .format(pysql.Identifier(f"idx_{old}_{idx}"),
                                             pysql.Identifier(f"idx_word_{idx}")))
            conn.commit()
