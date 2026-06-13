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

from ...db.connection import connect, Connection, drop_tables
from ...db.sql_preprocessor import SQLPreprocessor
from ..base import AbstractAnalyzer, AbstractTokenizer
from ...config import Configuration
from ...data.place_info import PlaceInfo
from .analyzer import FuzzyAnalyzer
from .config import FuzzyTokenizerConfig
from  .name_processor import FuzzyNameProcessor

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
            conn.execute('CREATE EXTENSION IF NOT EXISTS pg_search')
            self._setup_db_tables(conn, config)

    def init_from_project(self, config: Configuration) -> None:
        with connect(self.dsn) as conn:
            self.config = FuzzyTokenizerConfig(config, conn)

    def finalize_import(self, config: Configuration) -> None:
        pass

    def update_sql_functions(self, config: Configuration) -> None:
        pass

    def check_database(self, config: Configuration) -> Optional[str]:
        pass

    def update_statistics(self, config: Configuration, threads: int = 1) -> None:
        pass

    def update_word_tokens(self) -> None:
        pass

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
                id INTEGER GENERATED ALWAYS AS IDENTITY (MINVALUE 10000) PRIMARY KEY,
                type VARCHAR NOT NULL,
                token TEXT NOT NULL,
                attributes HSTORE,
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
                address_count INTEGER NOT NULL DEFAULT 1;

            CREATE INDEX idx_word_word ON word USING btree(word);

            GRANT SELECT ON token_source TO "{{config.DATABASE_WEBUSER}}";
            GRANT SELECT ON word TO "{{config.DATABASE_WEBUSER}}";
        """)
