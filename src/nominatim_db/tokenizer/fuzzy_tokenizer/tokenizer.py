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

from ...db.connection import connect, Connection
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
        pass
