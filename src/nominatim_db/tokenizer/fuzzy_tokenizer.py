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

from ..db.connection import connect, Connection
from .base import AbstractAnalyzer, AbstractTokenizer
from ..config import Configuration
from ..data.place_info import PlaceInfo
from .icu_rule_loader import ICURuleLoader

def create(dsn: str) -> 'FuzzyTokenizer':
    """ Create a new instance of the tokenizer provided by this module.
    """
    return FuzzyTokenizer(dsn)


class FuzzyTokenizer(AbstractTokenizer):

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.loader: Optional[ICURuleLoader] = None

    def init_new_db(self, config: Configuration, init_db: bool = True) -> None:
        self.loader = ICURuleLoader(config)

        with connect(self.dsn) as conn:
            conn.execute('CREATE EXTENSION IF NOT EXISTS pg_search')

    def init_from_project(self, config: Configuration) -> None:
        self.loader = ICURuleLoader(config)

        with connect(self.dsn) as conn:
            self.loader.load_config_from_db(conn)

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
        assert self.loader is not None
        return FuzzyAnalyzer(self.dsn, self.loader.make_sanitizer())

    def most_frequent_words(self, conn: Connection, num: int) -> list[str]:
        pass


class FuzzyAnalyzer(AbstractAnalyzer):

    def __init__(self, dsn: str, sanitizer: PlaceSanitizer) -> None:
        self.conn: Optional[Connection] = connect(dsn)
        self.conn.autocommit = True
        self.sanitizer = sanitizer


    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_word_token_info(self, words: list[str]) -> list[tuple[str, str, Optional[int]]]:
        pass

    def normalize_postcode(self, postcode: str) -> str:
        pass

    def update_postcodes_from_db(self) -> None:
        pass

    def update_special_phrases(self,
                               phrases: Iterable[tuple[str, str, str, str]],
                               should_replace: bool) -> None:
        pass

    def add_country_names(self, country_code: str, names: dict[str, str]) -> None:
        pass

    def process_place(self, place: PlaceInfo) -> Any:
        token_info = _TokenInfo()

        names, address = self.sanitizer.process_names(place)

        if names:
            token_info.set_names(self._compute_name_tokens(names))

        if address:
            self._process_place_address(token_info, address)

        return token_info.get_dict()


class _TokenInfo:
    """ Collect the token information being sent back to the database.
    """
    def __init__(self) -> None:
        self._data: dict[str, Any] = {}

    def get_dict(self) -> dict[str, Any]:
        return self._data
