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
from ..base import AbstractAnalyzer
from ...config import Configuration
from ...data.place_info import PlaceInfo

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

        if place.searchable_names:
            full_tokens = self._compute_name_tokens(place.searchable_names)
            token_info.set_names(self._compute_name_tokens(place.searchable_names))

            # TODO add to country names table

        if place.searchable_address:
            self._process_place_address(token_info, place.searchable_address)

        return token_info.get_dict()

    def _compute_name_tokens(self, names: Sequence[PlaceName]) -> dict[int, str]:
        """ Process the given names and return a dictionary of
            word token to word.
        """


def _mk_array(tokens: Iterable[Any]) -> str:
    """ Create an array string suitable for Postgres array input.
    """
    return '{' + ','.join((str(s) for s in tokens)) + '}'


class _TokenInfo:
    """ Collect the token information being sent back to the database.
    """
    def __init__(self) -> None:
        # Token IDs for the full names of the place
        self.full_names: set[int] = set()
        # Partial tokens as normalized strings
        self.partials: set[str] = set()

    def get_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}

        if self.full_names:
            out['full_names'] = _mk_array(self.full_names)
        if self.partials:
            out['partials'] = list(self.partials)

        return out
