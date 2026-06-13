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
import dataclasses

from ...db.connection import connect, Connection
from ..base import AbstractAnalyzer
from ...config import Configuration
from ...data.place_info import PlaceInfo
from ...data.place_name import PlaceName, PlaceNames
from .name_processor import FuzzyNameProcessor, FuzzyTokens

class FuzzyAnalyzer(AbstractAnalyzer):

    def __init__(self, dsn: str, name_processor: FuzzyNameProcessor) -> None:
        self.conn: Optional[Connection] = connect(dsn)
        self.conn.autocommit = True
        self.name_proc = name_processor

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_word_token_info(self, words: list[str]) -> list[tuple[str, str, Optional[int]]]:
        # TODO: implement
        return []

    def normalize_postcode(self, postcode: str) -> str:
        return postcode.strip().upper()

    def update_postcodes_from_db(self) -> None:
        pass # not needed

    def update_special_phrases(self,
                               phrases: Iterable[tuple[str, str, str, str]],
                               should_replace: bool) -> None:
        pass

    def add_country_names(self, country_code: str, names: PlaceNames) -> None:
        pass

    def process_place(self, place: PlaceInfo) -> Any:
        assert self.conn is not None
        if place.searchable_names:
            names = [self.name_proc.normalize_place_name(n) for n in place.searchable_names]
            analyzed = self.name_proc.apply_variants('name', names, self.conn)
            token_info = _TokenInfo(full_names=analyzed.tokens,
                                    partials=analyzed.partials)

            # TODO add to country names table
        else:
            token_info = _TokenInfo()


        if place.searchable_address:
            for item in place.searchable_address:
                if item.kind == 'postcode':
                    token_info.postcode = self.normalize_postcode(item.name)
                elif item.kind == 'housenumber':
                    token_info.housenumbers.update(self._housenumber_to_tokens(item))
                elif item.kind == 'street':
                    norm = self.name_proc.normalize_place_name(item)
                    token_info.street.update(
                        self.name_proc.lookup_tokens('name', norm, self.conn))


        return token_info.get_dict()

    def _housenumber_to_tokens(self, name: PlaceName) -> Iterable[int]:
        """ Computes the token ID(s) for the given house number.

            Numeric house numbers up to 9999 have a fixed token ID that
            corresponds to the house number. This should avoid expensive
            lookups for a majority of house numbers.
        """
        if len(name.name) <= 4 and name.name.isdecimal():
            return [int(name.name)]

        norm = self.name_proc.normalize_place_name(name)

        assert self.conn is not None
        return self.name_proc.apply_variants('housenumber', [norm], self.conn).tokens

    def _lookup_street_tokens(self, name: PlaceName) -> Iterable[int]:
        """ Look up all tokens that cover a given name.

            addr:street requires a matching street. Thus the full name
            must already have been indexed.
        """


def _mk_array(tokens: Iterable[int]) -> str:
    """ Create an int array string suitable for Postgres array input.
    """
    return '{' + ','.join((str(s) for s in tokens)) + '}'


@dataclasses.dataclass
class _TokenInfo:
    # Token IDs for the full names of the place
    full_names: set[int] = set()
    # Partial tokens as normalized strings
    partials: set[str] = set()
    # Postcode, normalized with special function
    postcode: Optional[str] = None
    # Token IDs of housenumbers
    housenumbers: set[int] = dataclasses.field(default_factory=set)


    def get_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}

        if self.full_names:
            out['full'] = _mk_array(self.full_names)
        if self.partials:
            out['part'] = ' '.join(self.partials)
        if self.postcode:
            out['pc'] = self.postcode
        if self.housenumbers:
            out['hnr'] = _mk_array(self.housenumbers)

        return out
