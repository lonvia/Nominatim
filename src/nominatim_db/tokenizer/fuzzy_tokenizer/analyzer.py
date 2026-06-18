# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Analyzer which works with full word tokens as ints and partial tokens as str.
"""
from typing import Optional, Iterable, Any
import dataclasses
from collections import defaultdict

from ...db.connection import connect, Connection
from ..base import AbstractAnalyzer
from ...config import Configuration
from ...data.place_info import PlaceInfo
from ...data.place_name import PlaceName, PlaceNames
from .name_processor import FuzzyNameProcessor, FuzzyTokens, FuzzyNames
from . import types as ttyp

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

    def update_special_phrases(self, phrases: Iterable[tuple[str, str, str, str]],
                               should_replace: bool) -> None:
        # TODO: implement
        pass

    def add_country_names(self, country_code: str, names: PlaceNames) -> None:
        """ Add internal names of countries. These will always available,
            independently of what is imported via OSM.
        """
        assert self.conn is not None
        self.name_proc.update_country_names(
            country_code,
            [self.name_proc.normalize_place_name(n) for n in names],
            True, self.conn)

    def process_place(self, place: PlaceInfo) -> Any:
        assert self.conn is not None
        if place.searchable_names:
            names = [self.name_proc.normalize_place_name(n) for n in place.searchable_names]
            analyzed = self.name_proc.apply_variants(ttyp.TOKEN_WORD, names, self.conn)
            token_info = _TokenInfo(full_names=analyzed.tokens,
                                    partials=analyzed.partials)

            if place.is_country():
                assert place.country_code is not None
                self.name_proc.update_country_names(place.country_code, names,
                                                    False, self.conn)
        else:
            token_info = _TokenInfo(set(), set())


        if place.searchable_address:
            address_names: dict[str, FuzzyNames] = defaultdict(list)
            for item in place.searchable_address:
                if item.kind == 'postcode':
                    token_info.postcode = self.normalize_postcode(item.name)
                elif item.kind == 'housenumber':
                    norm, tokens = self._housenumber_to_tokens(item)
                    token_info.housenumbers.add(norm)
                    token_info.hnr_tokens.update(tokens)
                elif item.kind == 'street':
                    tokens = self._lookup_street_tokens(item)
                    if token_info.street is None:
                        token_info.street = tokens
                    else:
                        token_info.street.update(tokens)
                else:
                    address_names[item.kind].append(self.name_proc.normalize_place_name(item))

            for k, names in address_names.items():
                token_info.address[k] = self._create_address_part_info(names)

        return token_info.get_dict()

    def _housenumber_to_tokens(self, name: PlaceName) -> tuple[str, Iterable[int]]:
        """ Computes the normalized name and token ID(s) for the given house number.

            Numeric house numbers up to 9999 have a fixed token ID that
            corresponds to the house number. This should avoid expensive
            lookups for a majority of house numbers.
        """
        if len(name.name) <= 4 and name.name.isdecimal():
            return name.name, [int(name.name)]

        norm = self.name_proc.normalize_place_name(name)

        assert self.conn is not None
        return norm.token, self.name_proc.apply_variants(ttyp.TOKEN_HOUSENUMBER,
                                                         [norm], self.conn).tokens

    def _lookup_street_tokens(self, name: PlaceName) -> set[int]:
        """ Look up all tokens that cover a given name.

            addr:street requires a matching street. Thus the full name
            must already have been indexed.
        """
        norm = self.name_proc.normalize_place_name(name)
        assert self.conn is not None
        return self.name_proc.lookup_tokens(ttyp.TOKEN_WORD, norm, self.conn)

    def _create_address_part_info(self, names: FuzzyNames) -> dict[str, Any]:
        assert self.conn is not None
        lookup: set[int] = set()
        for n in names:
            lookup.update(self.name_proc.lookup_tokens(ttyp.TOKEN_WORD, n, self.conn))
        analysed = self.name_proc.apply_variants(ttyp.TOKEN_WORD, names, self.conn)

        return {
            'match': _mk_array(lookup),
            'full': _mk_array(analysed.tokens),
            'part': ' '.join(analysed.partials)
        }


def _mk_array(tokens: Iterable[int]) -> str:
    """ Create an int array string suitable for PostgreSOL array input.
    """
    return '{' + ','.join((str(s) for s in tokens)) + '}'


@dataclasses.dataclass
class _TokenInfo:
    # Token IDs for the full names of the place
    full_names: set[int]
    # Partial tokens as normalized strings
    partials: set[str]
    # Postcode, normalized with special function
    postcode: Optional[str] = None
    # Normalized house numbers
    housenumbers: set[str] = dataclasses.field(default_factory=set)
    # Token IDs of house numbers
    hnr_tokens: set[int] = dataclasses.field(default_factory=set)
    # Token IDs for full name matches on street name
    street: Optional[set[int]] = None
    # Token details for address parts by their function
    address: dict[str, dict[str, Any]] = dataclasses.field(default_factory=dict)

    def get_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}

        if self.full_names:
            out['full'] = _mk_array(self.full_names)
        if self.partials:
            out['part'] = ' '.join(self.partials)
        if self.postcode:
            out['pc'] = self.postcode
        if self.housenumbers:
            out['hnr'] = ';'.join(self.housenumbers)
        if self.hnr_tokens:
            out['hnrt'] = _mk_array(self.hnr_tokens)
        if self.street is not None:
            out['st'] = _mk_array(self.street)
        if self.address:
            out['addr'] = self.address

        return out
