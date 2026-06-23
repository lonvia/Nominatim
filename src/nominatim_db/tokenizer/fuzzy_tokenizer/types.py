# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Type definitions for the Fuzzy tokenizer.
"""
from typing import Optional, cast
from abc import ABC, abstractmethod
import dataclasses

from icu import Transliterator
from .config import FuzzyVariantConfig

TOKEN_WORD = 'W'
TOKEN_HOUSENUMBER = 'H'
TOKEN_COUNTRY = 'C'

TOKEN_LABELS = {
    TOKEN_WORD: 'name',
    TOKEN_HOUSENUMBER: 'housenumber',
    TOKEN_COUNTRY: 'country'
}

@dataclasses.dataclass
class FuzzyToken:
    token_id: int
    token: str


@dataclasses.dataclass
class AnalyzedWord:
    tokens: set[int]
    partials: set[str]


@dataclasses.dataclass
class FuzzyName:
    name_attr: dict[str, str]
    token: str
    attr_idxs: set[int] = cast(set[int], frozenset())


FuzzyTokens = list[FuzzyToken]
FuzzyNames = list[FuzzyName]


class VariantProcessor(ABC):
    RESERVED_RULE_KEYS: tuple[str, ...]

    @abstractmethod
    def process_name(self, name: str) -> Optional[list[str]]:
        pass

    def set_filter_idxs(self, idxs: set[int]) -> None:
        self.filter_set = idxs

    @property
    def filter_idxs(self) -> set[int]:
        return self.filter_set
