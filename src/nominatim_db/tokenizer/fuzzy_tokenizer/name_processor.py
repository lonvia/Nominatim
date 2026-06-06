# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Encapsulates word processing for the fuzzy tokenizer.
"""

import icu

from .config import FuzzyTokenizerConfig
from ...data.place_name import PlaceNames

class FuzzyNameProcessor:
    """ Provides functions for normalizing and tokenizing names.

        This processor is not thread-safe.
    """

    def __init__(self, config: FuzzyTokenizerConfig) -> None:
        # Ignore boundary types for name analysis
        self.normalizer = icu.Transliterator.createFromRules(
            "fuzzy_normalization",
            config.normalization_rules + ";[[:Space:][-:]]+ > ' ';")
        self.breaker = icu.RuleBasedBreakIterator(config.breaker_rules)
        self.transliteration = icu.Transliterator.createFromRules(
            "fuzzy_transliteration",
            config.transliteration_rules)

    def normalize(self, name: str) -> str:
        """ Runs normalization and word-breaking on the input name.
        """
        normed = self.normalizer.transliterate(name)
        self.breaker.setText(normed)
        lastpos = 0
        parts = []
        while (bnd := self.breaker.nextBoundary()) >= 0:
            if not (part := normed[lastpos:bnd]).is_space():
                parts.append(part)
            lastpos = bnd

        return ' '.join(parts)


    def normalize_name_list(self, names: PlaceNames) -> None:
        for name in names:
            name.name = self.normalize(name.name)
