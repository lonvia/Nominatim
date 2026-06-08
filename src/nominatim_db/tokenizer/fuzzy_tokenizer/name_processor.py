# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Encapsulates word processing for the fuzzy tokenizer.
"""
import logging
import icu

from ...errors import UsageError
from .config import FuzzyTokenizerConfig, FuzzyVariantConfig
from ...data.place_name import PlaceNames

LOG = logging.getLogger()

TOKEN_TYPES = {
    'name': 0,
    'housenumber': 1,
    'postcode': 2
}


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

        self._create_variant_processors(config.variant_rules)

        self.name_filter_attributes = sorted({r['filter-attribute']
                                              for r in config.get_variants_for('name')
                                              if 'filter-attribute' in r})

    def _create_variant_processors(self, in_rules: list[FuzzyVariantConfig]) -> None:
        self.variant_processors = [[] for _ in range(max(TOKEN_TYPES.values()))]

        for vconfig in in_rules:
            if in_rules.applies_to:
                apply_to = list({TOKEN_TYPES[n] for n in in_rules.applies_to
                                if n in TOKEN_TYPES})
            else:
                apply_to = list(TOKEN_TYPES.values())

            if not apply_to:
                LOG.warn('Variant processor skipped. Unknown applies-to: %s', in_rules.applies_to)
                continue

            if in_rules.rule_type == 'lexical':
                proc = LexicalProcessor(in_rules.config, in_rules.rules,
                                        self.normalizer)
            elif in_rules.rule_type == 'mutation':
                proc = MutationProcessor(in_rules.config, in_rules.rules,
                                         self.normalizer)
            else:
                LOG.fatal("Unknown variant processor type '%s'.", in_rules.rule_type)
                raise UsageError('Syntax error in tokenizer configuration file.')

            for nr in apply_to:
                self.variant_processors[nr].append(proc)


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

    def apply_variants(self, names: PlaceNames) -> None:
