# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Encapsulates word processing for the fuzzy tokenizer.
"""
from typing import Optional
import logging
import icu
import dataclasses

from ...errors import UsageError
from .config import FuzzyTokenizerConfig, FuzzyVariantConfig
from ...data.place_name import PlaceNames, PlaceName

LOG = logging.getLogger()

TOKEN_TYPES = {
    'name': 0,
    'housenumber': 1,
    'postcode': 2
}


@dataclasses.dataclass
class FuzzyName:
    name_attr: dict[str, str]
    token: str
    token_id: Optional[int] = None
    attr_idxs: set[int] = frozenset()


FuzzyNames = list[FuzzyName]
AttributeValueTuple = tuple[Optional[str], ...]

class VariantProcessor:

    def __init__(self, vconfig: FuzzyVariantConfig, normalizer: icu.Transliterator) -> None:
        pass

    def get_filter_attributes(self) -> tuple[tuple[str, str], ...]:
        return tuple()

    def process_name(self, name: str) -> list[str]:
        return []


class LexicalProcessor(VariantProcessor):
    pass

class MutationProcessor(VariantProcessor):
    pass


class FilteredVariantProcessor:

    def __init__(self, proc: VariantProcessor, filt_attr: tuple[str, ...]) -> None:
        self.proc = proc
        self.filters = tuple((filt_attr.index(a), val) for a, val in proc.get_filter_attributes())
        self.filter_idxs = {f[0] for f in self.filters}


    def process(self, attr_values: AttributeValueTuple, in_names: FuzzyNames) -> FuzzyNames:
        if self.filters and any(attr_values[i] != val for i, val in self.filters):
            return in_names

        out_names = []
        for name in in_names:
            if (variants := self.proc.process_name(name.token)) is not None:
                for variant in variants:
                    if variant == name.token:
                        out_names.append(name)
                    else:
                        # A real variant. Tag our attributes.
                        out_names.append(
                            FuzzyName(name.name_attr, variant,
                                      attr_idxs=name.attr_idxs | self.filter_idxs))
            else:
                out_names.append(name)

        return []

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

        self._name_cache: dict[tuple[str, tuple[str, ...]], FuzzyNames] = {}

    def _create_variant_processors(self, in_rules: list[FuzzyVariantConfig]) -> None:
        varprocs: list[list[VariantProcessor]] = [[] for _ in range(max(TOKEN_TYPES.values()))]

        filtersets: list[set[str]] = [set() for _ in range(max(TOKEN_TYPES.values()))]

        for vconfig in in_rules:
            if vconfig.applies_to:
                apply_to = list({TOKEN_TYPES[n] for n in vconfig.applies_to
                                if n in TOKEN_TYPES})
            else:
                apply_to = list(TOKEN_TYPES.values())

            if not apply_to:
                LOG.warn('Variant processor skipped. Unknown applies-to: %s', vconfig.applies_to)
                continue

            proc: VariantProcessor
            if vconfig.rule_type == 'lexical':
                proc = LexicalProcessor(vconfig, self.normalizer)
            elif vconfig.rule_type == 'mutation':
                proc = MutationProcessor(vconfig, self.normalizer)
            else:
                LOG.fatal("Unknown variant processor type '%s'.", vconfig.rule_type)
                raise UsageError('Syntax error in tokenizer configuration file.')

            for nr in apply_to:
                varprocs[nr].append(proc)
                filtersets[nr].update(k for k, _ in proc.get_filter_attributes())

        self.filter_attributes = [tuple(sorted(fs)) for fs in filtersets]

        self.variant_processors = []
        for vproclist, filt_attr in zip(varprocs, self.filter_attributes):
            self.variant_processors.append(
                [FilteredVariantProcessor(p, filt_attr) for p in vproclist])

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

    def normalized_place_names(self, names: PlaceNames) -> FuzzyNames:
        """ Takes a list of PlaceName items and converts it into the
            internally used FuzzyName list, normalizing the names
            on the way.
        """
        return [FuzzyName(name_attr=n.attr, token=self.normalize(n.name)) for n in names]

    def apply_variants(self, token_type: str, names: FuzzyNames) -> FuzzyNames:
        """ Apply variant processing for the given type of tokens to
            the name list and return the extended name list.
        """
        token_type_id = TOKEN_TYPES[token_type]
        varprocs = self.variant_processors[token_type_id]
        attr_keys = self.filter_attributes[token_type_id]

        out_names: FuzzyNames = []
        for name in names:
            attr_values = tuple(name.name_attr.get(a) for a in attr_keys)
            cache_key = (name, attr_values)
            if (cached := self._name_cache.get(cache_key)) is not None:
                variants = cached
            else:
                # TODO: add caching here
                variants = [name]
                for varproc in varprocs:
                    variants = varproc.process(attr_values, variants)
                # TODO save to word table here
                out_names.extend(variants)

                self._name_cache[cache_key] = variants

        return out_names
