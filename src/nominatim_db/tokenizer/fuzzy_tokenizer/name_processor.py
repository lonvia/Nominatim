# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Encapsulates word processing for the fuzzy tokenizer.
"""
from typing import Optional, Iterable, cast
import logging
import icu
import dataclasses
from collections import defaultdict

from psycopg import sql as pysql

from . import config
from ...errors import UsageError
from ...data.place_name import PlaceNames, PlaceName
from ...db.connection import Connection

LOG = logging.getLogger()

TOKEN_TYPES = {
    'name': 0,
    'housenumber': 1,
    'postcode': 2
}


@dataclasses.dataclass
class FuzzyToken:
    token_id: int
    token: str


FuzzyTokens = list[FuzzyToken]


@dataclasses.dataclass
class AnalyzedWord:
    tokens: set[int]
    partials: set[str]


@dataclasses.dataclass
class FuzzyName:
    name_attr: dict[str, str]
    token: str
    attr_idxs: set[int] = cast(set[int], frozenset())


FuzzyNames = list[FuzzyName]
# Cache key: tuple of token base name and an attribute tuple
TokenCacheKey = tuple[str, tuple[Optional[str], ...]]

class VariantProcessor:

    def __init__(self, vconfig: config.FuzzyVariantConfig, normalizer: icu.Transliterator) -> None:
        pass

    def get_filter_attributes(self) -> tuple[tuple[str, str], ...]:
        return tuple()

    def process_name(self, name: str) -> list[str]:
        return []


class LexicalProcessor(VariantProcessor):
    # TODO: implement
    pass

class MutationProcessor(VariantProcessor):
    # TODO: implement
    pass


class FilteredVariantProcessor:

    def __init__(self, proc: VariantProcessor, filt_attr: tuple[str, ...]) -> None:
        self.proc = proc
        self.filters = tuple((filt_attr.index(a), val) for a, val in proc.get_filter_attributes())
        self.filter_idxs = {f[0] for f in self.filters}


    def process(self, attr_values: tuple[Optional[str], ...], in_names: FuzzyNames) -> FuzzyNames:
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

    def _save_by_name(self, conn: Connection, token: str, attr_keys: tuple[str, ...],
                      attr_values: tuple[Optional[str], ...],
                      names: FuzzyNames) -> FuzzyTokens:
        """ Adds the token to the database if it doesn't exist and sets
            the token ID saved in the DB for all names.
        """
        attr_idxs: set[int] = set()
        for name in names:
            attr_idxs.union(name.attr_idxs)
        attr_dict = {attr_keys[i]: attr_values[i] for i in attr_idxs
                     if attr_values[i] is not None}

        variants = {n.token for n in names}
        variants.discard(token)
        sql_variant_list = sorted(variants) if variants else None
        variants.add(token)

        with conn.cursor() as cur:
            cur.execute("""INSERT INTO token_source (type, token, attributes, variants)
                             VALUES (%s, %s, %s, %s)
                             ON CONFLICT (type, token, attributes)
                             DO NOTHING
                             RETURNING OLD.id as old_id, NEW.id as new_id, OLD.variants
                        """,
                        ('W', token, attr_dict if attr_dict else None, sql_variant_list))
            result = cur.fetchone()
            if result is None:
                return []

            old_id, tuple_id, old_variants = result
            if old_id is None:
                self._insert_tokens_into_word(conn, tuple_id, 'W', variants)
            else:
                assert old_variants is None or isinstance(old_variants, list)
                if old_variants != sql_variant_list:
                    if old_variants is None:
                        old_variants = [token]
                    else:
                        old_variants.append(token)
                    self._update_tokens_in_word(conn, tuple_id, 'W', old_variants, variants)

        return [FuzzyToken(tuple_id, v) for v in variants]

    def _save_by_attribute(self, conn: Connection, token: str, attr_keys: tuple[str, ...],
                           attr_values: tuple[Optional[str], ...],
                           names: FuzzyNames) -> FuzzyTokens:
        """ Add the token to the database giving each attribute combination
            a different token ID.
        """
        # First group variants by attribute types
        subtokens: dict[tuple[int, ...], FuzzyNames] = defaultdict(list)
        for name in names:
            subtokens[tuple(sorted(name.attr_idxs))].append(name)

        variantlist = []
        for idxs, subnames in subtokens.items():
            attr_dict = {attr_keys[i]: attr_values[i] for i in idxs
                         if attr_values[i] is not None}
            variants = {n.token for n in subnames}
            if variants:
                variantlist.append(('W', token, attr_dict if attr_dict else None,
                                    sorted(variants)))

        results: FuzzyTokens = []
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO token_source (type, token, attributes, variants)
                             VALUES (%s, %s, %s, %s)
                             ON CONFLICT (type, token, attributes)
                             DO NOTHING
                             RETURNING OLD.id as old_id, NEW.id as new_id, OLD.variants
                """, variantlist, returning=True)

            for i, (old_id, tuple_id, old_variants) in enumerate(cur.results()):
                new_variants = variantlist[i][3]
                if old_id is None:
                    self._insert_tokens_into_word(conn, tuple_id, 'W', new_variants)
                else:
                    assert old_variants is None or isinstance(old_variants, list)
                    if old_variants != new_variants:
                        self._update_tokens_in_word(conn, tuple_id, 'W',
                                                    old_variants, new_variants)
                results.extend(FuzzyToken(tuple_id, v) for v in new_variants)

        return results

    def _insert_tokens_into_word(self, conn: Connection, word_id: int,
                                 wtype: str, tokens: Iterable[str],
                                 counts: tuple[int, int] = (1, 1) ) -> None:
        """ Insert the given normalized words into the word table.

            The token list is assumed to be unique.
        """
        params = []
        for token in tokens:
            trans = ' '.join(self.get_word_partials(token))
            if trans:
                params.append((word_id, wtype, trans, token, *counts))

        if params:
            with conn.cursor() as cur:
                cur.executemany("""INSERT INTO word (word_id, type, word, src,
                                                     name_count, address_count)
                                     VALUES (%s, %s, %s, %s, %s, %s)
                                """, params)

    def _update_tokens_in_word(self, conn: Connection, word_id: int,
                                 wtype: str, old_tokens: Optional[Iterable[str]],
                                 new_tokens: Optional[Iterable[str]]) -> None:
        """ The list of words for a certain word type has changed.
            Update the entries in word accordingly.
        """
        with conn.cursor() as cur:
            cur.execute("""UPDATE token_source SET variants = %s
                           WHERE id = %s AND variants != %s""")
            # It is possible here that another thread was faster here updating
            # the words. Thus only modify the word table when the token_source
            # table was actually changed.
            if cur.rowcount > 0:
                # Be lazy here and simply replace all values.
                # Carry over name and address counts, though.
                counts = (1, 1)
                if old_tokens:
                    cur.execute("""DELETE FROM word WHERE word_id = %s
                                   RETURNING name_count, address_count""", (word_id, ))
                    if cur.rowcount > 0:
                        counts = cast(tuple[int, int], cur.fetchone())
                if new_tokens:
                    self._insert_tokens_into_word(conn, word_id, wtype, new_tokens,
                                                  counts=counts)

    def __init__(self, config: config.FuzzyTokenizerConfig) -> None:
        if config.word_grouping == 'by-name':
            self.save_by_group = self._save_by_name
        else:
            self.save_by_group = self._save_by_attribute
        # Ignore boundary types for name analysis
        self.normalizer = icu.Transliterator.createFromRules(
            "fuzzy_normalization",
            config.normalization_rules + ";[[:Space:][-:]]+ > ' ';")
        self.breaker = icu.RuleBasedBreakIterator(config.breaker_rules)
        self.transliteration = icu.Transliterator.createFromRules(
            "fuzzy_transliteration",
            config.transliteration_rules + ";[[:Space:][-:]]+ > '';")

        self._create_variant_processors(config.variant_rules)

        self._cache: list[dict[TokenCacheKey, AnalyzedWord]] = \
            [{} for _ in range(max(TOKEN_TYPES.values()))]

    def _create_variant_processors(self, in_rules: list[config.FuzzyVariantConfig]) -> None:
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

    def get_word_partials(self, normalized_name: str) -> Iterable[str]:
        return filter(None, (self.transliteration.transliterate(s).strip()
                             for s in normalized_name.split()))

    def normalize_place_name(self, name: PlaceName) -> FuzzyName:
        """ Takes a list of PlaceName items and converts it into the
            internally used FuzzyName list, normalizing the names
            on the way.
        """
        return FuzzyName(name_attr=name.attr, token=self.normalize(name.name))

    def apply_variants(self, token_type: str, names: FuzzyNames, conn: Connection) -> AnalyzedWord:
        """ Apply variant processing for the given type of tokens to
            the name list and return the extended name list.
        """
        token_type_id = TOKEN_TYPES[token_type]
        varprocs = self.variant_processors[token_type_id]
        attr_keys = self.filter_attributes[token_type_id]
        cache = self._cache[token_type_id]

        out_tokens: set[int] = set()
        out_partials: set[str] = set()
        for name in names:
            attr_values = tuple(name.name_attr.get(a) for a in attr_keys)
            cache_key = (name.token, attr_values)
            if (cached := cache.get(cache_key)) is not None:
                analysed = cached
            else:
                variants = [name]
                for varproc in varprocs:
                    variants = varproc.process(attr_values, variants)

                tokens = self.save_by_group(conn, name.token, attr_keys, attr_values, variants)
                analysed = AnalyzedWord({t.token_id for t in tokens}, self.create_partials(tokens))
                cache[cache_key] = analysed

            out_tokens.update(analysed.tokens)
            out_partials.update(analysed.partials)

        return AnalyzedWord(out_tokens, out_partials)

    def create_partials(self, tokens: FuzzyTokens) -> set[str]:
        partials: set[str] = set()
        for token in tokens:
            partials.update(s.strip() for s in token.token.split())

        trans = {self.transliteration.transliterate(s) for s in partials}

        partials.update(trans)
        partials.discard('')

        return partials
