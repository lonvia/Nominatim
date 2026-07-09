# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Encapsulates word processing for the fuzzy tokenizer.
"""
from typing import Optional, Iterable, cast, Any
import logging
import icu
import dataclasses
import re
from collections import defaultdict

from psycopg import sql as pysql

from . import config
from ...errors import UsageError
from ...data.place_name import PlaceNames, PlaceName
from ...db.connection import Connection
from . import types as ttyp
from .variants_lexical import LexicalProcessor
from .variants_mutate import MutationProcessor

LOG = logging.getLogger()

TOKEN_TYPES = {
    ttyp.TOKEN_WORD: 0,
    ttyp.TOKEN_HOUSENUMBER: 1,
    ttyp.TOKEN_COUNTRY: 2
}

NORM_BREAK_REGEX = re.compile('[ :-]')

# Cache key: tuple of token base name and an attribute tuple
TokenCacheKey = tuple[str, tuple[Optional[str], ...]]


class VariantProcessorInfo:

    def __init__(self, rules: dict[str, Any], proc: ttyp.VariantProcessor) -> None:
        self.proc = proc

        self.filters: dict[str, str] = {}
        for k, v in rules.items():
            if k not in proc.RESERVED_RULE_KEYS:
                if not isinstance(v, (str, int)):
                    raise UsageError('Variant attributes must be strings')
                self.filters[k] = str(v)

    def get_filter_values(self, attr_keys: tuple[str, ...]) -> list[Optional[str]]:
        return [self.filters.get(a) for a in attr_keys]


class VariantTree:

    def __init__(self, attributes: list[list[Optional[str]]]) -> None:
        self.default: Any
        self.nodes: dict[str, Any] = {}

        if not attributes:
            self.default = []
        elif len(attributes[0]) == 1:
            self.default = []
            for attr in {a[0] for a in attributes if a[0] is not None}:
                self.nodes[attr] = []
        else:
            self.default = VariantTree([a[1:] for a in attributes])
            for attr in {a[0] for a in attributes if a[0] is not None}:
                self.nodes[attr] = VariantTree([a[1:] for a in attributes if a[0] == attr or a[0] is None])

    def add_processor(self, attr_values: list[Optional[str]], proc: ttyp.VariantProcessor) -> None:
        attr = attr_values[0]
        if not attr_values:
            self.default.append(proc)
        elif len(attr_values) == 1:
            assert isinstance(self.default, list)
            if attr is None:
                self.default.append(proc)
                for proclist in self.nodes.values():
                    proclist.append(proc)
            else:
                self.nodes[attr].append(proc)
        else:
            remain = attr_values[1:]
            assert isinstance(self.default, VariantTree)
            if attr is None:
                self.default.add_processor(remain, proc)
                for proclist in self.nodes.values():
                    proclist.add_processor(remain, proc)
            else:
                self.nodes[attr].add_processor(remain, proc)

    def get(self, attr_values: tuple[Optional[str], ...], pos: int) -> list[ttyp.VariantProcessor]:
        if not attr_values:
            return cast(list[ttyp.VariantProcessor], self.default)

        next_node = self.nodes.get(attr_values[pos], self.default)  # type: ignore[arg-type]
        if pos + 1 == len(attr_values):
            return cast(list[ttyp.VariantProcessor], next_node)

        return cast(VariantTree, next_node).get(attr_values, pos + 1)

    def __bool__(self) -> bool:
        return bool(self.nodes)


class TokenTypeData:

    def __init__(self, varprocs: list[VariantProcessorInfo]) -> None:
        attr_set: set[str] = set()
        for vp in varprocs:
            attr_set.update(vp.filters.keys())
        self.filter_attributes = tuple(sorted(attr_set))

        self.variant_processors = VariantTree(
            [vp.get_filter_values(self.filter_attributes) for vp in varprocs])
        for vp in varprocs:
            filter_values = vp.get_filter_values(self.filter_attributes)
            vp.proc.set_filter_idxs({i for i, v in enumerate(filter_values)
                                     if v is not None})
            self.variant_processors.add_processor(filter_values, vp.proc)

        self.analyse_cache: dict[TokenCacheKey, ttyp.AnalyzedWord] = {}
        self.token_lookup_cache: dict[str, set[int]] = {}

    def apply_variants(self, attr_values: tuple[Optional[str], ...], name: ttyp.FuzzyName) -> ttyp.FuzzyNames:
        in_names = [name]

        vp_chain = self.variant_processors.get(attr_values, 0)
        if not vp_chain:
            return in_names

        for vp in vp_chain:
            out_names = []
            for name in in_names:
                if (variants := vp.process_name(name.token)) is not None:
                    for variant in variants:
                        if variant == name.token:
                            out_names.append(name)
                        else:
                            # A real variant. Tag our attributes.
                            out_names.append(
                                ttyp.FuzzyName(name.name_attr, variant,
                                          attr_idxs=name.attr_idxs | vp.filter_idxs))
                else:
                    out_names.append(name)
            in_names = out_names

        return in_names

class FuzzyNameProcessor:
    """ Provides functions for normalizing and tokenizing names.

        This processor is not thread-safe.
    """

    def _save_by_name(self, conn: Connection, token: str, token_type: str,
                      attr_keys: tuple[str, ...],
                      attr_values: tuple[Optional[str], ...],
                      names: ttyp.FuzzyNames) -> ttyp.FuzzyTokens:
        """ Adds the token to the database if it doesn't exist and sets
            the token ID saved in the DB for all names.
        """
        attr_idxs: set[int] = set()
        for name in names:
            attr_idxs.update(name.attr_idxs)
        attr_dict = {attr_keys[i]: attr_values[i] for i in attr_idxs
                     if attr_values[i] is not None}

        variants = {n.token for n in names}
        variants.discard(token)
        sql_variant_list = sorted(variants) if variants else None
        variants.add(token)

        with conn.cursor() as cur:
            cur.execute("SELECT (getorcreate_token_source(%s, %s, %s, %s)).*",
                        (token_type, token, attr_dict, sql_variant_list))
            result = cur.fetchone()
            if result is None:
                return []

            created, tuple_id, old_variants = result
            if created:
                self._insert_tokens_into_word(conn, tuple_id, token_type, variants)
            else:
                assert old_variants is None or isinstance(old_variants, list)
                if old_variants != sql_variant_list:
                    if old_variants is None:
                        old_variants = [token]
                    else:
                        old_variants.append(token)
                    self._update_tokens_in_word(conn, tuple_id, token_type, old_variants, variants)

        return [ttyp.FuzzyToken(tuple_id, v) for v in variants]

    def _save_by_attribute(self, conn: Connection, token: str, token_type: str,
                           attr_keys: tuple[str, ...],
                           attr_values: tuple[Optional[str], ...],
                           names: ttyp.FuzzyNames) -> ttyp.FuzzyTokens:
        """ Add the token to the database giving each attribute combination
            a different token ID.
        """
        # First group variants by attribute types
        subtokens: dict[tuple[int, ...], ttyp.FuzzyNames] = defaultdict(list)
        for name in names:
            subtokens[tuple(sorted(name.attr_idxs))].append(name)

        variantlist = []
        for idxs, subnames in subtokens.items():
            attr_dict = {attr_keys[i]: attr_values[i] for i in idxs
                         if attr_values[i] is not None}
            variants = {n.token for n in subnames}
            if variants:
                variantlist.append((token_type, token, attr_dict,
                                    sorted(variants)))

        results: ttyp.FuzzyTokens = []
        with conn.cursor() as cur:
            cur.executemany("SELECT (getorcreate_token_source(%s, %s, %s, %s)).*",
                            variantlist, returning=True)

            for vl in variantlist:
                result = cur.fetchone()
                cur.nextset()
                if result is None:
                    raise RuntimeError('getorcreate_token_source() did not return results.')
                (created, tuple_id, old_variants) = result

                new_variants = vl[3]
                if created:
                    self._insert_tokens_into_word(conn, tuple_id, token_type, new_variants)
                else:
                    assert old_variants is None or isinstance(old_variants, list)
                    if old_variants != new_variants:
                        self._update_tokens_in_word(conn, tuple_id, token_type,
                                                    old_variants, new_variants)
                results.extend(ttyp.FuzzyToken(tuple_id, v) for v in new_variants)

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

    def _delete_tokens_from_word(self, conn: Connection, word_id: int,
                                 wtype: str, tokens: Iterable[str]) -> None:
        """ Delete the given normalized words from the word table.
        """
        # Word type must be given as a literal or the index won't work!
        conn.execute(
            pysql.SQL("DELETE FROM WORD WHERE type = {} and src = ANY(%s)")
                 .format(pysql.Literal(wtype)),
            (list(tokens), ))

    def _update_tokens_in_word(self, conn: Connection, word_id: int,
                                 wtype: str, old_tokens: Optional[Iterable[str]],
                                 new_tokens: Optional[Iterable[str]]) -> None:
        """ The list of words for a certain word type has changed.
            Update the entries in word accordingly.
        """
        with conn.cursor() as cur:
            if new_tokens:
                cur.execute("UPDATE token_source SET variants = %s WHERE id = %s",
                            (list(new_tokens), word_id))
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

        self._create_token_type_data(config.variant_rules)


    def _create_token_type_data(self, in_rules: list[config.FuzzyVariantConfig]) -> None:
        type_range = range(max(TOKEN_TYPES.values()) + 1)
        varprocs: list[list[VariantProcessorInfo]] = [[] for _ in type_range]

        apply_lookup = {v: TOKEN_TYPES[k] for k,v in ttyp.TOKEN_LABELS.items()
                        if k in TOKEN_TYPES}

        for vconfig in in_rules:
            if vconfig.applies_to:
                apply_to = list({apply_lookup[n] for n in vconfig.applies_to
                                if n in apply_lookup})
            else:
                apply_to = list(apply_lookup.values())

            if not apply_to:
                LOG.warn('Variant processor skipped. Unknown applies-to: %s', vconfig.applies_to)
                continue

            proc_class: type[ttyp.VariantProcessor]
            if vconfig.rule_type == 'lexical':
                proc_class = LexicalProcessor
            elif vconfig.rule_type == 'mutation':
                proc_class = MutationProcessor
            else:
                LOG.fatal("Unknown variant processor type '%s'.", vconfig.rule_type)
                raise UsageError('Syntax error in tokenizer configuration file.')

            proc = VariantProcessorInfo(
                    vconfig.rules, proc_class(vconfig.config, vconfig.rules, self.normalizer))
            for nr in apply_to:
                varprocs[nr].append(proc)

        self.token_type_data = [TokenTypeData(vp) for vp in varprocs]

    def normalize(self, name: str) -> str:
        """ Runs normalization and word-breaking on the input name.
        """
        normed = self.normalizer.transliterate(f" {name} ")
        len_normed = len(normed)
        self.breaker.setText(normed)
        lastpos = 0
        parts = []
        while (bnd := self.breaker.nextBoundary()) >= 0:
            if bnd > len_normed:
                bnd = len_normed
            if bnd > lastpos \
                    and (lastpos + 1 > bnd or normed[lastpos] not in (' ', '-', ':')):
                parts.append(normed[lastpos:bnd])
            lastpos = bnd
            if lastpos >= len_normed:
                break

        return ' '.join(parts)

    def get_word_partials(self, normalized_name: str) -> Iterable[str]:
        return filter(None, (self.transliteration.transliterate(s).strip()
                             for s in normalized_name.split()))

    def normalize_place_name(self, name: PlaceName, country_code: Optional[str]) -> ttyp.FuzzyName:
        """ Takes a list of PlaceName items and converts it into the
            internally used FuzzyName list, normalizing the names
            on the way.
        """
        if country_code:
            attr = {'country': country_code}
            attr.update(name.attr)
        else:
            attr = name.attr
        return ttyp.FuzzyName(name_attr=attr, token=self.normalize(name.name))

    def apply_variants(self, token_type: str, names: Iterable[ttyp.FuzzyName], conn: Connection) -> ttyp.AnalyzedWord:
        """ Apply variant processing for the given type of tokens to
            the name list and return the extended name list.

            Partial tokens are only computed for word types.
        """
        tdata = self.token_type_data[TOKEN_TYPES[token_type]]
        with_partials = token_type == ttyp.TOKEN_WORD

        out_tokens: set[int] = set()
        out_partials: set[str] = set()
        for name in names:
            attr_values = tuple(name.name_attr.get(a) for a in tdata.filter_attributes)
            cache_key = (name.token, attr_values)
            if (cached := tdata.analyse_cache.get(cache_key)) is not None:
                analysed = cached
            else:
                variants = tdata.apply_variants(attr_values, name)

                tokens = self.save_by_group(conn, name.token, token_type,
                                            tdata.filter_attributes, attr_values, variants)
                analysed = ttyp.AnalyzedWord({t.token_id for t in tokens},
                                        self.create_partials(tokens) if with_partials else set())
                tdata.analyse_cache[cache_key] = analysed

            out_tokens.update(analysed.tokens)
            if with_partials:
                out_partials.update(analysed.partials)

        return ttyp.AnalyzedWord(out_tokens, out_partials)

    def create_partials(self, tokens: ttyp.FuzzyTokens) -> set[str]:
        partials: set[str] = set()
        for token in tokens:
            partials.update(s.strip() for s in token.token.split())

        trans = {self.transliteration.transliterate(s) for s in partials}

        trans.discard('')

        return trans

    def lookup_tokens(self, token_type: str, norm_name: str, conn: Connection) -> set[int]:
        """ Find all token IDs of the given type that resolve to a normalized
            name as given. Works against variants as well.
        """
        tdata = self.token_type_data[TOKEN_TYPES[token_type]]

        if (tlist := tdata.token_lookup_cache.get(norm_name)) is not None:
            return tlist

        with conn.cursor() as cur:
            cur.execute(pysql.SQL('SELECT word_id FROM word WHERE type = {} AND src = %s')
                             .format(pysql.Literal(token_type)),
                        (norm_name, ))
            tlist = set(cast(int, r[0]) for r in cur)
            tdata.token_lookup_cache[norm_name] = tlist

        return tlist

    def update_country_names(self, country_code: str, names: ttyp.FuzzyNames,
                             internal: bool, conn: Connection) -> None:
        """ Country names are added with two tokens per country: one for internal
            names and one for names imported from OSM.

            Names from OSM are handled as addition: they are only added when
            the same name does not exist internally.
        """
        tdata = self.token_type_data[TOKEN_TYPES[ttyp.TOKEN_COUNTRY]]

        if not tdata.variant_processors:
            # Shortcut: just used the names as given
            variants = {n.token for n in names}
        else:
            # Otherwise do full variant processor processing.
            variants = set()
            for name in names:
                attr_values = tuple(name.name_attr.get(a) for a in tdata.filter_attributes)
                name_vars = tdata.apply_variants(attr_values, name)
                variants.update(t.token for t in name_vars)

        my_attr = {'int': 'yes'} if internal else {}

        with conn.cursor() as cur:
            cur.execute("SELECT (getorcreate_token_source('C', %s, %s, NULL)).*",
                        (country_code, my_attr))
            result = cur.fetchone()
            if result is None:
                raise RuntimeError('Inserting country names did not yield a result.')

            created, my_token_id, _ = result
            if not created:
                cur.execute("SELECT src FROM word WHERE word_id = %s", (my_token_id, ))
                my_old_names = {cast(str, r[0]) for r in cur}
                if variants == my_old_names:
                    # Nothing has changed, no further processing necessary.
                    return
            else:
                my_old_names = set()

            other_attr = {} if internal else {'int': 'yes'}

            cur.execute("SELECT (getorcreate_token_source('C', %s, %s, NULL)).*",
                        (country_code, other_attr))
            result = cur.fetchone()
            if result is None:
                raise RuntimeError('Inserting country names did not yield a result.')

            created, other_token_id, _ = result
            if not created:
                cur.execute("SELECT src FROM word WHERE word_id = %s", (other_token_id, ))
                other_old_names = {cast(str, r[0]) for r in cur}
            else:
                other_old_names = set()

        if internal:
            if (added := variants - my_old_names):
                self._insert_tokens_into_word(conn, my_token_id, 'C', added)
            if (deleted := my_old_names - variants):
                self._delete_tokens_from_word(conn, my_token_id, 'C', deleted)
            # Also remove OSM-generated names that are now duplicats.
            if (other_deleted := other_old_names & variants):
                self._delete_tokens_from_word(conn, other_token_id, 'C', other_deleted)
        else:
            if (added := variants - my_old_names - other_old_names):
                self._insert_tokens_into_word(conn, my_token_id, 'C', added)
            if (deleted := my_old_names - variants):
                self._delete_tokens_from_word(conn, my_token_id, 'C', deleted)


