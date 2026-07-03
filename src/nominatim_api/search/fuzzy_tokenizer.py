# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Implementation of query analysis for the Fuzzy tokenizer.
"""
from typing import Optional, Any, cast, Iterator
import dataclasses
import re
from itertools import zip_longest

import icu
import sqlalchemy as sa

from ..logging import log
from ..typing import SaRow
from ..sql.sqlalchemy_types import Json
from . import query as qmod
from .query_analyzer_factory import AbstractQueryAnalyzer
from .postcode_parser import PostcodeParser
from ..connection import SearchConnection

DBCFG_NORM_RULES = "tokenizer_normalisation_rules"
DBCFG_TRANS_RULES = "tokenizer_transliteration_rules"
DBCFG_BREAKER_RULES = "tokenizer_breaker_rules"

DB_TO_TOKEN_TYPE = {
    'W': qmod.TOKEN_WORD,
    'H': qmod.TOKEN_HOUSENUMBER,
    'C': qmod.TOKEN_COUNTRY
}

PENALTY_BREAK = {
     qmod.BREAK_START: -0.5,
     qmod.BREAK_END: -0.5,
     qmod.BREAK_PHRASE: -0.5,
     qmod.BREAK_SOFT_PHRASE: -0.5,
     qmod.BREAK_WORD: 0.1,
     qmod.BREAK_PART: 0.2,
     qmod.BREAK_TOKEN: 0.4
}

BREAK_CHARS = qmod.BREAK_PART + qmod.BREAK_WORD + qmod.BREAK_SOFT_PHRASE

TOKEN_SPLIT_RE = re.compile(f"([{BREAK_CHARS}])")

@dataclasses.dataclass
class CategoryInfo:
    classtype: tuple[str, str]
    operator: Optional[str]

# Lookup from token ID to country code
fuzzy_country_tokens: dict[int, str] = {}
# Lookup from token ID to category
fuzzy_category_tokens: dict[int, CategoryInfo] = {}

MISSING_CATEGORY_INFO = CategoryInfo(('place', 'yes'), '')

@dataclasses.dataclass
class FuzzyToken(qmod.Token):
    """ Specialised token for the Fuzzy tokenizer.
    """

    def get_category(self) -> tuple[str, str]:
        return fuzzy_category_tokens.get(self.token, MISSING_CATEGORY_INFO).classtype

    def get_country(self) -> str:
        return fuzzy_country_tokens.get(self.token, '')

    @staticmethod
    def from_db_row(row: SaRow) -> 'FuzzyToken':
        penalty = 0.0
        if row.type == 'W':
            if len(row.word) == 1 and row.word == row.src:
                penalty += 0.2 if row.word.isdigit() else 0.3
        elif row.type == 'H':
            penalty += sum(0.1 for c in row.word_token if c != ' ' and not c.isdigit())
            if all(not c.isdigit() for c in row.word):
                penalty += 0.2 * (len(row.word) - 1)
        elif row.type == 'C':
            if len(row.word) == 1:
                penalty += 0.3

        return FuzzyToken(penalty=penalty, token=row.word_id,
                          count=row.name_count, addr_count=row.address_count,
                          lookup_word=row.src)


@dataclasses.dataclass
class FuzzyAnalyzerConfig:
    postcode_parser: PostcodeParser
    normalizer: icu.Transliterator
    breaker: icu.RuleBasedBreakIterator
    transliterator: icu.Transliterator

    @staticmethod
    async def create(conn: SearchConnection) -> 'FuzzyAnalyzerConfig':
        rules = await conn.get_property(DBCFG_NORM_RULES)
        normalizer = icu.Transliterator.createFromRules("normalization", rules)

        rules = await conn.get_property(DBCFG_BREAKER_RULES)
        breaker = icu.RuleBasedBreakIterator(rules)

        rules = await conn.get_property(DBCFG_TRANS_RULES)
        transliterator = icu.Transliterator.createFromRules("transliteration", rules)

        return FuzzyAnalyzerConfig(PostcodeParser(conn.config), normalizer,
                                   breaker, transliterator)


class FuzzyQueryAnalyzer(AbstractQueryAnalyzer):

    def __init__(self, conn: SearchConnection, config: FuzzyAnalyzerConfig) -> None:
        self.conn = conn
        self.postcode_parser = config.postcode_parser
        self.normalizer = config.normalizer
        self.breaker = config.breaker
        self.transliterator = config.transliterator

    def normalize_text(self, text: str) -> str:
        """ Bring the given text into a normalized form. That is the
            standardized form search will work with. All information removed
            at this stage is inevitably lost.
        """
        normed = cast(str, self.normalizer.transliterate(text))
        self.breaker.setText(normed)
        lastpos = 0
        parts: list[str] = []
        current_break_type = 0
        while (bnd := self.breaker.nextBoundary()) >= 0:
            part = normed[lastpos:bnd]
            if len(part) == 1 and part[0] in BREAK_CHARS:
                current_break_type = max(current_break_type, BREAK_CHARS.index(part[0]))
            else:
                if parts:
                    parts.append(BREAK_CHARS[current_break_type])
                parts.append(part)
                current_break_type = 0
            lastpos = bnd

        return ''.join(parts)

    async def analyze_query(self, phrases: list[qmod.Phrase]) -> qmod.QueryStruct:
        """ Analyze the given phrases and return the tokenized query.
        """
        log().section('Analyze query (using Fuzzy tokenizer)')
        phrases = list(filter(lambda p: p.text,
                              (qmod.Phrase(p.ptype, self.normalize_text(p.text))
                               for p in phrases)))
        query = qmod.QueryStruct(phrases)

        log().var_dump('Normalized query', query.source)
        if not query.source:
            return query

        self._split_query(query)
        log().var_dump('Transliterated query',
                       lambda: ''.join(f"{n.btype}{n.term_lookup}" for n in query.nodes)
                               + ' / '
                               + ''.join(f"{n.btype}{n.term_normalized}" for n in query.nodes))
        words = query.extract_words()

        for row in await self._lookup_in_db(list(words.keys())):
            for trange in words[row.word]:
                # Create a new token for each position because the token
                # penalty can vary depending on the position in the query.
                # (See rerank_tokens() below.)
                token = FuzzyToken.from_db_row(row)
                log().var_dump('Token', token)
                if row.type == 'S':
                    catinfo = fuzzy_category_tokens.get(row.word_id, MISSING_CATEGORY_INFO)
                    if catinfo.operator in ('in', 'near'):
                        if trange.start == 0:
                            query.add_token(trange, qmod.TOKEN_NEAR_ITEM, token)
                    else:
                        if trange.start == 0 and trange.end == query.num_token_slots():
                            query.add_token(trange, qmod.TOKEN_NEAR_ITEM, token)
                        else:
                            query.add_token(trange, qmod.TOKEN_QUALIFIER, token)
                else:
                    query.add_token(trange, DB_TO_TOKEN_TYPE[row.type], token)

        self._add_extra_tokens(query)
        self._rerank_tokens(query)
        # Set the break penalties for the nodes in the query.
        for node in query.nodes:
            node.penalty = PENALTY_BREAK[node.btype]

        log().table_dump('Word tokens', _dump_word_tokens(query))

        return query

    def _split_query(self, query: qmod.QueryStruct) -> None:
        """ Transliterate the phrases and split them into tokens.
        """
        breakchar: Optional[str] = qmod.BREAK_START
        for phrase in query.source:
            for word in TOKEN_SPLIT_RE.split(phrase.text):
                if breakchar is None:
                    breakchar = word
                else:
                    if word:
                        if trans := self.transliterator.transliterate(word):
                            query.add_node(breakchar, phrase.ptype, trans, word)
                    breakchar = None
            breakchar = qmod.BREAK_PHRASE

        query.add_node(qmod.BREAK_END, qmod.PHRASE_ANY)

    async def _lookup_in_db(self, words: list[str]) -> 'sa.Result[Any]':
        """ Return the token information from the database for the
            given word tokens.

            This function excludes postcode tokens
        """
        t = self.conn.t.meta.tables['word']
        return await self.conn.execute(t.select().where(t.c.word.in_(words)))

    def _add_extra_tokens(self, query: qmod.QueryStruct) -> None:
        """ Add tokens to query that are not saved in the database.
        """
        # numerical housenumbers up to 9999
        candidate: Optional[str] = None
        for i, node in enumerate(query.nodes):
            if node.btype in (qmod.BREAK_TOKEN, qmod.BREAK_PART):
                candidate = None
            else:
                if candidate is not None:
                    query.add_token(qmod.TokenRange(i - 1, i), qmod.TOKEN_HOUSENUMBER,
                                    FuzzyToken(penalty=0.0, token=int(candidate),
                                               count=1, addr_count=1,
                                               lookup_word=candidate))
                if len(node.term_normalized) <= 4 and node.term_normalized.isdecimal():
                    candidate = node.term_normalized
                else:
                    candidate = None

        # postcodes
        for start, end, pc in self.postcode_parser.parse(query):
            query.add_token(qmod.TokenRange(start, end),
                            qmod.TOKEN_POSTCODE,
                            FuzzyToken(penalty=0.0, token=0, count=1, addr_count=1,
                                       lookup_word=pc))

    def _rerank_tokens(self, query: qmod.QueryStruct) -> None:
        """ Add penalties to tokens that depend on presence of other token.
        """
        for start, end, tlist in query.iter_tokens_by_edge():
            if len(tlist) > 1:
                # If it looks like a Postcode, give preference.
                if qmod.TOKEN_POSTCODE in tlist:
                    for ttype, tokens in tlist.items():
                        if ttype != qmod.TOKEN_POSTCODE and \
                               (ttype != qmod.TOKEN_HOUSENUMBER or
                                start + 1 > end or
                                len(query.nodes[end].term_lookup) > 4):
                            for token in tokens:
                                token.penalty += 0.39
                        if (start + 1 == end):
                            query.nodes[end].term_penalty += 0.39

                # If it looks like a simple housenumber, prefer that.
                if qmod.TOKEN_HOUSENUMBER in tlist:
                    hnr_lookup = tlist[qmod.TOKEN_HOUSENUMBER][0].lookup_word
                    if len(hnr_lookup) <= 3 and any(c.isdigit() for c in hnr_lookup):
                        penalty = 0.5 - tlist[qmod.TOKEN_HOUSENUMBER][0].penalty
                        for ttype, tokens in tlist.items():
                            if ttype != qmod.TOKEN_HOUSENUMBER:
                                for token in tokens:
                                    token.penalty += penalty
                        if (start + 1 == end):
                            query.nodes[end].term_penalty += penalty

            # rerank tokens against the normalized form
            norm = ''.join(f"{n.term_normalized}{'' if n.btype == qmod.BREAK_TOKEN else ' '}"
                           for n in query.nodes[start + 1:end + 1]).strip()
            for ttype, tokens in tlist.items():
                for token in tokens:
                    token.penalty += token.match_penalty(norm) * \
                                        (1 if ttype == qmod.TOKEN_WORD else 2)



async def create_query_analyzer(conn: SearchConnection) -> AbstractQueryAnalyzer:
    """ Create and set up a new query analyzer for a database based
        on the fuxxy tokenizer.
    """
    async def _get_config() -> FuzzyAnalyzerConfig:
        if 'word' not in conn.t.meta.tables:
            sa.Table('word', conn.t.meta,
                     sa.Column('word_id', sa.Integer, nullable=False),
                     sa.Column('type', sa.Text, nullable=False),
                     sa.Column('word', sa.Text, nullable=False),
                     sa.Column('src', sa.Text, nullable=False),
                     sa.Column('name_count', sa.Integer, nullable=False),
                     sa.Column('address_count', sa.Integer, nullable=False))

        if 'token_source' not in conn.t.meta.tables:
            # ignore all columns that are not required for search
            t = sa.Table('token_source', conn.t.meta,
                         sa.Column('id', sa.Integer, nullable=False),
                         sa.Column('type', sa.Text, nullable=False),
                         sa.Column('token', sa.Text, nullable=False),
                         sa.Column('info', Json))

            if not fuzzy_country_tokens:
                sql = sa.select(t.c.id, t.c.token).where(t.c.type == 'C')

                for row in await conn.execute(sql):
                    fuzzy_country_tokens[row.id] = row.token

            if not fuzzy_category_tokens:
                sql = sa.select(t.c.id, t.c.token, t.c.info).where(t.c.type == 'S')

                for row in await conn.execute(sql):
                    fuzzy_category_tokens[row.id] = \
                        CategoryInfo(tuple(row.token.split('.', 2)),
                                     row.info.get('op', ''))


        return await FuzzyAnalyzerConfig.create(conn)

    config = await conn.get_cached_value('FUZZYTOK', 'config', _get_config)

    return FuzzyQueryAnalyzer(conn, config)


def _dump_word_tokens(query: qmod.QueryStruct) -> Iterator[list[Any]]:
    yield ['type', 'from', 'to', 'token', 'word', 'src', 'penalty', 'name_count', 'addr_count', 'extra']
    for i, node in enumerate(query.nodes):
        if node.term_lookup:
            yield [qmod.TOKEN_PARTIAL, i, i + 1, '-', node.term_lookup,
                   node.term_normalized, '-', '-', '-', '']
    for i, node in enumerate(query.nodes):
        for tlist in node.starting:
            lookup = ' '.join(query.iter_partials_trans(qmod.TokenRange(i, tlist.end)))
            for t in tlist.tokens:
                if tlist.ttype == qmod.TOKEN_COUNTRY:
                    extra = f"country code: {fuzzy_country_tokens.get(t.token, '')}"
                elif tlist.ttype in (qmod.TOKEN_QUALIFIER, qmod.TOKEN_NEAR_ITEM):
                    info = fuzzy_category_tokens.get(t.token, MISSING_CATEGORY_INFO)
                    extra = f"category: {info.classtype[0]}.{info.classtype[1]}, " \
                            f"operator: {info.operator}"
                else:
                    extra = ''
                yield [tlist.ttype, i, tlist.end, t.token, lookup,
                       t.lookup_word or '', t.penalty, t.count, t.addr_count, extra]

