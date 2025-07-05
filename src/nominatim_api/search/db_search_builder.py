# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2025 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Conversion from token assignment to an abstract DB search.
"""
from typing import Optional, List, Tuple, Iterator, Dict, Any
import heapq

from ..types import SearchDetails, DataLayer
from . import query as qmod
from .token_assignment import TokenAssignment
from . import db_search_fields as dbf
from . import db_searches as dbs
from . import db_search_lookups as lookups


def wrap_near_search(categories: List[Tuple[str, str]],
                     search: dbs.AbstractSearch) -> dbs.NearSearch:
    """ Create a new search that wraps the given search in a search
        for near places of the given category.
    """
    return dbs.NearSearch(penalty=search.penalty,
                          categories=dbf.WeightedCategories(categories,
                                                            [0.0] * len(categories)),
                          search=search)


def build_poi_search(category: List[Tuple[str, str]],
                     countries: Optional[List[str]]) -> dbs.PoiSearch:
    """ Create a new search for places by the given category, possibly
        constraint to the given countries.
    """
    if countries:
        ccs = dbf.WeightedStrings(countries, [0.0] * len(countries))
    else:
        ccs = dbf.WeightedStrings([], [])

    class _PoiData(dbf.SearchData):
        penalty = 0.0
        qualifiers = dbf.WeightedCategories(category, [0.0] * len(category))
        countries = ccs

    return dbs.PoiSearch(_PoiData())


def _split_for_lookup_index(tokens: List[Tuple[int, Any]], limit: int) -> int:
    """ Takes a list of tuples where the first tuple member is a count
        and suggests how many items to use for index lookup.
        The list is expected to be sorted by counts.

        The idea here is that we use as few tokens as possible for the
        index lookup and avoid tokens with high counts.

        The function will return -1 if the items don't look to be suitable
        for index lookup at all.
    """
    length = len(tokens)
    min_count = tokens[0][0]
    if min_count == 1:
        return min(length, 3)  # no statistics available, use index

    for i in range(min(length, 3)):
        ilimit = (limit * (10**i))
        if i > 0 and tokens[i][0] > 10 * min_count:
            return i if min_count < ilimit else -1
        if min_count < ilimit:
            return i + 1

    return -1


class SearchBuilder:
    """ Build the abstract search queries from token assignments.
    """

    def __init__(self, query: qmod.QueryStruct, details: SearchDetails) -> None:
        self.query = query
        self.details = details

    @property
    def configured_for_country(self) -> bool:
        """ Return true if the search details are configured to
            allow countries in the result.
        """
        return self.details.min_rank <= 4 and self.details.max_rank >= 4 \
            and self.details.layer_enabled(DataLayer.ADDRESS)

    @property
    def configured_for_postcode(self) -> bool:
        """ Return true if the search details are configured to
            allow postcodes in the result.
        """
        return self.details.min_rank <= 5 and self.details.max_rank >= 11\
            and self.details.layer_enabled(DataLayer.ADDRESS)

    @property
    def configured_for_housenumbers(self) -> bool:
        """ Return true if the search details are configured to
            allow addresses in the result.
        """
        return self.details.max_rank >= 30 \
            and self.details.layer_enabled(DataLayer.ADDRESS)

    def build(self, assignment: TokenAssignment) -> Iterator[dbs.AbstractSearch]:
        """ Yield all possible abstract searches for the given token assignment.
        """
        sdata = self.get_search_data(assignment)
        if sdata is None:
            return

        near_items = self.get_near_items(assignment)
        if near_items is not None and not near_items:
            return  # impossible combination of near items and category parameter

        if assignment.name is None:
            if near_items and not sdata.postcodes:
                sdata.qualifiers = near_items
                near_items = None
                builder = self.build_poi_search(sdata)
            elif assignment.housenumber:
                hnr_tokens = self.query.get_tokens(assignment.housenumber,
                                                   qmod.TOKEN_HOUSENUMBER)
                builder = self.build_housenumber_search(sdata, hnr_tokens, assignment.address)
            else:
                builder = self.build_special_search(sdata, assignment.address,
                                                    bool(near_items))
        elif not assignment.housenumber and not assignment.address:
            builder = self.build_name_search(sdata, assignment.name)
        else:
            builder = self.build_address_search(sdata, assignment.name, assignment.address,
                                                bool(near_items))

        if near_items:
            penalty = min(near_items.penalties)
            near_items.penalties = [p - penalty for p in near_items.penalties]
            for search in builder:
                search_penalty = search.penalty
                search.penalty = 0.0
                yield dbs.NearSearch(penalty + assignment.penalty + search_penalty,
                                     near_items, search)
        else:
            for search in builder:
                search.penalty += assignment.penalty
                yield search

    def build_poi_search(self, sdata: dbf.SearchData) -> Iterator[dbs.AbstractSearch]:
        """ Build abstract search query for a simple category search.
            This kind of search requires an additional geographic constraint.
        """
        if not sdata.housenumbers \
           and ((self.details.viewbox and self.details.bounded_viewbox) or self.details.near):
            yield dbs.PoiSearch(sdata)

    def build_special_search(self, sdata: dbf.SearchData,
                             address: List[qmod.TokenRange],
                             is_category: bool) -> Iterator[dbs.AbstractSearch]:
        """ Build abstract search queries for searches that do not involve
            a named place.
        """
        if sdata.qualifiers:
            # No special searches over qualifiers supported.
            return

        if sdata.countries and not address and not sdata.postcodes \
           and self.configured_for_country:
            yield dbs.CountrySearch(sdata)

        if sdata.postcodes and (is_category or self.configured_for_postcode):
            penalty = 0.0 if sdata.countries else 0.1
            if address:
                sdata.lookups = [dbf.FieldLookup('nameaddress_vector',
                                                 [t.token for r in address
                                                  for t in self.query.iter_partials(r)],
                                                 lookups.Restrict)]
            yield dbs.PostcodeSearch(penalty, sdata)

    def build_housenumber_search(self, sdata: dbf.SearchData, hnrs: List[qmod.Token],
                                 address: List[qmod.TokenRange]) -> Iterator[dbs.AbstractSearch]:
        """ Build a simple address search for special entries where the
            housenumber is the main name token.
        """
        sdata.lookups = [dbf.FieldLookup('name_vector', [t.token for t in hnrs], lookups.LookupAny)]
        expected_count = sum(t.count for t in hnrs)

        partials = [(t.addr_count, t.token) for trange in address
                    for t in self.query.iter_partials(trange)]

        if not partials:
            # can happen when none of the partials is indexed
            return

        # Counting oddness: if the count is 1 assume that no statistics are
        # available for this item and avoid for index use.
        partials.sort(key=lambda t: t[0] if t[0] > 1 else 100000)
        split = _split_for_lookup_index(partials, 30000)

        if (1 < partials[0][0] < expected_count or expected_count >= 10000) and split > 0:
            sdata.lookups.extend(dbf.lookup_split('nameaddress_vector', partials, split))
        elif expected_count < 10000:
            sdata.lookups.append(dbf.FieldLookup('nameaddress_vector',
                                                 [t[1] for t in partials], lookups.Restrict))
        else:
            addr_fulls = [t.token for t
                          in self.query.get_tokens(address[0], qmod.TOKEN_WORD)]
            if len(addr_fulls) > 5:
                return
            sdata.lookups.append(
                dbf.FieldLookup('nameaddress_vector', addr_fulls, lookups.LookupAny))
            sdata.lookups.append(dbf.FieldLookup('nameaddress_vector',
                                                 [t[1] for t in partials], lookups.Restrict))

        yield dbs.PlaceSearch(0.05, sdata, expected_count, True)

    def build_name_search(self, sdata: dbf.SearchData,
                          name: qmod.TokenRange) -> Iterator[dbs.AbstractSearch]:
        """ Build abstract search queries for simple name searches.
        """
        ranking = self.get_name_ranking(name)
        name_penalty = ranking.normalize_penalty()
        if ranking.rankings:
            sdata.rankings.append(ranking)

        name_partials = [(t.count, t.token) for t in self.query.iter_partials(name)]
        assert name_partials
        # Counting oddness: if the count is 1 assume that no statistics are
        # available for this item and avoid for index use.
        name_partials.sort(key=lambda t: t[0] if t[0] > 1 else 100000)

        split = _split_for_lookup_index(name_partials, 30000)

        if split > 0:
            sdata.lookups = dbf.lookup_split('name_vector', name_partials, split)
            yield dbs.PlaceSearch(name_penalty, sdata,
                                  name_partials[0][0] / (5**(split - 1)), False)
        else:
            # lots of results expected: try lookup by full names first
            name_fulls = list(filter(lambda t: t.count < 10000,
                                     self.query.get_tokens(name, qmod.TOKEN_WORD)))
            if name_fulls:
                fulls_count = sum(t.count for t in name_fulls)
                sdata.lookups = dbf.lookup_by_any_name([t.token for t in name_fulls], [], [])
                yield dbs.PlaceSearch(name_penalty, sdata, fulls_count, False)
            # penalty for the standard lookup by partials
            name_penalty += 0.5

            # look the name up by its partials
            exp_count = name_partials[0][0] / (5**(len(name_partials) - 1))
            if exp_count < 50000:
                sdata.lookups = dbf.lookup_by_names([t[1] for t in name_partials], [])
                yield dbs.PlaceSearch(name_penalty, sdata, exp_count, False)

    def build_address_search(self, sdata: dbf.SearchData,
                             name: qmod.TokenRange, address: List[qmod.TokenRange],
                             is_category: bool) -> Iterator[dbs.AbstractSearch]:
        """ Build abstract search queries for simple address searches.
        """
        if is_category or not sdata.housenumbers or self.configured_for_housenumbers:
            ranking = self.get_name_ranking(name)
            name_penalty = ranking.normalize_penalty()
            if ranking.rankings:
                sdata.rankings.append(ranking)
            for penalty, count, lookup in self.yield_lookups(name, address):
                sdata.lookups = lookup
                if sdata.housenumbers:
                    yield dbs.AddressSearch(penalty + name_penalty, sdata, count, bool(address))
                else:
                    yield dbs.PlaceSearch(penalty + name_penalty, sdata, count, True)

    def yield_lookups(self, name: qmod.TokenRange, address: List[qmod.TokenRange]
                      ) -> Iterator[Tuple[float, int, List[dbf.FieldLookup]]]:
        """ Yield all variants how the given name and address should best
            be searched for. This takes into account how frequent the terms
            are and tries to find a lookup that optimizes index use.
        """
        penalty = 0.0

        name_partials = [(t.count, t.token) for t in self.query.iter_partials(name)]
        assert name_partials
        name_partials.sort(key=lambda t: t[0] if t[0] > 1 else 100000)
        name_split = _split_for_lookup_index(name_partials, 20000)

        addr_partials = list({(t.addr_count, t.token)
                              for r in address for t in self.query.iter_partials(r)})
        if addr_partials:
            addr_partials.sort(key=lambda t: t[0] if t[0] > 1 else 100000)
            addr_split = _split_for_lookup_index(addr_partials, 20000)
        else:
            addr_split = 0

        print(name_partials, name_split, addr_partials, addr_split)
        if name_split < 0 and addr_split < 0:
            # Partial term too frequent. Try looking up by rare full names first.
            name_fulls = self.query.get_tokens(name, qmod.TOKEN_WORD)
            if name_fulls:
                fulls_count = sum(t.count for t in name_fulls)

                if fulls_count < 80000:
                    yield 0.0, fulls_count, \
                        dbf.lookup_by_any_name([t.token for t in name_fulls],
                                               [t[1] for t in addr_partials],
                                               [])
                    penalty += 0.4
            name_split = _split_for_lookup_index(name_partials, 50000)
            if addr_partials:
                addr_split = _split_for_lookup_index(addr_partials, 50000)

        if name_split > 0 and (addr_split <= 0 or name_partials[0][0] <= addr_partials[0][0]):
            # lookup by name
            lookup = dbf.lookup_split('name_vector', name_partials, name_split)
            if addr_partials:
                lookup.append(dbf.FieldLookup('nameaddress_vector',
                                              [t[1] for t in addr_partials],
                                              lookups.Restrict))
            yield penalty, name_partials[0][0] / (5**(name_split - 1)), lookup
        elif addr_split > 0:
            lookup = dbf.lookup_split('nameaddress_vector', addr_partials, addr_split)
            lookup.append(dbf.FieldLookup('name_vector',
                                          [t[1] for t in name_partials],
                                          lookups.Restrict))
            yield penalty, addr_partials[0][0] / (5**(addr_split - 1)), lookup
        else:
            penalty += 0.3
            # To catch remaining results, lookup by name and address
            # We only do this if there is a reasonable number of results expected.
            if addr_partials:
                exp_count = min(name_partials[0][0], addr_partials[0][0])
            else:
                exp_count = name_partials[0][0]
            exp_count = int(exp_count / (min(3, len(name_partials)) + min(3, len(addr_partials))))
            if exp_count < 50000:
                lookup = dbf.lookup_split('name_vector', name_partials, 3)
                if addr_partials:
                    lookup.extend(dbf.lookup_split('nameaddress_vector', addr_partials, 3))

                yield penalty, exp_count, lookup

    def get_name_ranking(self, trange: qmod.TokenRange,
                         db_field: str = 'name_vector') -> dbf.FieldRanking:
        """ Create a ranking expression for a name term in the given range.
        """
        name_fulls = self.query.get_tokens(trange, qmod.TOKEN_WORD)
        ranks = [dbf.RankedTokens(t.penalty, [t.token]) for t in name_fulls]
        ranks.sort(key=lambda r: r.penalty)
        # Fallback, sum of penalty for partials
        default = sum(t.penalty for t in self.query.iter_partials(trange)) + 0.2
        return dbf.FieldRanking(db_field, default, ranks)

    def get_addr_ranking(self, trange: qmod.TokenRange) -> dbf.FieldRanking:
        """ Create a list of ranking expressions for an address term
            for the given ranges.
        """
        todo: List[Tuple[int, int, dbf.RankedTokens]] = []
        heapq.heappush(todo, (0, trange.start, dbf.RankedTokens(0.0, [])))
        ranks: List[dbf.RankedTokens] = []

        while todo:
            neglen, pos, rank = heapq.heappop(todo)
            # partial node
            partial = self.query.nodes[pos].partial
            if partial is not None:
                if pos + 1 < trange.end:
                    penalty = rank.penalty + partial.penalty \
                              + PENALTY_WORDCHANGE[self.query.nodes[pos + 1].btype]
                    heapq.heappush(todo, (neglen - 1, pos + 1,
                                   dbf.RankedTokens(penalty, rank.tokens)))
                else:
                    ranks.append(dbf.RankedTokens(rank.penalty + partial.penalty,
                                                  rank.tokens))
            # full words
            for tlist in self.query.nodes[pos].starting:
                if tlist.ttype == qmod.TOKEN_WORD:
                    if tlist.end < trange.end:
                        chgpenalty = PENALTY_WORDCHANGE[self.query.nodes[tlist.end].btype]
                        for t in tlist.tokens:
                            heapq.heappush(todo, (neglen - 1, tlist.end,
                                                  rank.with_token(t, chgpenalty)))
                    elif tlist.end == trange.end:
                        ranks.extend(rank.with_token(t, 0.0) for t in tlist.tokens)

            if len(ranks) >= 10:
                # Too many variants, bail out and only add
                # Worst-case Fallback: sum of penalty of partials
                default = sum(t.penalty for t in self.query.iter_partials(trange)) + 0.2
                ranks.append(dbf.RankedTokens(rank.penalty + default, []))
                # Bail out of outer loop
                break

        ranks.sort(key=lambda r: len(r.tokens))
        default = ranks[0].penalty + 0.3
        del ranks[0]
        ranks.sort(key=lambda r: r.penalty)

        return dbf.FieldRanking('nameaddress_vector', default, ranks)

    def get_search_data(self, assignment: TokenAssignment) -> Optional[dbf.SearchData]:
        """ Collect the tokens for the non-name search fields in the
            assignment.
        """
        sdata = dbf.SearchData()
        sdata.penalty = assignment.penalty
        if assignment.country:
            tokens = self.get_country_tokens(assignment.country)
            if not tokens:
                return None
            sdata.set_strings('countries', tokens)
        elif self.details.countries:
            sdata.countries = dbf.WeightedStrings(self.details.countries,
                                                  [0.0] * len(self.details.countries))
        if assignment.housenumber:
            sdata.set_strings('housenumbers',
                              self.query.get_tokens(assignment.housenumber,
                                                    qmod.TOKEN_HOUSENUMBER))
        if assignment.postcode:
            sdata.set_strings('postcodes',
                              self.query.get_tokens(assignment.postcode,
                                                    qmod.TOKEN_POSTCODE))
        if assignment.qualifier:
            tokens = self.get_qualifier_tokens(assignment.qualifier)
            if not tokens:
                return None
            sdata.set_qualifiers(tokens)
        elif self.details.categories:
            sdata.qualifiers = dbf.WeightedCategories(self.details.categories,
                                                      [0.0] * len(self.details.categories))

        if assignment.address:
            if not assignment.name and assignment.housenumber:
                # housenumber search: the first item needs to be handled like
                # a name in ranking or penalties are not comparable with
                # normal searches.
                sdata.set_ranking([self.get_name_ranking(assignment.address[0],
                                                         db_field='nameaddress_vector')]
                                  + [self.get_addr_ranking(r) for r in assignment.address[1:]])
            else:
                sdata.set_ranking([self.get_addr_ranking(r) for r in assignment.address])
        else:
            sdata.rankings = []

        return sdata

    def get_country_tokens(self, trange: qmod.TokenRange) -> List[qmod.Token]:
        """ Return the list of country tokens for the given range,
            optionally filtered by the country list from the details
            parameters.
        """
        tokens = self.query.get_tokens(trange, qmod.TOKEN_COUNTRY)
        if self.details.countries:
            tokens = [t for t in tokens if t.lookup_word in self.details.countries]

        return tokens

    def get_qualifier_tokens(self, trange: qmod.TokenRange) -> List[qmod.Token]:
        """ Return the list of qualifier tokens for the given range,
            optionally filtered by the qualifier list from the details
            parameters.
        """
        tokens = self.query.get_tokens(trange, qmod.TOKEN_QUALIFIER)
        if self.details.categories:
            tokens = [t for t in tokens if t.get_category() in self.details.categories]

        return tokens

    def get_near_items(self, assignment: TokenAssignment) -> Optional[dbf.WeightedCategories]:
        """ Collect tokens for near items search or use the categories
            requested per parameter.
            Returns None if no category search is requested.
        """
        if assignment.near_item:
            tokens: Dict[Tuple[str, str], float] = {}
            for t in self.query.get_tokens(assignment.near_item, qmod.TOKEN_NEAR_ITEM):
                cat = t.get_category()
                # The category of a near search will be that of near_item.
                # Thus, if search is restricted to a category parameter,
                # the two sets must intersect.
                if (not self.details.categories or cat in self.details.categories)\
                   and t.penalty < tokens.get(cat, 1000.0):
                    tokens[cat] = t.penalty
            return dbf.WeightedCategories(list(tokens.keys()), list(tokens.values()))

        return None


PENALTY_WORDCHANGE = {
    qmod.BREAK_START: 0.0,
    qmod.BREAK_END: 0.0,
    qmod.BREAK_PHRASE: 0.0,
    qmod.BREAK_SOFT_PHRASE: 0.0,
    qmod.BREAK_WORD: 0.1,
    qmod.BREAK_PART: 0.2,
    qmod.BREAK_TOKEN: 0.4
}
