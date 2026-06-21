# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2025 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Conversion from token assignment to an abstract DB search.
"""
from typing import Optional, List, Tuple, Iterator, Dict
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
        else:
            builder = self.build_name_search(sdata, assignment.name, assignment.address,
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
                sdata.lookups = [dbf.FieldLookup('nameaddress_partials',
                                                 [t for r in address
                                                  for t in self.query.iter_partials_trans(r)],
                                                 lookups.PartialLookup)]
            yield dbs.PostcodeSearch(penalty, sdata)

    def build_housenumber_search(self, sdata: dbf.SearchData, hnrs: List[qmod.Token],
                                 address: List[qmod.TokenRange]) -> Iterator[dbs.AbstractSearch]:
        """ Build a simple address search for special entries where the
            housenumber is the main name token.
        """
        hnr_tokens = [t.token for t in hnrs]
        expected_count = sum(t.count for t in hnrs)

        sdata.lookups = [dbf.FieldLookup('name_vector', hnr_tokens, lookups.LookupAny),
                         dbf.FieldLookup('nameaddress_partials',
                                         [t for r in address
                                          for t in self.query.iter_partials_trans(r)],
                                         lookups.PartialLookup)]

        sdata.housenumbers = dbf.WeightedStrings([], [])
        yield dbs.PlaceSearch(0.0, sdata, expected_count, True)

    def build_name_search(self, sdata: dbf.SearchData,
                          name: qmod.TokenRange, address: List[qmod.TokenRange],
                          is_category: bool) -> Iterator[dbs.AbstractSearch]:
        """ Build abstract search queries for simple name or address searches.
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
                    yield dbs.PlaceSearch(penalty + name_penalty, sdata, count, bool(address))

    def yield_lookups(self, name: qmod.TokenRange, address: List[qmod.TokenRange]
                      ) -> Iterator[Tuple[float, int, List[dbf.FieldLookup]]]:
        """ Yield all variants how the given name and address should best
            be searched for. This takes into account how frequent the terms
            are and tries to find a lookup that optimizes index use.
        """
        name_partials = list(self.query.iter_partials_trans(name))
        addr_partials = [t for r in address
                         for t in self.query.iter_partials_trans(r)]

        lookup = [dbf.FieldLookup('name_partials', name_partials, lookups.PartialLookup)]

        if addr_partials:
            lookup.append(
                dbf.FieldLookup('nameaddress_partials', addr_partials, lookups.PartialLookup))

        yield 0.0, 10, lookup

    def get_name_ranking(self, trange: qmod.TokenRange,
                         db_field: str = 'name_vector') -> dbf.FieldRanking:
        """ Create a ranking expression for a name term in the given range.
        """
        name_fulls = self.query.get_tokens(trange, qmod.TOKEN_WORD)
        full_word_penalty = self.query.get_in_word_penalty(trange)
        ranks = [dbf.RankedTokens(t.penalty + full_word_penalty, [t.token])
                 for t in name_fulls]
        ranks.sort(key=lambda r: r.penalty)
        # Fallback, sum of penalty for partials
        default = self.query.get_partial_penalty(trange) + 0.2
        default += sum(n.word_break_penalty
                       for n in self.query.nodes[trange.start + 1:trange.end])
        return dbf.FieldRanking(db_field, default, ranks)

    def get_addr_ranking(self, trange: qmod.TokenRange) -> dbf.FieldRanking:
        """ Create a list of ranking expressions for an address term
            for the given ranges.
        """
        todo: List[Tuple[int, int, dbf.RankedTokens]] = []
        heapq.heappush(todo, (0, trange.start, dbf.RankedTokens(0.0, [])))
        ranks: List[dbf.RankedTokens] = []

        while todo:
            _, pos, rank = heapq.heappop(todo)
            # partial node
            next_node = self.query.nodes[pos + 1]
            if pos + 1 < trange.end:
                penalty = rank.penalty + next_node.term_penalty \
                          + next_node.word_break_penalty
                heapq.heappush(todo, (-(pos + 1), pos + 1,
                               dbf.RankedTokens(penalty, rank.tokens)))
            else:
                ranks.append(dbf.RankedTokens(rank.penalty + next_node.term_penalty,
                                              rank.tokens))
            # full words
            for tlist in self.query.nodes[pos].starting:
                if tlist.ttype == qmod.TOKEN_WORD:
                    if tlist.end < trange.end:
                        chgpenalty = self.query.nodes[tlist.end].word_break_penalty \
                                     + self.query.get_in_word_penalty(
                                            qmod.TokenRange(pos, tlist.end))
                        for t in tlist.tokens:
                            heapq.heappush(todo, (-tlist.end, tlist.end,
                                                  rank.with_token(t, chgpenalty)))
                    elif tlist.end == trange.end:
                        chgpenalty = self.query.get_in_word_penalty(
                                        qmod.TokenRange(pos, tlist.end))
                        ranks.extend(rank.with_token(t, chgpenalty)
                                     for t in tlist.tokens)

            if len(ranks) >= 10:
                # Too many variants, bail out and only add
                # Worst-case Fallback: sum of penalty of partials
                default = self.query.get_partial_penalty(trange) + 0.2
                default += sum(n.word_break_penalty
                               for n in self.query.nodes[trange.start + 1:trange.end])
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
            sdata.set_countries(tokens)
            sdata.penalty += self.query.get_in_word_penalty(assignment.country)
        elif self.details.countries:
            sdata.countries = dbf.WeightedStrings(self.details.countries,
                                                  [0.0] * len(self.details.countries))
        if assignment.housenumber:
            sdata.set_strings('housenumbers',
                              self.query.get_tokens(assignment.housenumber,
                                                    qmod.TOKEN_HOUSENUMBER))
            sdata.penalty += self.query.get_in_word_penalty(assignment.housenumber)
        if assignment.postcode:
            sdata.set_strings('postcodes',
                              self.query.get_tokens(assignment.postcode,
                                                    qmod.TOKEN_POSTCODE))
            sdata.penalty += self.query.get_in_word_penalty(assignment.postcode)
        if assignment.qualifier:
            tokens = self.get_qualifier_tokens(assignment.qualifier)
            if not tokens:
                return None
            sdata.set_qualifiers(tokens)
            sdata.penalty += self.query.get_in_word_penalty(assignment.qualifier)
        elif self.details.categories:
            sdata.qualifiers = dbf.WeightedCategories(self.details.categories,
                                                      [0.0] * len(self.details.categories))

        if assignment.address:
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
            tokens = [t for t in tokens if t.get_country() in self.details.countries]

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
