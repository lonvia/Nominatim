# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Variant processor for lexical variants.
"""
from typing import Optional, Any, NamedTuple, Iterator
from collections import defaultdict
import itertools
import re

from icu import Transliterator

from ...errors import UsageError
from . import types as ttyp
from ..token_analysis.simple_trie import SimpleTrie

class ICUVariant(NamedTuple):
    """ A single replacement rule for variant creation.
    """
    source: str
    replacement: str

class LexicalProcessor(ttyp.VariantProcessor):
    RESERVED_RULE_KEYS = ('words', )

    def __init__(self, config: dict[str, Any], rules: dict[str, Any],
                 normalizer: Transliterator) -> None:
        if 'words' not in rules:
            raise UsageError("Variant processor requires 'words' section.")
        if not isinstance(rules['words'], list) or \
                any(not isinstance(r, str) for r in rules['words']):
            raise UsageError("'words' section in variant processor needs to be list of strings.")
        filters: list[tuple[str, str]] = []
        for k, v in rules.items():
            if k != 'words':
                if not isinstance(v, (str, int)):
                    raise UsageError('Variant attributes must be strings')
                filters.append((k, str(v)))

        self.filters = tuple(sorted(filters))

        vset: set[ICUVariant] = set()
        vmaker = _VariantMaker(normalizer)

        for rule in rules['words']:
            vset.update(vmaker.compute(rule))

        # Intermediate reorder by source.
        immediate = defaultdict(list)
        for variant in vset:
            if variant.source[-1] == ' ' and variant.replacement[-1] == ' ':
                replstr = variant.replacement[:-1]
            else:
                replstr = variant.replacement
            immediate[variant.source].append(replstr)

        self.replacements = SimpleTrie(immediate.items())

    def process_name(self, name: str) -> Optional[list[str]]:
        baseform = '^ ' + name + ' ^'
        baselen = len(baseform)
        partials = ['']

        startpos = 0
        if self.replacements is not None:
            pos = 0
            force_space = False
            while pos < baselen:
                frm = pos
                repl, pos = self.replacements.longest_prefix(baseform, pos)
                if repl is not None:
                    done = baseform[startpos:frm]
                    partials = [v + done + r
                                for v, r in itertools.product(partials, repl)
                                if not force_space or r.startswith(' ')]
                    if len(partials) > 128:
                        # If too many variants are produced, they are unlikely
                        # to be helpful. Only use the original term.
                        startpos = 0
                        break
                    if baseform[pos - 1] == ' ':
                        pos -= 1
                        force_space = True
                    startpos = pos
                else:
                    pos += 1
                    force_space = False

        # No variants detected? Fast return.
        if startpos == 0:
            return None

        if startpos < baselen:
            variants = (part[1:] + baseform[startpos:-1] for part in partials)
        else:
            variants = (part[1:-1].strip() for part in partials)

        return [v for v in {n.strip() for n in variants} if v]


class _VariantMaker:
    """ Generator for all necessary ICUVariants from a single variant rule.

        All text in rules is normalized to make sure the variants match later.
    """

    def __init__(self, normalizer: Any) -> None:
        self.norm = normalizer

    def compute(self, rule: Any) -> Iterator[ICUVariant]:
        """ Generator for all ICUVariant tuples from a single variant rule.
        """
        parts = re.split(r'(\|)?([=-])>', rule)
        if len(parts) != 4:
            raise UsageError(f"Syntax error in variant rule: {rule}")

        decompose = parts[1] is None
        src_terms = [self._parse_variant_word(t) for t in parts[0].split(',')]
        repl_terms = (self.norm.transliterate(t).strip() for t in parts[3].split(','))

        # If the source should be kept, add a 1:1 replacement
        if parts[2] == '-':
            for src in src_terms:
                if src:
                    for froms, tos in _create_variants(*src, src[0], decompose):
                        yield ICUVariant(froms, tos)

        for src, repl in itertools.product(src_terms, repl_terms):
            if src and repl:
                for froms, tos in _create_variants(*src, repl, decompose):
                    yield ICUVariant(froms, tos)

    def _parse_variant_word(self, name: str) -> Optional[tuple[str, str, str]]:
        name = name.strip()
        match = re.fullmatch(r'([~^]?)([^~$^]*)([~$]?)', name)
        if match is None or (match.group(1) == '~' and match.group(3) == '~'):
            raise UsageError(f"Invalid variant word descriptor '{name}'")
        norm_name = self.norm.transliterate(match.group(2)).strip()
        if not norm_name:
            return None

        return norm_name, match.group(1), match.group(3)


_FLAG_MATCH = {'^': '^ ',
               '$': ' ^',
               '': ' '}


def _create_variants(src: str, preflag: str, postflag: str,
                     repl: str, decompose: bool) -> Iterator[tuple[str, str]]:
    if preflag == '~':
        postfix = _FLAG_MATCH[postflag]
        # suffix decomposition
        src = src + postfix
        repl = repl + postfix

        yield src, repl
        yield ' ' + src, ' ' + repl

        if decompose:
            yield src, ' ' + repl
            yield ' ' + src, repl
    elif postflag == '~':
        # prefix decomposition
        prefix = _FLAG_MATCH[preflag]
        src = prefix + src
        repl = prefix + repl

        yield src, repl
        yield src + ' ', repl + ' '

        if decompose:
            yield src, repl + ' '
            yield src + ' ', repl
    else:
        prefix = _FLAG_MATCH[preflag]
        postfix = _FLAG_MATCH[postflag]

        yield prefix + src + postfix, prefix + repl + postfix
