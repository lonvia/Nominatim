# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Sanitizer that removes name tags which are a simple concatenation of
language-specific name tags.

In areas where multiple languages are spoken, OSM often has a `name` tag
which contains all language variants joined by a delimiter (e.g.
``name=Rue du Marché aux Poulets - Kiekenmarkt``). These language variants
also appear in separate tags like ``name:fr`` and ``name:nl``. The
combined name is redundant and adds noise to the search index.

This sanitizer detects when a bare ``name`` tag (i.e. one without a
language suffix) consists entirely of a concatenation of names that are
already present in language-specific name tags, and removes it. Exact
duplicates are also considered redundant and removed, even when no
concatenation is involved.

Different delimiters used within a single name, i.e. ``Foo / Bar - Baz``
is recognised as a concatenation as well. A trailing/leading delimiter is ignored, i.e.
``- Foo - Bar - `` is still recognised.

Arguments:
    delimiters: A list of delimiter strings to try when checking if the
                name is a concatenation.
                (default: ``["/", "-", " "]``)

    filter-kind: Define which 'kind' of names are considered for
                 removal. Takes a string or list of strings where each
                 string is a regular expression.
                 (default: ``name``)
"""
import re
from typing import Sequence

from .base import ProcessInfo, SanitizerFunc
from .config import SanitizerConfig


def create(config: SanitizerConfig) -> SanitizerFunc:
    """ Create a function to remove multilingual concatenated names.
    """
    delimiters: Sequence[str] = config.get_string_list('delimiters',
                                                       ['/', '-', ' '])
    filter_kind = config.get_filter('filter-kind', ['name'])

    # Suffix having less than 4 letters before ending or being followed by a non-letter
    # (eg. `name:en`, `name:fr-CA`, `name:zh-Hans`) is considered a valid language code.
    illegal_lang_pattern = re.compile('(?:^[A-Za-z]{4,}.*)')

    delim_alts = [r'\s*' + re.escape(d) + r'\s*' for d in delimiters]
    delim_pattern = re.compile('(?:' + '|'.join(delim_alts) + ')')

    def _is_concatenated(name_value: str, lang_names: set[str]) -> bool:
        """ Check if name_value can be fully decomposed into values
            from lang_names, optionally separated by delimiters.
        """

        lang_names_by_length = sorted(lang_names, key=len, reverse=True)
        name_pattern = re.compile(
            '(?:' + '|'.join(re.escape(n) for n in lang_names_by_length) + ')'
        )

        idx = 0
        m = delim_pattern.match(name_value, idx)
        if m:
            idx = m.end()

        while idx < len(name_value):
            m = name_pattern.match(name_value, idx)
            if not m:
                return False
            idx = m.end()

            if idx < len(name_value):
                m = delim_pattern.match(name_value, idx)
                if not m:
                    return False
                idx = m.end()

        return True

    def _process(obj: ProcessInfo) -> None:
        if not obj.names:
            return

        # Collect language-specific name values for each kind.
        # A language-specific name has the same kind but a non-empty suffix.
        lang_names_by_kind: dict[str, set[str]] = {}
        for name in obj.names:
            if name.suffix and not illegal_lang_pattern.match(name.suffix) \
               and filter_kind(name.kind):
                lang_names_by_kind.setdefault(name.kind, set()).add(name.name)

        if not lang_names_by_kind:
            return

        obj.names = [name for name in obj.names
                     if not (name.suffix is None
                             and name.kind in lang_names_by_kind
                             and _is_concatenated(name.name, lang_names_by_kind[name.kind]))]

    return _process
