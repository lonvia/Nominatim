# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Variant processor for regex mutations.
"""
from typing import Optional, Any
import re

from icu import Transliterator

from ...errors import UsageError
from . import types as ttyp

class MutationProcessor(ttyp.VariantProcessor):
    RESERVED_RULE_KEYS = ('mutations', )

    def __init__(self, config: dict[str, Any], rules: dict[str, Any],
                 normalizer: Transliterator) -> None:
        self.mode = config.get('mode', 'append')  # one of: append, replace
        scope = config.get('scope', 'global')  # one of: global, variants

        if scope == 'global':
            self._internal_process_name = self._process_name_global
        else:
            raise UsageError(f"Unknown scope '{scope}' for mutation.")

        if 'mutations' not in rules:
            raise UsageError("Variant processor requires 'mutations' section.")
        if not isinstance(rules['mutations'], list):
            raise UsageError("'mutations' section in variant processor needs to be list.")
        filters: list[tuple[str, str]] = []
        for k, v in rules.items():
            if k != 'mutations':
                if not isinstance(v, (str, int)):
                    raise UsageError('Variant attributes must be strings')
                filters.append((k, str(v)))

        self.filters = tuple(sorted(filters))

        self.regexes = [(re.compile(r['pattern']), str(r['replacement']))
                        for r in rules['mutations']
                        if 'pattern' in r and 'replacement' in r]

    def process_name(self, name: str) -> Optional[list[str]]:
        return self._internal_process_name(name)

    def _process_name_global(self, name: str) -> Optional[list[str]]:
        mutated = name
        for pattern, repl in self.regexes:
            mutated = pattern.sub(repl, mutated)

        if mutated == name:
            return None

        return [name, mutated] if self.mode == 'append' else [mutated]

