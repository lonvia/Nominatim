# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Variant processor for regex mutations.
"""
from typing import Optional

from icu import Transliterator

from .config import FuzzyVariantConfig
from . import types as ttyp

class MutationProcessor(ttyp.VariantProcessor):

    def __init__(self, vconfig: FuzzyVariantConfig, normalizer: Transliterator) -> None:
        pass

    def get_filter_attributes(self) -> tuple[tuple[str, str], ...]:
        return tuple()

    def process_name(self, name: str) -> Optional[list[str]]:
        return None

