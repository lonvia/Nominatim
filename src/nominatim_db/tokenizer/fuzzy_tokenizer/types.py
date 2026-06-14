# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Type definitions for the Fuzzy tokenizer.
"""

TOKEN_WORD = 'W'
TOKEN_HOUSENUMBER = 'H'
TOKEN_COUNTRY = 'C'

TOKEN_LABELS = {
    TOKEN_WORD: 'name',
    TOKEN_HOUSENUMBER: 'housenumber',
    TOKEN_COUNTRY: 'country'
}
