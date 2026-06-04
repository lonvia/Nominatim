# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Helper class managing the configuration of the fuzzy tokenizer.
"""
from typing import Mapping, Any, cast, Optional
import logging

import icu

from ...errors import UsageError
from ...config import flatten_config_list, Configuration
from ...db.connection import Connection
from ...db.properties import set_property, get_property

LOG = logging.getLogger()

DBCFG_NORM_RULES = "tokenizer_normalisation_rules"
DBCFG_TRANS_RULES = "tokenizer_transliteration_rules"
DBCFG_BREAKER_RULES = "tokenizer_breaker_rules"


def _get_section(rules: Mapping[str, Any], section: str, dtype: type) -> Any:
    """ Get the section named 'section' from the rules. If the section does
        not exist, raise a usage error with a meaningful message.
    """
    if section not in rules:
        LOG.fatal("Section '%s' not found in tokenizer config.", section)
        raise UsageError("Syntax error in tokenizer configuration file.")

    section = rules[section]

    if not isinstance(section, dtype):
        LOG.fatal("Setion '%s' should be of type %s", section, dtype.__name__)

    return rules[section]


def _create_icu_rules(rules: Mapping[str, Any], section: str,
                      conn: Optional[Connection], prop: str) -> str:
    """ Create an ICU rule string.

        If 'conn' is given, try to load it from Nominatim's properties table.
        If that fails or 'conn' is not given, load the rules from a
        configuration file.
    """
    if conn is not None:
        if (loaded_rules := get_property(conn, prop)):
            return loaded_rules

    content = _get_section(rules, section, list)

    return ';'.join(s.strip(';') for s in flatten_config_list(content, section)) + ';'


def _create_breaker_rules(conn: Optional[Connection], prop: str) -> str:
    """ Create an ICU word breaking rule string.

        If 'conn' is given, try to load the rules from Nominatim's properties table.
        If that fails or 'conn' is not given, create a set of standard rules.
    """
    if conn is not None:
        if (rules := get_property(conn, prop)):
            return rules

    # Use ICU's default word break rules but save them to avoid
    # mismatches when switching ICU versions.
    return cast(str, icu.BreakIterator.createWordInstance((icu.Locale.getUS())).getRules())


class FuzzyTokenizerConfig:
    """ Container class managing the configuration of the Fuzzy tokenizer.

        If a connection object is supplied to the constructor, then the
        configuration is initialized from database properties. Otherwise
        it is read from the configuration files.
    """

    def __init__(self, config: Configuration, conn: Optional[Connection] = None) -> None:
        rules = config.load_sub_configuration('fuzzy_tokenizer.yaml')

        self.normalization_rules = _create_icu_rules(rules, 'normalization',
                                                     conn, DBCFG_NORM_RULES)
        self.transliteration_rules = _create_icu_rules(rules, 'transliteration',
                                                       conn, DBCFG_TRANS_RULES)
        self.breaker_rules = _create_breaker_rules(conn, DBCFG_BREAKER_RULES)

    def save_to_db(self, conn: Connection) -> None:
        set_property(conn, DBCFG_NORM_RULES, self.normalization_rules)
        set_property(conn, DBCFG_TRANS_RULES, self.transliteration_rules)
        set_property(conn, DBCFG_BREAKER_RULES, self.breaker_rules)
