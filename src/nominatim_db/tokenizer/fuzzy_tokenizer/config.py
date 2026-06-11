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
from dataclasses import dataclass

import icu

from ...errors import UsageError
from ...config import flatten_config_list, Configuration
from ...db.connection import Connection
from ...db.properties import set_property, get_property

LOG = logging.getLogger()

DBCFG_NORM_RULES = "tokenizer_normalisation_rules"
DBCFG_TRANS_RULES = "tokenizer_transliteration_rules"
DBCFG_BREAKER_RULES = "tokenizer_breaker_rules"
DBCFG_VARIANT_GROUPING = "tokenizer_variant_grouping"

def _get_section(rules: Mapping[str, Any], section: str, dtype: Any,
                 base_section: str = 'tokenizer config') -> Any:
    """ Get the section named 'section' from the rules. If the section does
        not exist, raise a usage error with a meaningful message.
    """
    if section not in rules:
        LOG.fatal("Section '%s' not found in %s.", section, base_section)
        raise UsageError("Syntax error in tokenizer configuration file.")

    section = rules[section]

    if not isinstance(section, dtype):
        LOG.fatal("Section '%s' should be of type %s", section, dtype.__name__)
        raise UsageError("Syntax error in tokenizer configuration file.")

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


@dataclass
class FuzzyVariantConfig:
    applies_to: list[str]
    rule_type: str
    config: dict[str, Any]
    rules: dict[str, Any]


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

        self.variant_rules = []
        if 'variants' in rules:
            for vconfig in _get_section(rules, 'variants', list):
                if not isinstance(vconfig, dict):
                    LOG.fatal("Variant config excpected to be a dict")
                    raise UsageError("Syntax error in tokenizer configuration file.")

                if (cfg := vconfig.get('applies-to')):
                    if isinstance(cfg, str):
                        applies_to = [cfg]
                    else:
                        if not isinstance(cfg, list) and \
                                any(not isinstance(cfg, str) for v in cfg):
                            LOG.fatal("'applies-to' needs to be a string or list of strings.")
                            raise UsageError("Syntax error in tokenizer configuration file.")
                        applies_to = cfg
                else:
                    applies_to = []

                rule_type = _get_section(vconfig, 'type', str, 'variants configuration')
                rule_config = {k: v for k,v in vconfig.items()
                               if k not in ('rules', 'applies-to', 'type')}

                for rule in flatten_config_list(_get_section(vconfig, 'rules', list,
                                                             'rules section for variants')):
                    self.variant_rules.append(FuzzyVariantConfig(
                        applies_to, rule_type, rule_config, rule))

        if conn is None:
            if 'token-analysis' in rules:
                analysis_rules = _get_section(rules, 'variants', dict)
                self.word_grouping = analysis_rules.get('variant-grouping', 'by-attribute')
                if not isinstance(self.word_grouping, str) \
                        or self.word_grouping not in ('by-attribute', 'by-name'):
                    raise UsageError('Unknown value for token-analysis:variant-grouping.')
            else:
                self.word_grouping = 'by-attribute'
        else:
            self.word_grouping = get_property(conn, DBCFG_VARIANT_GROUPING)


    def save_to_db(self, conn: Connection) -> None:
        """ Save all parts of the configuration to the database that
            need to be persistent, i.e. not changed after import.
        """
        set_property(conn, DBCFG_NORM_RULES, self.normalization_rules)
        set_property(conn, DBCFG_TRANS_RULES, self.transliteration_rules)
        set_property(conn, DBCFG_BREAKER_RULES, self.breaker_rules)
        set_property(conn, DBCFG_VARIANT_GROUPING, self.word_grouping)
