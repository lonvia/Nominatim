"""
Helper class to create ICU rules from a configuration file.
"""
import importlib
import json
import logging

from nominatim.config import flatten_config_list
from nominatim.errors import UsageError
from nominatim.tokenizer.place_preprocessing import PlaceProcessor
from nominatim.tokenizer.icu_token_analysis import ICUTokenAnalysis
from nominatim.tools import country_info
from nominatim.db.properties import set_property, get_property

LOG = logging.getLogger()

DBCFG_IMPORT_NORM_RULES = "tokenizer_import_normalisation"
DBCFG_IMPORT_TRANS_RULES = "tokenizer_import_transliteration"
DBCFG_IMPORT_ANALYSIS_RULES = "tokenizer_token_analysis"


def _get_section(rules, section):
    """ Get the section named 'section' from the rules. If the section does
        not exist, raise a usage error with a meaningful message.
    """
    if section not in rules:
        LOG.fatal("Section '%s' not found in tokenizer config.", section)
        raise UsageError("Syntax error in tokenizer configuration file.")

    return rules[section]


class ICURuleLoader:
    """ Compiler for ICU rules from a tokenizer configuration file.
    """

    def __init__(self, config):
        rules = config.load_sub_configuration('icu_tokenizer.yaml',
                                              config='TOKENIZER_CONFIG')

        # Make sure that country information is available for processors.
        country_info.setup_country_config(config)

        # Preprocessing rule section and all its subsections are optional.
        self.preprocessing_rules = rules.get('preprocessing', {})

        if 'name' not in self.preprocessing_rules:
            self.preprocessing_rules['name']  = []

        self.normalization_rules = self._cfg_to_icu_rules(rules, 'normalization')
        self.transliteration_rules = self._cfg_to_icu_rules(rules, 'transliteration')

        self.analysis_rules = _get_section(rules, 'token-analysis')
        self._setup_analysis()


    def _setup_analysis(self):
        self.analysis = {}
        for section in self.analysis_rules:
            name = section.get('id', None)
            if name in self.analysis:
                if name is None:
                    LOG.fatal("ICU tokenizer configuration has two default token analyzers.")
                else:
                    LOG.fatal("ICU tokenizer configuration has two token "
                              "analyzers with id '%s'.", name)
                UsageError("Syntax error in ICU tokenizer config.")
            self.analysis[name] = TokenAnalyzerRule(section, self.normalization_rules)


    def load_config_from_db(self, conn):
        """ Get previously saved parts of the configuration from the
            database.
        """
        self.normalization_rules = get_property(conn, DBCFG_IMPORT_NORM_RULES)
        self.transliteration_rules = get_property(conn, DBCFG_IMPORT_TRANS_RULES)
        self.analysis_rules = json.loads(get_property(conn, DBCFG_IMPORT_ANALYSIS_RULES))
        self._setup_analysis()


    def save_config_to_db(self, conn):
        """ Save the part of the configuration that cannot be changed into
            the database.
        """
        set_property(conn, DBCFG_IMPORT_NORM_RULES, self.normalization_rules)
        set_property(conn, DBCFG_IMPORT_TRANS_RULES, self.transliteration_rules)
        set_property(conn, DBCFG_IMPORT_ANALYSIS_RULES, json.dumps(self.analysis_rules))


    def make_place_preprocessor(self):
        """ Create a place preprocessor from the configured rules.
        """
        return PlaceProcessor(self.preprocessing_rules)

    def make_token_analysis(self):
        """ Create a dictionary of configured name analyzers.
        """
        return ICUTokenAnalysis(self.normalization_rules,
                                self.transliteration_rules,
                                self.analysis)


    @staticmethod
    def _cfg_to_icu_rules(rules, section):
        """ Load an ICU ruleset from the given section. If the section is a
            simple string, it is interpreted as a file name and the rules are
            loaded verbatim from the given file. The filename is expected to be
            relative to the tokenizer rule file. If the section is a list then
            each line is assumed to be a rule. All rules are concatenated and returned.
        """
        content = _get_section(rules, section)

        if content is None:
            return ''

        return ';'.join(flatten_config_list(content, section)) + ';'


class TokenAnalyzerRule:
    """ Container for the analysis module and the configuration of a
        single token analyzer.
    """

    def __init__(self, rules, normalization_rules):
        # Find the analysis module
        module_name = 'nominatim.tokenizer.token_analysis.' \
                      + _get_section(rules, 'analyzer').replace('-', '_')
        analysis_mod = importlib.import_module(module_name)
        self.create = analysis_mod.create

        self.config = analysis_mod.load_config(rules, normalization_rules)
