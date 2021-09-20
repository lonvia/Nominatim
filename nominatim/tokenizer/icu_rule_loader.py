"""
Helper class to create ICU rules from a configuration file.
"""
import importlib
import logging
import itertools
import re

from icu import Transliterator

from nominatim.errors import UsageError
import nominatim.tokenizer.icu_variants as icu_variants
from nominatim.tokenizer.place_preprocessing import PlaceProcessor
from nominatim.tokenizer.icu_token_analysis import ICUTokenAnalysis
from nominatim.db.properties import set_property, get_property

LOG = logging.getLogger()

DBCFG_IMPORT_NORM_RULES = "tokenizer_import_normalisation"
DBCFG_IMPORT_TRANS_RULES = "tokenizer_import_transliteration"

def _flatten_config_list(content):
    if not content:
        return []

    if not isinstance(content, list):
        raise UsageError("List expected in ICU configuration.")

    output = []
    for ele in content:
        if isinstance(ele, list):
            output.extend(_flatten_config_list(ele))
        else:
            output.append(ele)

    return output


def _get_section(rules, section):
    """ Get the section named 'section' from the rules. If the section does
        not exist, raise a usage error with a meaningful message.
    """
    if section not in rules:
        LOG.fatal("Section '%s' not found in tokenizer config.", section)
        raise UsageError("Syntax error in tokenizer configuration file.")

    return rules[section]


class VariantRule:
    """ Saves a single variant expansion.

        An expansion consists of the normalized replacement term and
        a dicitonary of properties that describe when the expansion applies.
    """

    def __init__(self, replacement, properties):
        self.replacement = replacement
        self.properties = properties or {}


class ICURuleLoader:
    """ Compiler for ICU rules from a tokenizer configuration file.
    """

    def __init__(self, config):
        rules = config.load_sub_configuration('icu_tokenizer.yaml',
                                              config='TOKENIZER_CONFIG')

        # Preprocessing rule section and all its subsections are optional.
        self.preprocessing_rules = rules.get('preprocessing', {})

        if 'name' not in self.preprocessing_rules:
            self.preprocessing_rules['name']  = []

        self.normalization_rules = self._cfg_to_icu_rules(rules, 'normalization')
        self.transliteration_rules = self._cfg_to_icu_rules(rules, 'transliteration')

        self.analysis = {}
        for section in _get_section(rules, 'token-analysis'):
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
        # XXX should we also load/save the token-analysis section?


    def save_config_to_db(self, conn):
        """ Save the part of the configuration that cannot be changed into
            the database.
        """
        set_property(conn, DBCFG_IMPORT_NORM_RULES, self.normalization_rules)
        set_property(conn, DBCFG_IMPORT_TRANS_RULES, self.transliteration_rules)


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

        return ';'.join(_flatten_config_list(content)) + ';'


class TokenAnalyzerRule:
    """ Load the configuration for a single token analysis.
    """

    def __init__(self, rules, normalization_rules):
        # Find the analysis module
        module_name = 'nominatim.tokenizer.token_analysis.' \
                      + _get_section(rules, 'analyzer').replace('-', '_')
        self.create = importlib.import_module(module_name).create

        self._parse_variant_list(rules, _VariantMaker(normalization_rules))


    def _parse_variant_list(self, rules, vmaker):
        """ Load the variants section. The section is optional. if it does
            not exist, create an empty set of variants.
        """
        self.variants = set()

        rules = rules.get('variants')

        if not rules:
            return

        rules = _flatten_config_list(rules)

        properties = []
        for section in rules:
            # Create the property field and deduplicate against existing
            # instances.
            props = icu_variants.ICUVariantProperties.from_rules(section)
            for existing in properties:
                if existing == props:
                    props = existing
                    break
            else:
                properties.append(props)

            for rule in section.get('words', []):
                self.variants.update(vmaker.compute(rule, props))


class _VariantMaker:
    """ Generater for all necessary ICUVariants from a single variant rule.

        All text in rules is normalized to make sure the variants match later.
    """

    def __init__(self, norm_rules):
        self.norm = Transliterator.createFromRules("rule_loader_normalization",
                                                   norm_rules)


    def compute(self, rule, props):
        """ Generator for all ICUVariant tuples from a single variant rule.
        """
        parts = re.split(r'(\|)?([=-])>', rule)
        if len(parts) != 4:
            raise UsageError("Syntax error in variant rule: " + rule)

        decompose = parts[1] is None
        src_terms = [self._parse_variant_word(t) for t in parts[0].split(',')]
        repl_terms = (self.norm.transliterate(t.strip()) for t in parts[3].split(','))

        # If the source should be kept, add a 1:1 replacement
        if parts[2] == '-':
            for src in src_terms:
                if src:
                    for froms, tos in _create_variants(*src, src[0], decompose):
                        yield icu_variants.ICUVariant(froms, tos, props)

        for src, repl in itertools.product(src_terms, repl_terms):
            if src and repl:
                for froms, tos in _create_variants(*src, repl, decompose):
                    yield icu_variants.ICUVariant(froms, tos, props)


    def _parse_variant_word(self, name):
        name = name.strip()
        match = re.fullmatch(r'([~^]?)([^~$^]*)([~$]?)', name)
        if match is None or (match.group(1) == '~' and match.group(3) == '~'):
            raise UsageError("Invalid variant word descriptor '{}'".format(name))
        norm_name = self.norm.transliterate(match.group(2))
        if not norm_name:
            return None

        return norm_name, match.group(1), match.group(3)


_FLAG_MATCH = {'^': '^ ',
               '$': ' ^',
               '': ' '}


def _create_variants(src, preflag, postflag, repl, decompose):
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
