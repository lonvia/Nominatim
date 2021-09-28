"""
Processor for names that are imported into the database based on the
ICU library.
"""
from collections import defaultdict
import itertools
import functools
import re

from icu import Transliterator
import datrie

from nominatim.config import flatten_config_list
from nominatim.errors import UsageError
import nominatim.tokenizer.icu_variants as icu_variants

####  Rule loading section

def load_config(rules, normalization_rules):
    """ Load the configuration for the analysis.

        The generic analyser understands exactly one section 'variants'.
    """
    return {'variants': _get_variant_config(rules.get('variants'),
                                            normalization_rules),
            'mode': rules.get('mode', '')}


def _get_variant_config(rules, normalization_rules):
    """ Parse the variant section of the configuration and convert
        it to the internal configuration format used later.
    """
    if not rules:
        return None

    variants = _parse_variant_list(rules, _VariantMaker(normalization_rules))

    # Intermediate reorder by source and compute required character set.
    immediate = defaultdict(list)
    chars = set()

    for variant in variants:
        if variant.source[-1] == ' ' and variant.replacement[-1] == ' ':
            replstr = variant.replacement[:-1]
        else:
            replstr = variant.replacement
        immediate[variant.source].append(replstr)
        chars.update(variant.source)

    return {'chars': ''.join(chars), 'replacements': list(immediate.items())}


def _parse_variant_list(rules, vmaker):
    """ Load the variants section. The section is optional. if it does
        not exist, create an empty set of variants.
    """
    variants = set()

    rules = flatten_config_list(rules, 'variants')

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
            variants.update(vmaker.compute(rule, props))

    return variants


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


_FLAG_MATCH = {'^': '^ ', '$': ' ^', '': ' '}


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

####  Analysis section

def create(name, normalizer, transliterator, config):
    """ Create a new analysis instance for this module.
    """
    return GenericTokenAnalysis(name, normalizer, transliterator,
                                config['variants'], config['mode'])


class GenericTokenAnalysis:
    """ Collects the different transformation rules for normalisation of names
        and provides the functions to aply the transformations.
    """

    def __init__(self, name, normalizer, transliterator, variants, mode):
        self.variant_type = name
        self.normalizer = normalizer
        self.to_ascii = transliterator
        self.variants_only = mode == 'variant-only'

        if variants:
            # Create a datrie from replacements.
            self.replacements = datrie.Trie(variants['chars'])
            for src, repllist in variants['replacements']:
                self.replacements[src] = repllist
        else:
            self.replacements = None


    @functools.lru_cache(maxsize=5000)
    def get_variants_ascii(self, norm_name):
        """ Compute the spelling variants for the given normalized name
            and transliterate the result.
        """
        baseform = '^ ' + norm_name + ' ^'
        partials = ['']

        startpos = 0
        pos = 0
        force_space = False
        while pos < len(baseform):
            full, repl = self.replacements.longest_prefix_item(baseform[pos:],
                                                               (None, None))
            if full is not None:
                done = baseform[startpos:pos]
                partials = [v + done + r
                            for v, r in itertools.product(partials, repl)
                            if not force_space or r.startswith(' ')]
                if len(partials) > 128:
                    # If too many variants are produced, they are unlikely
                    # to be helpful. Only use the original term.
                    startpos = 0
                    break
                startpos = pos + len(full)
                if full[-1] == ' ':
                    startpos -= 1
                    force_space = True
                pos = startpos
            else:
                pos += 1
                force_space = False

        # No variants detected? Fast return.
        if startpos == 0:
            if self.variants_only:
                return []

            trans_name = self.to_ascii.transliterate(norm_name).strip()
            return [trans_name] if trans_name else []

        if self.variants_only:
            exclude = self.to_ascii.transliterate(norm_name).strip()
        else:
            exclude = ''

        return self._compute_result_set(partials, baseform[startpos:], exclude)


    def get_full_terms(self, name):
        """ Compute spelling variants given the tagged name.
            Returns a tuple of (word, variant type, list of terms).
        """
        norm_name = self.normalizer.transliterate(name.name)

        # If there are no variant rules, use a shortcut here.
        if self.replacements is None:
            terms = [self.to_ascii.transliterate(norm_name)]
        else:
            terms = self.get_variants_ascii(norm_name)

        return norm_name, self.variant_type, terms


    def _compute_result_set(self, partials, prefix, exclude):
        results = set()

        for variant in partials:
            vname = variant + prefix
            trans_name = self.to_ascii.transliterate(vname[1:-1]).strip()
            if trans_name and trans_name != exclude:
                results.add(trans_name)

        return list(results)
