"""
Processor for names that are imported into the database based on the
ICU library.
"""
from collections import defaultdict
import itertools
import functools

import datrie

def create(name, normalizer, transliterator, analysis_rules):
    """ Create a new analysis instance for this module.
    """
    return GenericTokenAnalysis(name, normalizer, transliterator,
                                analysis_rules.variants)


class GenericTokenAnalysis:
    """ Collects the different transformation rules for normalisation of names
        and provides the functions to aply the transformations.
    """

    def __init__(self, name, normalizer, transliterator, variants):
        self.variant_type = name
        self.normalizer = normalizer
        self.to_ascii = transliterator

        if not variants:
            self.replacements = None
        else:
            # Intermediate reorder by source. Also compute required character set.
            immediate = defaultdict(list)
            chars = set()
            for variant in variants:
                if variant.source[-1] == ' ' and variant.replacement[-1] == ' ':
                    replstr = variant.replacement[:-1]
                else:
                    replstr = variant.replacement
                immediate[variant.source].append(replstr)
                chars.update(variant.source)
            # Then copy to datrie
            self.replacements = datrie.Trie(''.join(chars))
            for src, repllist in immediate.items():
                self.replacements[src] = repllist


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
            trans_name = self.to_ascii.transliterate(norm_name).strip()
            return [trans_name] if trans_name else []

        return self._compute_result_set(partials, baseform[startpos:])


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


    def _compute_result_set(self, partials, prefix):
        results = set()

        for variant in partials:
            vname = variant + prefix
            trans_name = self.to_ascii.transliterate(vname[1:-1]).strip()
            if trans_name:
                results.add(trans_name)

        return list(results)
