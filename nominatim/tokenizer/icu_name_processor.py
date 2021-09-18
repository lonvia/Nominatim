"""
Processor for names that are imported into the database based on the
ICU library.
"""
from collections import defaultdict
import itertools
import functools

from icu import Transliterator
import datrie

from nominatim.tokenizer import icu_variants as variants


class ICUNameProcessor:
    """ Collects the different transformation rules for normalisation of names
        and provides the functions to aply the transformations.
    """

    def __init__(self, loader):
        self.normalizer = Transliterator.createFromRules("icu_normalization",
                                                         rules.norm_rules)
        self.to_ascii = Transliterator.createFromRules("icu_to_ascii",
                                                       rules.trans_rules +
                                                       ";[:Space:]+ > ' '")
        self.search = Transliterator.createFromRules("icu_search",
                                                     rules.search_rules)

        # Intermediate reorder by source. Also compute required character set.
        immediate = defaultdict(list)
        chars = set()
        for variant in rules.replacements:
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


    def get_normalized(self, name):
        """ Normalize the given name, i.e. remove all elements not relevant
            for search.
        """
        return self.normalizer.transliterate(name).strip()

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

    def get_variants(self, name):
        """ Compute spelling variants given the tagged name.
            Returns a tuple of (word, variant type, list of variants).
        """
        norm_name = self.get_normalized(name.name)

        if name.get_attr('analyzer') == 'ref':
            # references do not produce any variants
            return norm_name, 'ref', [norm_name]

        # other words produce non-types
        return norm_name, None, self.get_variants_ascii(norm_name)

    def _compute_result_set(self, partials, prefix):
        results = set()

        for variant in partials:
            vname = variant + prefix
            trans_name = self.to_ascii.transliterate(vname[1:-1]).strip()
            if trans_name:
                results.add(trans_name)

        return list(results)


    def get_search_normalized(self, name):
        """ Return the normalized version of the name (including transliteration)
            to be applied at search time.
        """
        return self.search.transliterate(' ' + name + ' ').strip()
