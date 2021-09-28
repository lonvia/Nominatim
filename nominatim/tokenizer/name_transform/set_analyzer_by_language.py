"""
Name processor for tagging the langauge of the name
"""
import re

from nominatim.errors import UsageError
from nominatim.tools import country_info

class _AnalyzerByLanguage:
    """ Processor for tagging the language of names in a place.
    """

    def __init__(self, config):
        if 'kinds' in config:
            self.regexes = [re.compile(regex) for regex in config['kinds']]
        else:
            self.regexes = None

        self.use_defaults = config.get('use-defaults', 'no')
        if not self.use_defaults in ('monolingual', 'all'):
            self.use_defaults = False

        self.replace = config.get('mode', 'replace') != 'append'

        self.whitelist = config.get('whitelist', [])

        # Compute the languages to use when no suffix is given.
        self.deflangs = {}
        for ccode, prop in country_info.iterate():
            clangs = prop['languages']
            if len(clangs) == 1 or self.use_defaults == 'all':
                if self.whitelist:
                    self.deflangs[ccode] = [l for l in clangs if l in self.whitelist]
                else:
                    self.deflangs[ccode] = clangs



    def _kind_matches(self, kind):
        if self.regexes is None:
            return True

        return any(regex.search(kind) for regex in self.regexes)


    def __call__(self, obj):
        if not obj.names:
            return

        more_names = []

        for name in (n for n in obj.names if not n.has_attr('analyzer')
                                             and self._kind_matches(n.kind)):
            if name.suffix:
                if len(name.suffix) in (2, 3) and name.suffix.islower() \
                   and (not self.whitelist or name.suffix in self.whitelist):
                    langs = [name.suffix]
                else:
                    langs = None
            else:
                langs = self.deflangs.get(obj.place.country_code)

            if langs:
                if self.replace:
                    name.set_attr('analyzer', langs[0])
                else:
                    more_names.append(name.clone(attr={'analyzer': langs[0]}))

                more_names.extend(name.clone(attr={'analyzer': l}) for l in langs[1:])

        obj.names.extend(more_names)


def create(func):
    """ Create a function that tags names with their language.
        The language is taken from the suffix. Only 2 or 3-letter codes
        are accepted. If the name has no suffix, then use the default
        from the language configuration.

        The list of tag kinds that should get the analyzer needs to be set
        with the 'kinds' property. It expects a list of regular expressions
        that need to match against the tag.
    """
    return _AnalyzerByLanguage(func)
