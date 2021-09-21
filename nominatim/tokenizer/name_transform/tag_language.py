"""
Name processor for tagging the langauge of the name
"""
import re

from nominatim.errors import UsageError
from nominatim.tools import country_info

def create(func):
    """ Create a function that tags names with their language.
        The language is taken from the suffix. Only 2 or 3-letter codes
        are accepted. If the name has no suffix, then use the default
        from the language configuration.

        The list of tag kinds that should get the analyzer needs to be set
        with the 'kinds' property. It expects a list of regular expressions
        that need to match against the tag.
    """
    kind_set = func.get('kinds')
    regexes = [re.compile(regex) for regex in func.get('kinds', [])]

    def _process(place, names):
        ccode = place.country_code

        for name in names:
            for regex in regexes:
                if regex.search(name.kind) is not None:
                    break
            else:
                continue

            if not name.suffix:
                if ccode:
                    deflangs = country_info.get_property(ccode, 'languages')
                    if deflangs:
                        name.set_attr('lang', deflangs)
            else:
                if len(name.suffix) in (2, 3) and name.suffix.islower():
                    name.set_attr('lang', [name.suffix])

        return names

    return _process
