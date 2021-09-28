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
    if 'kinds' in func:
        regexes = [re.compile(regex) for regex in func['kinds']]
    else:
        regexes = None
    use_defaults = func.get('use-defaults', 'monolingual')
    if use_defaults == 'monolingual':
        use_defaults = True
    elif not isinstance(use_defaults, bool):
        UsageError("Illegal value for 'use-defaults'.")

    whitelist = func.get('whitelist', [])

    def _process(obj):
        if not obj.names:
            return

        ccode = obj.place.country_code

        for name in obj.names:
            if regexes is not None:
                for regex in regexes:
                    if regex.search(name.kind) is not None:
                        break
                else:
                    continue

            if not name.suffix:
                if ccode and use_defaults:
                    deflangs = country_info.get_property(ccode, 'languages')
                    lang = deflangs[0] if len(deflangs) == 1 else None
            else:
                if len(name.suffix) in (2, 3) and name.suffix.islower():
                    lang = name.suffix
                else:
                    lang = None

            if lang and (not whitelist or lang in whitelist):
                name.set_attr('analyzer', lang)

    return _process
