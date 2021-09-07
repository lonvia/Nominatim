"""
Name processor that splits name values with multiple values into their components.
"""
import re

from nominatim.declutter.places import PlaceName

def create(func):
    """ Create a name processing function that splits name values with
        multiple values into their components. The optional parameter
        'delimiters' can be used to define the characters that should be used
        for splitting. The default is ',;'.
    """
    regexp = '[{}]'.format(func.get('delimiters', ',;'))

    def _process(_, names):
        new_names = []
        for name in names:
            split_names = re.split(regexp, name.name)
            if len(split_names) == 1:
                new_names.append(name)
            else:
                new_names.extend(PlaceName(n, name.kind, name.suffix) for n in split_names)

        return new_names

    return _process
