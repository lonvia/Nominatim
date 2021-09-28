"""
Name processor that splits name values with multiple values into their components.
"""
import re

from nominatim.tokenizer.place_preprocessing import PlaceName

def create(func):
    """ Create a name processing function that splits name values with
        multiple values into their components. The optional parameter
        'delimiters' can be used to define the characters that should be used
        for splitting. The default is ',;'.
    """
    regexp = '[{}]'.format(func.get('delimiters', ',;'))

    def _process(obj):
        if not obj.names:
            return

        new_names = []
        for name in obj.names:
            split_names = re.split(regexp, name.name)
            if len(split_names) == 1:
                new_names.append(name)
            else:
                new_names.extend(PlaceName(n, name.kind, name.suffix) for n in split_names)

        obj.names = new_names

    return _process
