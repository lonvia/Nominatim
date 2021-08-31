"""
Functions for extracting sanitized information from the information
saved in the placex table.
"""

import re

def get_searchable_names(place):
    """ Convert the list of names for the given place into a list of
        searchable names. A name is considered 'searchable' when its full
        version may be used for finding the place. The function also adds
        properties to each name describing its nature.
    """
    names = place.name
    if not names:
        return None

    full_names = set()
    for name in (n.strip() for ns in names.values() for n in re.split('[;,]', ns)):
        if name:
            full_names.add(name)

            brace_idx = name.find('(')
            if brace_idx >= 0:
                full_names.add(name[:brace_idx].strip())

    return [(f, {}) for f in full_names]
