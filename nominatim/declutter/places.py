"""
Functions for extracting sanitized information from the information
saved in the placex table.
"""

import re

class PlaceName:
    """ A searchable name for a place together with properties.
        Every name has to basic properties:
        * 'kind' describes the name of the OSM key used without any suffixes
          (i.e. the part after the colon removed)
        * 'suffix' contains the suffix of the OSM tag, if any
    """

    def __init__(self, name, kind, suffix):
        self.name = name
        self.kind = kind
        self.suffix = suffix
        self.attr = {}



def get_searchable_names(place):
    """ Convert the list of names for the given place into a list of
        searchable names. A name is considered 'searchable' when its full
        version may be used for finding the place. The function also adds
        properties to each name describing its nature.
    """
    if not place.name:
        return None

    names = _create_basic_name_list(place)
    names = _split_multi_names(place, names)
    names = _add_non_braced(place, names)

    return names


def _create_basic_name_list(place):
    """ Convert a name dictionary retrived from the database into a list
        of PlaceNames.
    """
    names = []

    for key, value in place.name.items():
        parts = key.split(':', 1)
        names.append(PlaceName(value.strip(),
                               parts[0].strip(),
                               parts[1].strip() if len(parts) > 1 else None))

    return names


def _split_multi_names(_, names):
    """ Create a new name list where names with semi-colons are split in
        separate entities.
    """
    new_names = []
    for name in names:
        split_names = re.split('[;,]', name.name)
        if len(split_names) == 1:
            new_names.append(name)
        else:
            new_names.extend(PlaceName(n, name.kind, name.suffix) for n in split_names)

    return new_names


def _add_non_braced(_, names):
    """ Add variants for names that have a bracket extension.
    """
    names.extend(PlaceName(n.name.split('(')[0].strip(), n.kind, n.suffix)
                 for n in names if '(' in n.name)

    return names
