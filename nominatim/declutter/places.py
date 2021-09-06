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


    def set_attr(self, key, value):
        """ Add the given property to the name. If the property was already
            set, then the value is overwritten.
        """
        self.attr[key] = value


    def get_attr(self, key, default=None):
        """ Return the given property or the value of 'default' if it
            is not set.
        """
        return self.attr.get(key, default)


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
    names = _mark_references(place, names)

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

_REF_LIST = ("ref", "int_ref", "nat_ref", "reg_ref", "loc_ref", "old_ref",
            "iata", "icao", "pcode")

def _mark_references(_, names):
    """ Set the 'is_ref' property on all names that are unabbreviable
        references.
    """
    for name in names:
        if name.kind in _REF_LIST:
            name.set_attr('is_ref', True)

    return names
