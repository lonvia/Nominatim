"""
Functions for extracting sanitized information from the information
saved in the placex table.
"""
import importlib

from nominatim.errors import UsageError

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


    def clone(self, name=None, kind=None, suffix=None, attr=None):
        newobj = PlaceName(name or self.name,
                           kind or self.kind,
                           suffix or self.suffix)

        newobj.attr.update(self.attr)
        if attr:
            newobj.attr.update(attr)

        return newobj


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


    def has_attr(self, key):
        """ Check if the given attribute is set.
        """
        return key in self.attr


class _ProcessInfo:
    """ Container class for information handed through to handler functions.
    """

    def __init__(self, place):
        self.place = place
        self.names = self._convert_name_dict(place.name)
        self.address = self._convert_name_dict(place.address)


    @staticmethod
    def _convert_name_dict(names):
        """ Convert a dictionary of names into a list of PlaceNames.
            The dictionary key is split into the primary part of the key
            and the suffix (the part after an optional colon).
        """
        out = []

        if names:
            for key, value in names.items():
                parts = key.split(':', 1)
                out.append(PlaceName(value.strip(),
                                     parts[0].strip(),
                                     parts[1].strip() if len(parts) > 1 else None))

        return out


class PlaceProcessor:
    """ Pre-processor for place data processed by the indexer.

        The processor can be configured through the `place_processing.yaml` file.
    """

    def __init__(self, rules):
        self.name_proc_functions = self._create_name_functions(rules)


    @staticmethod
    def _create_name_functions(rules):
        """ Set up the pre-processing functions for names from the
            'name' section of the given rules.
        """
        funcs = []

        for func in rules:
            if 'step' not in func:
                raise UsageError("Name processing step is missing the 'step' attribute.")
            module_name = 'nominatim.tokenizer.name_transform.' + func['step'].replace('-', '_')
            step_func_module = importlib.import_module(module_name)
            funcs.append(step_func_module.create(func))

        return funcs


    def process_names(self, place):
        """ Convert the list of names for the given place into a list of
            searchable names. A name is considered 'searchable' when its full
            version may be used for finding the place. The function also adds
            properties to each name describing its nature.
        """
        obj = _ProcessInfo(place)

        for func in self.name_proc_functions:
            func(obj)

        return obj.names, obj.address

