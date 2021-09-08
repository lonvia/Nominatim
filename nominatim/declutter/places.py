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


class PlaceProcessor:
    """ Pre-processor for place data processed by the indexer.

        The processor can be configured through the `place_processing.yaml` file.
    """

    def __init__(self, rules):
        self.name_proc_functions = []
        if rules:
            self._create_name_functions(rules)


    def _create_name_functions(self, rules):
        """ Set up the pre-processing functions for names from the
            'name' section of the given rules.
        """
        if 'name' not in rules:
            raise UsageError("No 'name' section in 'place_processing.yaml'.")

        for func in rules['name']:
            if 'step' not in func:
                raise UsageError("Name processing step is missing the 'step' attribute.")
            module_name = 'nominatim.declutter.name_transform.' + func['step'].replace('-', '_')
            step_func_module = importlib.import_module(module_name)
            self.name_proc_functions.append(step_func_module.create(func))



    def get_searchable_names(self, place):
        """ Convert the list of names for the given place into a list of
            searchable names. A name is considered 'searchable' when its full
            version may be used for finding the place. The function also adds
            properties to each name describing its nature.
        """
        if not place.name:
            return None

        names = []

        # Convert the dictionary into a list of PlaceNames:
        for key, value in place.name.items():
            parts = key.split(':', 1)
            names.append(PlaceName(value.strip(),
                                   parts[0].strip(),
                                   parts[1].strip() if len(parts) > 1 else None))

        # Apply the configured transforms.
        for func in self.name_proc_functions:
            names = func(place, names)

        return names
