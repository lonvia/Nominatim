"""
Name processor handling names with addendums in braces.
"""
from nominatim.tokenizer.place_preprocessing import PlaceName

def create(_):
    """ Create a name processing function that creates additional name variants
        when a name has an addendum in brackets (e.g. "Halle (Saale)"). The
        additional variant only contains the main name without the bracket part.
    """
    def _process(_, names):
        """ Add variants for names that have a bracket extension.
        """
        names.extend(PlaceName(n.name.split('(')[0].strip(), n.kind, n.suffix)
                     for n in names if '(' in n.name)

        return names

    return _process
