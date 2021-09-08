"""
Name processor for changing the analyser of a name according to the kind.
"""

from nominatim.errors import UsageError

def create(func):
    """ Create a function that tags names with an analyzer.
        The name of the analyzer must be set with the analyzer property.
        The list of tag kinds that should get the analyzer needs to be set
        with the 'kinds' property.
    """
    kind_set = func.get('kinds')
    if kind_set is None:
        raise UsageError("set-analyzer-by-tag' processor needs 'kinds' property.")

    analyzer = func.get('analyzer')
    if analyzer is None:
        raise UsageError("set-analyzer-by-tag' processor needs 'analyzer' property.")
    if not isinstance(analyzer, str):
        raise UsageError("set-analyzer-by-tag' processor's 'analyzer' property must be a string.")

    def _process(_, names):
        for name in names:
            if name.kind in kind_set:
                name.set_attr('analyzer', analyzer)

        return names

    return _process
