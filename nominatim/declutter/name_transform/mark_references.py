"""
Name processor for tagging names that are immutable references.
"""

def create(func):
    """ Create a function that tags reference names with the 'is_ref' property.
        The list of tag keys that designate a reference can be configured
        with the 'ref-tags' setting. The default is: [ref]
    """
    ref_set = set(func.get('ref-tags', ('ref', )))

    def _process(_, names):
        for name in names:
            if name.kind in ref_set:
                name.set_attr('is_ref', True)

        return names

    return _process
