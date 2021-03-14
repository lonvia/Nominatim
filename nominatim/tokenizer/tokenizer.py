"""
Functions for splitting names into search terms.
"""

def get_search_terms(cursor, names):
    """ Determine the search terms for the given dictionary of dictionary of names.
    """
    if not names:
        return {}

    value_sql = "(%s, %s)"
    cursor.execute("SELECT id, make_keywords(name) FROM (VALUES {}) as x(id, name)"
                .format(','.join(["(%s, %s)"]  * len(names))),
                [val for sublist in names.items() for val in sublist])

    return {r[0] : r[1] for r in list(cursor)}
