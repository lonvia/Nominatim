# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Custom mocks for testing.
"""

from nominatim_db.db import properties


class MockPropertyTable:
    """ A property table for testing.
    """
    def __init__(self, conn):
        self.conn = conn

    def set(self, name, value):
        """ Set a property in the table to the given value.
        """
        properties.set_property(self.conn, name, value)

    def get(self, name):
        """ Set a property in the table to the given value.
        """
        return properties.get_property(self.conn, name)
