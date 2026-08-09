# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2024 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
    Contains the class which handles statistics for the
    import of special phrases.
"""
import logging
LOG = logging.getLogger()


class SpecialPhrasesImporterStatistics():
    """
        Class handling statistics of the import
        process of special phrases.
    """
    def __init__(self) -> None:
        self._intialize_values()

    def _intialize_values(self) -> None:
        """
            Set all counts for the global
            import to 0.
        """
        self.invalids = 0

    def notify_one_phrase_invalid(self) -> None:
        """
            Add +1 to the count of invalid entries
            fetched from the wiki.
        """
        self.invalids += 1

    def notify_import_done(self) -> None:
        """
            Print stats for the whole import process
            and reset all values.
        """
        LOG.info('====================================================================')
        LOG.info('Final statistics of the import:')
        LOG.info('- %s phrases were invalid.', self.invalids)
        if self.invalids > 0:
            LOG.info('  Those invalid phrases have been skipped.')

        if self.invalids > 0:
            LOG.warning('%s phrases were invalid and have been skipped during the whole process.',
                        self.invalids)

        self._intialize_values()
