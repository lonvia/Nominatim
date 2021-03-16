"""
Functions for creating a tokenizer or initialising the right one for an
existing database.

A tokenizer is something that is bound to the lifetime of a database. It
can be choosen and configured before the intial import but then needs to
be used consistently when querying and updating the database.

This module provides the functions to create and configure a new tokenizer
as well as instanciating the appropriate tokenizer for updating an existing
database.

Querying is currently still done in the PHP code. The appropriate PHP
normalizer module is installed, when the tokenizer is created.
"""
import logging

from ..errors import UsageError
from .legacy_tokenizer import LegacyTokenizer

LOG = logging.getLogger()

def create_tokenizer(config):
    """ Create a new tokenizer as defined by the given configuration.
    """
    return LegacyTokenizer(config.get_libpq_dsn(), config.project_dir)


def get_tokenizer_for_db(config):
    """ Instantiate a tokenizer for an existing database.

        The function makes sure that the same tokenizer is used as during
        import time.
    """
    basedir = config.project_dir / 'tokenizer'
    if not basedir.is_dir():
        LOG.fatal("Cannot find tokenizer data in '%s'.", basedir)
        #raise UsageError('Cannot initialize tokenizer.')
    # XXX
    return LegacyTokenizer(config.get_libpq_dsn(), config.project_dir)
