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
from pathlib import Path
import importlib

from ..errors import UsageError
from ..db import properties
from ..db.connection import connect

LOG = logging.getLogger()

def _import_tokenizer(name):
    """ Load the tokenizer.py module from project directory.
    """
    return importlib.import_module('nominatim.tokenizer.' + name + '_tokenizer')


def create_tokenizer(config, sqllib_dir, phplib_dir, module_dir, config_dir):
    """ Create a new tokenizer as defined by the given configuration.

        The tokenizer data and code is copied into the 'tokenizer' directory
        of the project directory and the tokenizer loaded from its new location.
    """
    # Create the directory for the tokenizer data
    basedir = config.project_dir / 'tokenizer'
    if not basedir.exists():
        basedir.mkdir()
    elif not basedir.is_dir():
        raise UsageError('Tokenizer directory %s cannot be created.', basedir)

    tokenizer_module = _import_tokenizer(config.TOKENIZER)

    tokenizer = tokenizer_module.create(config.get_libpq_dsn(), basedir)
    tokenizer.init_new_db(config, sqllib_dir, phplib_dir, module_dir, config_dir)

    with connect(config.get_libpq_dsn()) as conn:
        properties.set_property(conn, 'tokenizer', config.TOKENIZER)

    return tokenizer


def get_tokenizer_for_db(config):
    """ Instantiate a tokenizer for an existing database.

        The function makes sure that the same tokenizer is used as during
        import time.
    """
    basedir = config.project_dir / 'tokenizer'
    if not basedir.is_dir():
        LOG.fatal("Cannot find tokenizer data in '%s'.", basedir)
        raise UsageError('Cannot initialize tokenizer.')

    with connect(config.get_libpq_dsn()) as conn:
        name = properties.get_property(conn, 'tokenizer')
    if name is None:
        LOG.fatal("Tokenizer was not set up properly. Database property missing.")
        raise UsageError('Cannot initialize tokenizer.')

    tokenizer_module = _import_tokenizer(name)

    tokenizer = tokenizer_module.create(config.get_libpq_dsn(), basedir)
    tokenizer.init_from_project(config)

    return tokenizer
