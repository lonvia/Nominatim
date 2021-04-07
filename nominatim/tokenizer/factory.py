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
import importlib.util

from ..errors import UsageError
from .legacy_tokenizer import LegacyTokenizer

LOG = logging.getLogger()

def _import_tokenizer(target):
    """ Load the tokenizer.py module from project directory.
    """
    # We don't want to add the project directory to the search path, so
    # load the module manually (as per importlib documentation).
    spec = importlib.util.spec_from_file_location('project_tokenizer', str(target))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _copy_src_file(srcdir, targetdir, basename, suffix):
    src = (srcdir / (basename + suffix)).resolve()
    target = (targetdir / ('tokenizer' + suffix)).resolve()
    target.write_text(src.read_text())


def create_tokenizer_directory(config, sqllib_dir, phplib_dir):
    """ Create the tokenizer diretory in the project directory and populate it.

        Usually called as part of create_tokenizer().
    """
    # Create the directory for the tokenizer data
    basedir = config.project_dir / 'tokenizer'
    if not basedir.exists():
        basedir.mkdir()
    elif not basedir.is_dir():
        raise UsageError('Tokenizer directory %s cannot be created.', basedir)

    # Hard-coded use of legacy tokenizer
    name = 'legacy_tokenizer'
    _copy_src_file(Path(__file__) / '..', basedir, name, '.py')
    _copy_src_file(sqllib_dir / 'tokenizers', basedir, name, '.sql')
    _copy_src_file(phplib_dir / 'tokenizers', basedir, name, '.php')

    return basedir


def create_tokenizer(config, sqllib_dir, phplib_dir):
    """ Create a new tokenizer as defined by the given configuration.

        The tokenizer data and code is copied into the 'tokenizer' directory
        of the project directory and the tokenizer loaded from its new location.
    """
    basedir = create_tokenizer_directory(config, sqllib_dir, phplib_dir)

    tokenizer_module = _import_tokenizer(basedir / 'tokenizer.py')

    tokenizer = tokenizer_module.create(config.get_libpq_dsn(), basedir)
    tokenizer.init_new_db(config)

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

    tokenizer_module = _import_tokenizer(basedir / 'tokenizer.py')

    tokenizer = tokenizer_module.create(config.get_libpq_dsn(), basedir)
    tokenizer.init_from_project()

    return tokenizer
