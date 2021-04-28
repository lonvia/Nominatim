"""
    Implementation of the 'special-phrases' command.
"""
import logging
from nominatim.tools import SpecialPhrasesImporter
from nominatim.db.connection import connect

LOG = logging.getLogger()

# Do not repeat documentation of subcommand classes.
# pylint: disable=C0111

class ImportSpecialPhrases:
    """\
    Import special phrases.
    """
    @staticmethod
    def add_args(parser):
        group = parser.add_argument_group('Input arguments')
        group.add_argument('--import-from-wiki', action='store_true',
                           help='Import special phrases from the OSM wiki to the database.')

    @staticmethod
    def run(args):
        from ..tokenizer import factory as tokenizer_factory

        if args.import_from_wiki:
            LOG.warning('Special phrases importation starting')
            tokenizer = tokenizer_factory.get_tokenizer_for_db(args.config)
            with connect(args.config.get_libpq_dsn()) as db_connection:
                SpecialPhrasesImporter(
                    args.config, args.phplib_dir, db_connection
                ).import_from_wiki(tokenizer)
        return 0
