"""
    Functions to import special phrases into the database.
"""
import logging
import os
from os.path import isfile
from pathlib import Path
import re
import subprocess
import json

from psycopg2.sql import Identifier, Literal, SQL

from nominatim.tools.exec_utils import get_url
from nominatim.errors import UsageError
from nominatim.tools.special_phrases.importer_statistics import SpecialPhrasesImporterStatistics

LOG = logging.getLogger()
class SpecialPhrasesImporter():
    # pylint: disable-msg=too-many-instance-attributes
    """
        Class handling the process of special phrases importations.
    """
    def __init__(self, config, phplib_dir, db_connection) -> None:
        self.statistics_handler = SpecialPhrasesImporterStatistics()
        self.db_connection = db_connection
        self.config = config
        self.phplib_dir = phplib_dir
        self.black_list, self.white_list = self._load_white_and_black_lists()
        #Compile the regex here to increase performances.
        self.occurence_pattern = re.compile(
            r'\| *([^\|]+) *\|\| *([^\|]+) *\|\| *([^\|]+) *\|\| *([^\|]+) *\|\| *([\-YN])'
        )
        self.sanity_check_pattern = re.compile(r'^\w+$')
        # This set will contain all existing phrases to be added.
        # It contains tuples with the following format: (lable, class, type, operator)
        self.word_phrases = set()
        #This set will contain all existing place_classtype tables which doesn't match any
        #special phrases class/type on the wiki.
        self.table_phrases_to_delete = set()

    def import_from_wiki(self, tokenizer, languages=None):
        """
            Iterate through all specified languages and
            extract corresponding special phrases from the wiki.
        """
        if languages is not None and not isinstance(languages, list):
            raise TypeError('The \'languages\' argument should be of type list.')

        self._fetch_existing_place_classtype_tables()

        #Get all languages to process.
        languages = self._load_languages() if not languages else languages

        #Store pairs of class/type for further processing
        class_type_pairs = set()

        for lang in languages:
            LOG.warning('Importing phrases for lang: %s...', lang)
            wiki_page_xml_content = SpecialPhrasesImporter._get_wiki_content(lang)
            class_type_pairs.update(self._process_xml_content(wiki_page_xml_content, lang))
            self.statistics_handler.notify_current_lang_done(lang)

        self._create_place_classtype_table_and_indexes(class_type_pairs)
        self._remove_non_existent_tables_from_db()
        self.db_connection.commit()

        with tokenizer.name_analyzer() as analyzer:
            analyzer.update_special_phrases(self.word_phrases)

        LOG.warning('Import done.')
        self.statistics_handler.notify_import_done()


    def _fetch_existing_place_classtype_tables(self):
        """
            Fetch existing place_classtype tables.
            Fill the table_phrases_to_delete set of the class.
        """
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public'
            AND table_name like 'place_classtype_%';
        """
        with self.db_connection.cursor() as db_cursor:
            db_cursor.execute(SQL(query))
            for row in db_cursor:
                self.table_phrases_to_delete.add(row[0])

    def _load_white_and_black_lists(self):
        """
            Load white and black lists from phrases-settings.json.
        """
        settings_path = (self.config.config_dir / 'phrase-settings.json').resolve()

        if self.config.PHRASE_CONFIG:
            settings_path = self._convert_php_settings_if_needed(self.config.PHRASE_CONFIG)

        with settings_path.open("r") as json_settings:
            settings = json.load(json_settings)
        return settings['blackList'], settings['whiteList']

    def _load_languages(self):
        """
            Get list of all languages from env config file
            or default if there is no languages configured.
            The system will extract special phrases only from all specified languages.
        """
        default_languages = [
            'af', 'ar', 'br', 'ca', 'cs', 'de', 'en', 'es',
            'et', 'eu', 'fa', 'fi', 'fr', 'gl', 'hr', 'hu',
            'ia', 'is', 'it', 'ja', 'mk', 'nl', 'no', 'pl',
            'ps', 'pt', 'ru', 'sk', 'sl', 'sv', 'uk', 'vi']
        return self.config.LANGUAGES.split(',') if self.config.LANGUAGES else default_languages

    @staticmethod
    def _get_wiki_content(lang):
        """
            Request and return the wiki page's content
            corresponding to special phrases for a given lang.
            Requested URL Example :
                https://wiki.openstreetmap.org/wiki/Special:Export/Nominatim/Special_Phrases/EN
        """
        url = 'https://wiki.openstreetmap.org/wiki/Special:Export/Nominatim/Special_Phrases/' + lang.upper() # pylint: disable=line-too-long
        return get_url(url)

    def _check_sanity(self, lang, phrase_class, phrase_type):
        """
            Check sanity of given inputs in case somebody added garbage in the wiki.
            If a bad class/type is detected the system will exit with an error.
        """
        type_matchs = self.sanity_check_pattern.findall(phrase_type)
        class_matchs = self.sanity_check_pattern.findall(phrase_class)

        if not class_matchs or not type_matchs:
            LOG.warning("Bad class/type for language %s: %s=%s. It will not be imported",
                        lang, phrase_class, phrase_type)
            return False
        return True

    def _process_xml_content(self, xml_content, lang):
        """
            Process given xml content by extracting matching patterns.
            Matching patterns are processed there and returned in a
            set of class/type pairs.
        """
        #One match will be of format [label, class, type, operator, plural]
        matches = self.occurence_pattern.findall(xml_content)
        #Store pairs of class/type for further processing
        class_type_pairs = set()

        for match in matches:
            phrase_label = match[0].strip()
            phrase_class = match[1].strip()
            phrase_type = match[2].strip()
            phrase_operator = match[3].strip()
            #Needed if some operator in the wiki are not written in english
            phrase_operator = '-' if phrase_operator not in ('near', 'in') else phrase_operator
            #hack around a bug where building=yes was imported with quotes into the wiki
            phrase_type = re.sub(r'\"|&quot;', '', phrase_type)

            #blacklisting: disallow certain class/type combinations
            if (
                    phrase_class in self.black_list.keys() and
                    phrase_type in self.black_list[phrase_class]
            ):
                continue
            #whitelisting: if class is in whitelist, allow only tags in the list
            if (
                    phrase_class in self.white_list.keys() and
                    phrase_type not in self.white_list[phrase_class]
            ):
                continue

            #sanity check, in case somebody added garbage in the wiki
            if not self._check_sanity(lang, phrase_class, phrase_type):
                self.statistics_handler.notify_one_phrase_invalid()
                continue

            class_type_pairs.add((phrase_class, phrase_type))

            self.word_phrases.add((phrase_label, phrase_class,
                                   phrase_type, phrase_operator))

        return class_type_pairs


    def _create_place_classtype_table_and_indexes(self, class_type_pairs):
        """
            Create table place_classtype for each given pair.
            Also create indexes on place_id and centroid.
        """
        LOG.warning('Create tables and indexes...')

        sql_tablespace = self.config.TABLESPACE_AUX_DATA
        if sql_tablespace:
            sql_tablespace = ' TABLESPACE '+sql_tablespace

        with self.db_connection.cursor() as db_cursor:
            db_cursor.execute("CREATE INDEX idx_placex_classtype ON placex (class, type)")

        for pair in class_type_pairs:
            phrase_class = pair[0]
            phrase_type = pair[1]

            table_name = 'place_classtype_{}_{}'.format(phrase_class, phrase_type)

            if table_name in self.table_phrases_to_delete:
                self.statistics_handler.notify_one_table_ignored()
                #Remove this table from the ones to delete as it match a class/type
                #still existing on the special phrases of the wiki.
                self.table_phrases_to_delete.remove(table_name)
                #So dont need to create the table and indexes.
                continue

            #Table creation
            self._create_place_classtype_table(sql_tablespace, phrase_class, phrase_type)

            #Indexes creation
            self._create_place_classtype_indexes(sql_tablespace, phrase_class, phrase_type)

            #Grant access on read to the web user.
            self._grant_access_to_webuser(phrase_class, phrase_type)

            self.statistics_handler.notify_one_table_created()

        with self.db_connection.cursor() as db_cursor:
            db_cursor.execute("DROP INDEX idx_placex_classtype")


    def _create_place_classtype_table(self, sql_tablespace, phrase_class, phrase_type):
        """
            Create table place_classtype of the given phrase_class/phrase_type if doesn't exit.
        """
        table_name = 'place_classtype_{}_{}'.format(phrase_class, phrase_type)
        with self.db_connection.cursor() as db_cursor:
            db_cursor.execute(SQL("""
                    CREATE TABLE IF NOT EXISTS {{}} {} 
                    AS SELECT place_id AS place_id,st_centroid(geometry) AS centroid FROM placex 
                    WHERE class = {{}} AND type = {{}}""".format(sql_tablespace))
                              .format(Identifier(table_name), Literal(phrase_class),
                                      Literal(phrase_type)))


    def _create_place_classtype_indexes(self, sql_tablespace, phrase_class, phrase_type):
        """
            Create indexes on centroid and place_id for the place_classtype table.
        """
        index_prefix = 'idx_place_classtype_{}_{}_'.format(phrase_class, phrase_type)
        base_table = 'place_classtype_{}_{}'.format(phrase_class, phrase_type)
        #Index on centroid
        if not self.db_connection.index_exists(index_prefix + 'centroid'):
            with self.db_connection.cursor() as db_cursor:
                db_cursor.execute(SQL("""
                    CREATE INDEX {{}} ON {{}} USING GIST (centroid) {}""".format(sql_tablespace))
                                  .format(Identifier(index_prefix + 'centroid'),
                                          Identifier(base_table)), sql_tablespace)

        #Index on place_id
        if not self.db_connection.index_exists(index_prefix + 'place_id'):
            with self.db_connection.cursor() as db_cursor:
                db_cursor.execute(SQL(
                    """CREATE INDEX {{}} ON {{}} USING btree(place_id) {}""".format(sql_tablespace))
                                  .format(Identifier(index_prefix + 'place_id'),
                                          Identifier(base_table)))


    def _grant_access_to_webuser(self, phrase_class, phrase_type):
        """
            Grant access on read to the table place_classtype for the webuser.
        """
        table_name = 'place_classtype_{}_{}'.format(phrase_class, phrase_type)
        with self.db_connection.cursor() as db_cursor:
            db_cursor.execute(SQL("""GRANT SELECT ON {} TO {}""")
                              .format(Identifier(table_name),
                                      Identifier(self.config.DATABASE_WEBUSER)))

    def _remove_non_existent_tables_from_db(self):
        """
            Remove special phrases which doesn't exist on the wiki anymore.
            Delete the place_classtype tables.
        """
        LOG.warning('Cleaning database...')
        #Array containing all queries to execute. Contain tuples of format (query, parameters)
        queries_parameters = []

        #Delete place_classtype tables corresponding to class/type which are not on the wiki anymore
        for table in self.table_phrases_to_delete:
            self.statistics_handler.notify_one_table_deleted()
            query = SQL('DROP TABLE IF EXISTS {}').format(Identifier(table))
            queries_parameters.append((query, ()))

        with self.db_connection.cursor() as db_cursor:
            for query, parameters in queries_parameters:
                db_cursor.execute(query, parameters)

    def _convert_php_settings_if_needed(self, file_path):
        """
            Convert php settings file of special phrases to json file if it is still in php format.
        """
        if not isfile(file_path):
            raise UsageError(str(file_path) + ' is not a valid file.')

        file, extension = os.path.splitext(file_path)
        json_file_path = Path(file + '.json').resolve()

        if extension not in('.php', '.json'):
            raise UsageError('The custom NOMINATIM_PHRASE_CONFIG file has not a valid extension.')

        if extension == '.php' and not isfile(json_file_path):
            try:
                subprocess.run(['/usr/bin/env', 'php', '-Cq',
                                (self.phplib_dir / 'migration/PhraseSettingsToJson.php').resolve(),
                                file_path], check=True)
                LOG.warning('special_phrase configuration file has been converted to json.')
                return json_file_path
            except subprocess.CalledProcessError:
                LOG.error('Error while converting %s to json.', file_path)
                raise
        else:
            return json_file_path
