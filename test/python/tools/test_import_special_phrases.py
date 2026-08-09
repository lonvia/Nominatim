# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
    Tests for import special phrases methods
    of the class SPImporter.
"""
import pytest
from nominatim_db.tools.special_phrases.sp_importer import SPImporter
from nominatim_db.tools.special_phrases.sp_wiki_loader import SPWikiLoader
from nominatim_db.tools.special_phrases.special_phrase import SpecialPhrase


@pytest.fixture
def sp_importer(temp_db_conn, def_config, monkeypatch):
    """
        Return an instance of SPImporter.
    """
    monkeypatch.setenv('NOMINATIM_LANGUAGES', 'en')
    loader = SPWikiLoader(def_config)
    return SPImporter(def_config, temp_db_conn, loader)


@pytest.fixture
def xml_wiki_content(src_dir):
    """
        return the content of the static xml test file.
    """
    xml_test_content = src_dir / 'test' / 'testdata' / 'special_phrases_test_content.txt'
    return xml_test_content.read_text(encoding='utf-8')


def test_check_sanity_class(sp_importer):
    """
        Check for _check_sanity() method.
        If a wrong class or type is given, an UsageError should raise.
        If a good class and type are given, nothing special happens.
    """

    assert not sp_importer._check_sanity(SpecialPhrase('en', '', 'type', ''))
    assert not sp_importer._check_sanity(SpecialPhrase('en', 'class', '', ''))

    assert sp_importer._check_sanity(SpecialPhrase('en', 'class', 'type', ''))


def test_load_white_and_black_lists(sp_importer):
    """
        Test that _load_white_and_black_lists() well return
        black list and white list and that they are of dict type.
    """
    black_list, white_list = sp_importer._load_white_and_black_lists()

    assert isinstance(black_list, dict) and isinstance(white_list, dict)


@pytest.mark.parametrize("should_replace", [(True), (False)])
def test_import_phrases(monkeypatch, sp_importer, tokenizer_mock,
                        xml_wiki_content, should_replace):
    """
        Check that the main import_phrases() method is well executed.
        It should pass all phrases of the wiki content on to the tokenizer.
    """
    monkeypatch.setattr('nominatim_db.tools.special_phrases.sp_wiki_loader._get_wiki_content',
                        lambda lang: xml_wiki_content)

    tokenizer = tokenizer_mock()
    sp_importer.import_phrases(tokenizer, should_replace)

    assert len(tokenizer.analyser_cache['special_phrases']) == 19
    assert ('Zip Line', 'aerialway', 'zip_line', '-') in sp_importer.word_phrases
