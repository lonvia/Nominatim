# SPDX-License-Identifier: GPL-3.0-or-later
#
# This file is part of Nominatim. (https://nominatim.org)
#
# Copyright (C) 2026 by the Nominatim developer community.
# For a full list of authors see the git log.
"""
Tests for the sanitizer that removes multilingual concatenated names.
"""
import pytest

from nominatim_db.config import Configuration
from nominatim_db.data.place_info import PlaceInfo
from nominatim_db.tokenizer.place_sanitizer import PlaceSanitizer


@pytest.fixture
def run_sanitizer(def_config):
    def _run(extra_args=None, **kwargs):
        place = PlaceInfo({'name': {k.replace('_', ':'): v
                                    for k, v in kwargs.items()},
                           'country_code': 'be', 'rank_address': 26})

        sanitizer_args = {'step': 'clean-multilingual-names'}
        if extra_args:
            sanitizer_args.update(extra_args)

        PlaceSanitizer([sanitizer_args], def_config).process_names(place)

        return sorted([(p.name, p.kind, p.suffix or '')
                       for p in place.searchable_names])

    return _run


class TestDelimiter:
    """ Test removal with some common delimiters used worlwide.
    """

    def test_hyphen_delimiter(self, run_sanitizer):
        res = run_sanitizer(name='गढ़वा - Garhwa - گڑھوا',
                            name_hi='गढ़वा',
                            name_en='Garhwa',
                            name_ur='گڑھوا')

        assert ('गढ़वा - Garhwa - گڑھوا', 'name', '') not in res
        assert ('गढ़वा', 'name', 'hi') in res
        assert ('Garhwa', 'name', 'en') in res
        assert ('گڑھوا', 'name', 'ur') in res

    def test_slash_delimiter(self, run_sanitizer):
        res = run_sanitizer(name='Rue de la Gare / Stationsstraat/Bahnhofstraße',
                            name_fr='Rue de la Gare',
                            name_nl='Stationsstraat',
                            name_de='Bahnhofstraße')

        assert ('Rue de la Gare / Stationsstraat/Bahnhofstraße', 'name', '') not in res
        assert ('Rue de la Gare', 'name', 'fr') in res
        assert ('Stationsstraat', 'name', 'nl') in res
        assert ('Bahnhofstraße', 'name', 'de') in res

    def test_space_delimiter(self, run_sanitizer):
        res = run_sanitizer(name='Oran ⵡⴻⵀⵔⴰⵏ وهران',
                            name_en='Oran',
                            name_ber='ⵡⴻⵀⵔⴰⵏ',
                            name_ar='وهران',
                            name_be='Аран')

        assert ('Oran ⵡⴻⵀⵔⴰⵏ وهران', 'name', '') not in res
        assert ('Oran', 'name', 'en') in res
        assert ('ⵡⴻⵀⵔⴰⵏ', 'name', 'ber') in res
        assert ('وهران', 'name', 'ar') in res
        assert ('Аран', 'name', 'be') in res

    def test_space_delimiter_multiword_name(self, run_sanitizer):
        res = run_sanitizer(name='Baarin Left Banner ᠪᠠᠭᠠᠷᠢᠨ ᠵᠡᠭᠦᠨ 巴林左旗',
                            name_en='Baarin Left Banner',
                            name_mn='ᠪᠠᠭᠠᠷᠢᠨ ᠵᠡᠭᠦᠨ',
                            name_zh='巴林左旗')

        assert ('Baarin Left Banner ᠪᠠᠭᠠᠷᠢᠨ ᠵᠡᠭᠦᠨ 巴林左旗', 'name', '') not in res
        assert ('Baarin Left Banner', 'name', 'en') in res
        assert ('ᠪᠠᠭᠠᠷᠢᠨ ᠵᠡᠭᠦᠨ', 'name', 'mn') in res
        assert ('巴林左旗', 'name', 'zh') in res

    def test_mixed_delimiters(self, run_sanitizer):
        res = run_sanitizer(name='गढ़वा /Garhwa - گڑھوا',
                            name_hi='गढ़वा',
                            name_en='Garhwa',
                            name_ur='گڑھوا')

        assert ('गढ़वा /Garhwa - گڑھوا', 'name', '') not in res

    def test_name_equals_single_lang_name(self, run_sanitizer):
        res = run_sanitizer(name='New York City',
                            name_en='New York City',
                            name_fr='Ville de New York')

        assert ('New York City', 'name', '') not in res

    def test_name_does_not_match_any_pattern(self, run_sanitizer):
        res = run_sanitizer(name='Some Unique Name',
                            name_fr='Rue du Poulet',
                            name_nl='Kiekenstraat')

        assert ('Some Unique Name', 'name', '') in res

    def test_partial_match(self, run_sanitizer):
        res = run_sanitizer(name='Rue du Poulet - Unknown',
                            name_fr='Rue du Poulet',
                            name_nl='Kiekenstraat')

        assert ('Rue du Poulet - Unknown', 'name', '') in res

    def test_custom_delimiter(self, run_sanitizer):
        res = run_sanitizer(extra_args={'delimiters': ['|']},
                            name='गढ़वा|Garhwa | گڑھوا',
                            name_hi='गढ़वा',
                            name_en='Garhwa',
                            name_ur='گڑھوا')

        assert ('गढ़वा|Garhwa | گڑھوا', 'name', '') not in res

    def test_delimeter_in_name(self, run_sanitizer):
        res = run_sanitizer(name='Berchem-Sainte-Agathe - Sint-Agatha-Berchem',
                            name_ar='بيرشيم سانت أغاث',
                            name_fr='Berchem-Sainte-Agathe',
                            name_la='Berchemium Agathae',
                            name_li='Sint-Agatha-Berchem',
                            name_nl='Sint-Agatha-Berchem',
                            name_ru='Беркем-Сент-Агат')

        assert ('Berchem-Sainte-Agathe - Sint-Agatha-Berchem', 'name', '') not in res
        assert ('بيرشيم سانت أغاث', 'name', 'ar') in res
        assert ('Berchem-Sainte-Agathe', 'name', 'fr') in res
        assert ('Berchemium Agathae', 'name', 'la') in res
        assert ('Sint-Agatha-Berchem', 'name', 'li') in res
        assert ('Sint-Agatha-Berchem', 'name', 'nl') in res
        assert ('Беркем-Сент-Агат', 'name', 'ru') in res

    def test_leading_or_trailing_delimiter(self, run_sanitizer):
        res = run_sanitizer(name='/ Foo - Bar - ',
                            name_fr='Foo',
                            name_es='Bar')

        assert ('/ Foo - Bar -', 'name', '') not in res
        assert ('Foo', 'name', 'fr') in res
        assert ('Bar', 'name', 'es') in res

    def test_longest_match_priority(self, run_sanitizer):
        """ Test that overlapping substrings in language names do not break
            the concatenation check due to premature regex matching.
        """
        res = run_sanitizer(name='New - New York',
                            name_fr='New',
                            name_en='New York')

        assert ('New - New York', 'name', '') not in res
        assert ('New', 'name', 'fr') in res
        assert ('New York', 'name', 'en') in res

    def test_non_language_suffix_ignored(self, run_sanitizer):
        res = run_sanitizer(name='Old Town Hall',
                            name_prefix='Old Town',
                            name_suffix='Hall')

        assert ('Old Town Hall', 'name', '') in res

    def test_three_letter_language_used(self, run_sanitizer):
        # A language code followed by a script variant is a valid suffix.
        res = run_sanitizer(name='臺北 - Taipei',
                            name_zh_Hant='臺北',
                            name_en='Taipei')

        assert ('臺北 - Taipei', 'name', '') not in res


class TestFilterKind:
    """ Test with custom filter-kind configuration.
    """

    def test_filter_kind_ref(self, run_sanitizer):
        place = PlaceInfo({'name': {'name': 'गढ़वा - Garhwa',
                                    'name:fr': 'गढ़वा',
                                    'name:nl': 'Garhwa',
                                    'ref': 'A - B',
                                    'ref:fr': 'A',
                                    'ref:nl': 'B'},
                           'country_code': 'be', 'rank_address': 26})

        # When filter-kind is set to 'ref', only ref names are processed.
        cfg = Configuration(None)
        PlaceSanitizer([{'step': 'clean-multilingual-names',
                         'filter-kind': 'ref'}], cfg).process_names(place)

        names = [(p.name, p.kind, p.suffix or '') for p in place.searchable_names]
        # 'name' bare tag should be kept (not matched by filter-kind)
        assert ('गढ़वा - Garhwa', 'name', '') in names
        # 'ref' bare tag should be removed
        assert ('A - B', 'ref', '') not in names

    def test_alt_name_not_affected_by_default(self, run_sanitizer):
        place = PlaceInfo({'name': {'name': 'गढ़वा - Garhwa',
                                    'name:fr': 'गढ़वा',
                                    'name:nl': 'Garhwa',
                                    'alt_name': 'Baz - Qux',
                                    'alt_name:fr': 'Baz',
                                    'alt_name:nl': 'Qux'},
                           'country_code': 'be', 'rank_address': 26})

        cfg = Configuration(None)
        PlaceSanitizer([{'step': 'clean-multilingual-names'}], cfg).process_names(place)

        names = [(p.name, p.kind, p.suffix or '') for p in place.searchable_names]
        assert ('गढ़वा - Garhwa', 'name', '') not in names
        assert ('गढ़वा', 'name', 'fr') in names
        assert ('Garhwa', 'name', 'nl') in names
        # alt_name kind, doesn't match default filter-kind='name'.
        assert ('Baz - Qux', 'alt_name', '') in names
        assert ('Baz', 'alt_name', 'fr') in names
        assert ('Qux', 'alt_name', 'nl') in names
