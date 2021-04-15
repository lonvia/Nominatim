-- Functions for term normalisation and access to the 'word' table.

CREATE OR REPLACE FUNCTION transliteration(text) RETURNS text
  AS '{{ modulepath }}/nominatim.so', 'transliteration'
LANGUAGE c IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION gettokenstring(text) RETURNS text
  AS '{{ modulepath }}/nominatim.so', 'gettokenstring'
LANGUAGE c IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION make_standard_name(name TEXT) RETURNS TEXT
  AS $$
DECLARE
  o TEXT;
BEGIN
  o := public.gettokenstring(public.transliteration(name));
  RETURN trim(substr(o,1,length(o)));
END;
$$
LANGUAGE plpgsql IMMUTABLE;

-- returns NULL if the word is too common
CREATE OR REPLACE FUNCTION getorcreate_word_id(lookup_word TEXT) 
  RETURNS INTEGER
  AS $$
DECLARE
  lookup_token TEXT;
  return_word_id INTEGER;
  count INTEGER;
BEGIN
  lookup_token := trim(lookup_word);
  SELECT min(word_id), max(search_name_count) FROM word
    WHERE word_token = lookup_token and class is null and type is null
    INTO return_word_id, count;
  IF return_word_id IS NULL THEN
    return_word_id := nextval('seq_word');
    INSERT INTO word VALUES (return_word_id, lookup_token, null, null, null, null, 0);
  ELSE
    IF count > get_maxwordfreq() THEN
      return_word_id := NULL;
    END IF;
  END IF;
  RETURN return_word_id;
END;
$$
LANGUAGE plpgsql;

-- Create housenumber tokens from an OSM addr:housenumber.
-- The housnumber is split at comma and semicolon as necessary.
-- The function returns the normalized form of the housenumber suitable
-- for comparison.
CREATE OR REPLACE FUNCTION create_housenumber_id(housenumber TEXT)
  RETURNS TEXT
  AS $$
DECLARE
  normtext TEXT;
BEGIN
  SELECT array_to_string(array_agg(trans), ';')
    INTO normtext
    FROM (SELECT lookup_word as trans, getorcreate_housenumber_id(lookup_word)
          FROM (SELECT make_standard_name(h) as lookup_word
                FROM regexp_split_to_table(housenumber, '[,;]') h) x) y;

  return normtext;
END;
$$ LANGUAGE plpgsql STABLE STRICT;

CREATE OR REPLACE FUNCTION getorcreate_postcode_id(postcode TEXT)
  RETURNS INTEGER
  AS $$
DECLARE
  lookup_token TEXT;
  lookup_word TEXT;
  return_word_id INTEGER;
BEGIN
  lookup_word := upper(trim(postcode));
  lookup_token := ' ' || make_standard_name(lookup_word);
  SELECT min(word_id) FROM word
    WHERE word_token = lookup_token and word = lookup_word
          and class='place' and type='postcode'
    INTO return_word_id;
  IF return_word_id IS NULL THEN
    return_word_id := nextval('seq_word');
    INSERT INTO word VALUES (return_word_id, lookup_token, lookup_word,
                             'place', 'postcode', null, 0);
  END IF;
  RETURN return_word_id;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION getorcreate_country(lookup_word TEXT,
                                               lookup_country_code varchar(2))
  RETURNS INTEGER
  AS $$
DECLARE
  lookup_token TEXT;
  return_word_id INTEGER;
BEGIN
  lookup_token := ' '||trim(lookup_word);
  SELECT min(word_id) FROM word
    WHERE word_token = lookup_token and country_code=lookup_country_code
    INTO return_word_id;
  IF return_word_id IS NULL THEN
    return_word_id := nextval('seq_word');
    INSERT INTO word VALUES (return_word_id, lookup_token, null,
                             null, null, lookup_country_code, 0);
  END IF;
  RETURN return_word_id;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION getorcreate_amenity(lookup_word TEXT, normalized_word TEXT,
                                               lookup_class text, lookup_type text)
  RETURNS INTEGER
  AS $$
DECLARE
  lookup_token TEXT;
  return_word_id INTEGER;
BEGIN
  lookup_token := ' '||trim(lookup_word);
  SELECT min(word_id) FROM word
  WHERE word_token = lookup_token and word = normalized_word
        and class = lookup_class and type = lookup_type
  INTO return_word_id;
  IF return_word_id IS NULL THEN
    return_word_id := nextval('seq_word');
    INSERT INTO word VALUES (return_word_id, lookup_token, normalized_word,
                             lookup_class, lookup_type, null, 0);
  END IF;
  RETURN return_word_id;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION getorcreate_amenityoperator(lookup_word TEXT,
                                                       normalized_word TEXT,
                                                       lookup_class text,
                                                       lookup_type text,
                                                       op text)
  RETURNS INTEGER
  AS $$
DECLARE
  lookup_token TEXT;
  return_word_id INTEGER;
BEGIN
  lookup_token := ' '||trim(lookup_word);
  SELECT min(word_id) FROM word
  WHERE word_token = lookup_token and word = normalized_word
        and class = lookup_class and type = lookup_type and operator = op
  INTO return_word_id;
  IF return_word_id IS NULL THEN
    return_word_id := nextval('seq_word');
    INSERT INTO word VALUES (return_word_id, lookup_token, normalized_word,
                             lookup_class, lookup_type, null, 0, op);
  END IF;
  RETURN return_word_id;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION getorcreate_name_id(lookup_word TEXT, src_word TEXT)
  RETURNS INTEGER
  AS $$
DECLARE
  lookup_token TEXT;
  nospace_lookup_token TEXT;
  return_word_id INTEGER;
BEGIN
  lookup_token := ' '||trim(lookup_word);
  SELECT min(word_id) FROM word
  WHERE word_token = lookup_token and class is null and type is null
  INTO return_word_id;
  IF return_word_id IS NULL THEN
    return_word_id := nextval('seq_word');
    INSERT INTO word VALUES (return_word_id, lookup_token, src_word,
                             null, null, null, 0);
  END IF;
  RETURN return_word_id;
END;
$$
LANGUAGE plpgsql;


-- Normalize a string and lookup its word ids (partial words).
CREATE OR REPLACE FUNCTION addr_ids_from_name(lookup_word TEXT)
  RETURNS INTEGER[]
  AS $$
DECLARE
  words TEXT[];
  id INTEGER;
  return_word_id INTEGER[];
  word_ids INTEGER[];
  j INTEGER;
BEGIN
  words := string_to_array(make_standard_name(lookup_word), ' ');
  IF array_upper(words, 1) IS NOT NULL THEN
    FOR j IN 1..array_upper(words, 1) LOOP
      IF (words[j] != '') THEN
        SELECT array_agg(word_id) INTO word_ids
          FROM word
         WHERE word_token = words[j] and class is null and type is null;

        IF word_ids IS NULL THEN
          id := nextval('seq_word');
          INSERT INTO word VALUES (id, words[j], null, null, null, null, 0);
          return_word_id := return_word_id || id;
        ELSE
          return_word_id := array_merge(return_word_id, word_ids);
        END IF;
      END IF;
    END LOOP;
  END IF;

  RETURN return_word_id;
END;
$$
LANGUAGE plpgsql;


-- Normalize a string and look up its name ids (full words).
CREATE OR REPLACE FUNCTION word_ids_from_name(lookup_word TEXT)
  RETURNS INTEGER[]
  AS $$
DECLARE
  lookup_token TEXT;
  return_word_ids INTEGER[];
BEGIN
  lookup_token := ' '|| make_standard_name(lookup_word);
  SELECT array_agg(word_id) FROM word
    WHERE word_token = lookup_token and class is null and type is null
    INTO return_word_ids;
  RETURN return_word_ids;
END;
$$
LANGUAGE plpgsql STABLE STRICT;


CREATE OR REPLACE FUNCTION create_country(src HSTORE, country_code varchar(2))
  RETURNS VOID
  AS $$
DECLARE
  s TEXT;
  w INTEGER;
  words TEXT[];
  item RECORD;
  j INTEGER;
BEGIN
  FOR item IN SELECT (each(src)).* LOOP

    s := make_standard_name(item.value);
    w := getorcreate_country(s, country_code);

    words := regexp_split_to_array(item.value, E'[,;()]');
    IF array_upper(words, 1) != 1 THEN
      FOR j IN 1..array_upper(words, 1) LOOP
        s := make_standard_name(words[j]);
        IF s != '' THEN
          w := getorcreate_country(s, country_code);
        END IF;
      END LOOP;
    END IF;
  END LOOP;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION make_keywords(src TEXT)
  RETURNS INTEGER[]
  AS $$
DECLARE
  result INTEGER[];
  s TEXT;
  w INTEGER;
  words TEXT[];
  i INTEGER;
  j INTEGER;
BEGIN
  result := '{}'::INTEGER[];

  s := make_standard_name(src);
  w := getorcreate_name_id(s, src);

  IF NOT (ARRAY[w] <@ result) THEN
    result := result || w;
  END IF;

  w := getorcreate_word_id(s);

  IF w IS NOT NULL AND NOT (ARRAY[w] <@ result) THEN
    result := result || w;
  END IF;

  words := string_to_array(s, ' ');
  IF array_upper(words, 1) IS NOT NULL THEN
    FOR j IN 1..array_upper(words, 1) LOOP
      IF (words[j] != '') THEN
        w = getorcreate_word_id(words[j]);
        IF w IS NOT NULL AND NOT (ARRAY[w] <@ result) THEN
          result := result || w;
        END IF;
      END IF;
    END LOOP;
  END IF;

  words := regexp_split_to_array(src, E'[,;()]');
  IF array_upper(words, 1) != 1 THEN
    FOR j IN 1..array_upper(words, 1) LOOP
      s := make_standard_name(words[j]);
      IF s != '' THEN
        w := getorcreate_word_id(s);
        IF w IS NOT NULL AND NOT (ARRAY[w] <@ result) THEN
          result := result || w;
        END IF;
      END IF;
    END LOOP;
  END IF;

  s := regexp_replace(src, '市$', '');
  IF s != src THEN
    s := make_standard_name(s);
    IF s != '' THEN
      w := getorcreate_name_id(s, src);
      IF NOT (ARRAY[w] <@ result) THEN
        result := result || w;
      END IF;
    END IF;
  END IF;

  RETURN result;
END;
$$
LANGUAGE plpgsql;
