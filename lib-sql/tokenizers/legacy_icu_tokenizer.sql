CREATE OR REPLACE FUNCTION token_get_name_search_tokens(info JSONB)
  RETURNS INTEGER[]
AS $$
  SELECT (info->>'names')::INTEGER[]
$$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION token_get_name_match_tokens(info JSONB)
  RETURNS INTEGER[]
AS $$
  SELECT (info->>'names')::INTEGER[]
$$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION token_get_housenumber_search_tokens(info JSONB)
  RETURNS INTEGER[]
AS $$
  SELECT (info->>'hnr_search')::INTEGER[]
$$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION token_strip_info(info JSONB)
  RETURNS JSONB
AS $$
  SELECT NULL::JSONB;
$$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION token_addr_street_match_tokens(info JSONB)
  RETURNS INTEGER[]
AS $$
  SELECT (info->>'street_match')::INTEGER[]
$$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION token_addr_place_match_tokens(info JSONB)
  RETURNS INTEGER[]
AS $$
  SELECT (info->>'place_match')::INTEGER[]
$$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION token_addr_place_search_tokens(info JSONB)
  RETURNS INTEGER[]
AS $$
  SELECT (info->>'place_search')::INTEGER[]
$$ LANGUAGE SQL IMMUTABLE STRICT;


DROP TYPE IF EXISTS token_addresstoken CASCADE;
CREATE TYPE token_addresstoken AS (
  key TEXT,
  match_tokens INT[],
  search_tokens INT[]
);


CREATE OR REPLACE FUNCTION token_get_address_tokens(info JSONB)
  RETURNS SETOF token_addresstoken
AS $$
  SELECT key, (value->>1)::int[], (value->>0)::int[] FROM jsonb_each(info->'addr');
$$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION token_normalized_postcode(postcode TEXT)
  RETURNS TEXT
AS $$
  SELECT CASE WHEN postcode SIMILAR TO '%(,|;)%' THEN NULL ELSE upper(trim(postcode))END;
$$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION token_normalized_housenumber(info JSONB)
  RETURNS TEXT
AS $$
  SELECT info->>'hnr_match';
$$ LANGUAGE SQL IMMUTABLE STRICT;

-- --------------- private functions ----------------------------


-- USED
CREATE OR REPLACE FUNCTION getorcreate_housenumber_id(lookup_word TEXT)
  RETURNS INTEGER
  AS $$
DECLARE
  lookup_token TEXT;
  return_word_id INTEGER;
BEGIN
  lookup_token := ' ' || lookup_word;
  SELECT word_id FROM word
    WHERE word_token = lookup_token and class='place' and type='house'
    LIMIT 1
    INTO return_word_id;
  IF return_word_id IS NULL THEN
    return_word_id := nextval('seq_word');
    INSERT INTO word VALUES (return_word_id, lookup_token, null,
                             'place', 'house', null, 0);
  END IF;
  RETURN return_word_id;
END;
$$
LANGUAGE plpgsql;

-- USED
CREATE OR REPLACE FUNCTION create_postcode_id(postcode TEXT)
  RETURNS INTEGER
  AS $$
DECLARE
  lookup_token TEXT;
  return_word_id INTEGER;
BEGIN
  lookup_token := ' ' || postcode; -- XXX THIS IS WRONG!!!
  SELECT min(word_id) FROM word
    WHERE word_token = lookup_token and word = postcode
          and class='place' and type='postcode'
    INTO return_word_id;
  IF return_word_id IS NULL THEN
    return_word_id := nextval('seq_word');
    INSERT INTO word VALUES (return_word_id, lookup_token, postcode,
                             'place', 'postcode', null, 0);
  END IF;
  RETURN return_word_id;
END;
$$
LANGUAGE plpgsql STRICT;

-- USED
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
  words := string_to_array(lookup_word, ' ');
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

-- USED
-- returns NULL if the word is too common
CREATE OR REPLACE FUNCTION getorcreate_word_id(lookup_word TEXT) 
  RETURNS INTEGER
  AS $$
DECLARE
  return_word_id INTEGER;
  count INTEGER;
BEGIN
  SELECT min(word_id), max(search_name_count) FROM word
    WHERE word_token = lookup_word and class is null and type is null
    INTO return_word_id, count;
  IF return_word_id IS NULL THEN
    return_word_id := nextval('seq_word');
    INSERT INTO word VALUES (return_word_id, lookup_word, null, null, null, null, 0);
  ELSE
    IF count > get_maxwordfreq() THEN
      return_word_id := NULL;
    END IF;
  END IF;
  RETURN return_word_id;
END;
$$
LANGUAGE plpgsql;

-- XXX Function kept for initial postcode creation (UK and GB tables).
-- Needs to be removed when postcode creation is rewritten.
CREATE OR REPLACE FUNCTION getorcreate_postcode_id(postcode TEXT)
  RETURNS INTEGER
  AS $$
DECLARE
  lookup_token TEXT;
  lookup_word TEXT;
  return_word_id INTEGER;
BEGIN
  lookup_word := token_normalized_postcode(postcode);
  lookup_token := ' ' || lookup_word; -- XXX THIS IS WRONG!
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

-- USED
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

-- USED
-- Normalize a string and look up its name ids (full words).
-- XXX still used in Tiger and AUX imports
CREATE OR REPLACE FUNCTION word_ids_from_name(lookup_word TEXT)
  RETURNS INTEGER[]
  AS $$
DECLARE
  lookup_token TEXT;
  return_word_ids INTEGER[];
BEGIN
  lookup_token := ' '|| lookup_word;
  SELECT array_agg(word_id) FROM word
    WHERE word_token = lookup_token and class is null and type is null
    INTO return_word_ids;
  RETURN return_word_ids;
END;
$$
LANGUAGE plpgsql STABLE STRICT;


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
