-- SPDX-License-Identifier: GPL-2.0-only
--
-- This file is part of Nominatim. (https://nominatim.org)
--
-- Copyright (C) 2026 by the Nominatim developer community.
-- For a full list of authors see the git log.

-- Get tokens used for searching the given place.
--
-- These are the tokens that will be saved in the search_name table.
CREATE OR REPLACE FUNCTION token_get_name_search_tokens(info JSONB)
  RETURNS INTEGER[]
AS $$
  SELECT (info->>'names')::INTEGER[]
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


-- Get tokens for matching the place name against others.
--
-- This should usually be restricted to full name tokens.
CREATE OR REPLACE FUNCTION token_get_name_match_tokens(info JSONB)
  RETURNS INTEGER[]
AS $$
  SELECT (info->>'names')::INTEGER[]
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


-- Return the housenumber tokens applicable for the place.
CREATE OR REPLACE FUNCTION token_get_housenumber_search_tokens(info JSONB)
  RETURNS INTEGER[]
AS $$
  SELECT (info->>'hnr_tokens')::INTEGER[]
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


-- Return the housenumber in the form that it can be matched during search.
CREATE OR REPLACE FUNCTION token_normalized_housenumber(info JSONB)
  RETURNS TEXT
AS $$
  SELECT info->>'hnr';
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION token_is_street_address(info JSONB)
  RETURNS BOOLEAN
AS $$
  SELECT info->>'street' is not null or info->>'place' is null;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION token_has_addr_street(info JSONB)
  RETURNS BOOLEAN
AS $$
  SELECT info->>'street' is not null and info->>'street' != '{}';
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION token_has_addr_place(info JSONB)
  RETURNS BOOLEAN
AS $$
  SELECT info->>'place' is not null;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION token_matches_street(info JSONB, street_tokens INTEGER[])
  RETURNS BOOLEAN
AS $$
  SELECT (info->>'street')::INTEGER[] && street_tokens
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION token_matches_place(info JSONB, place_tokens INTEGER[])
  RETURNS BOOLEAN
AS $$
  SELECT (info->>'place')::INTEGER[] <@ place_tokens
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION token_addr_place_search_tokens(info JSONB)
  RETURNS INTEGER[]
AS $$
  SELECT (info->>'place')::INTEGER[]
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION token_get_address_keys(info JSONB)
  RETURNS SETOF TEXT
AS $$
  SELECT * FROM jsonb_object_keys(info->'addr');
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION token_get_address_search_tokens(info JSONB, key TEXT)
  RETURNS INTEGER[]
AS $$
  SELECT (info->'addr'->>key)::INTEGER[];
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION token_matches_address(info JSONB, key TEXT, tokens INTEGER[])
  RETURNS BOOLEAN
AS $$
  SELECT (info->'addr'->>key)::INTEGER[] <@ tokens;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION token_get_postcode(info JSONB)
  RETURNS TEXT
AS $$
  SELECT info->>'postcode';
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


-- Return token info that should be saved permanently in the database.
CREATE OR REPLACE FUNCTION token_strip_info(info JSONB)
  RETURNS JSONB
AS $$
  SELECT NULL::JSONB;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

--------------- private functions ----------------------------------------------

-- Can be replaced with INSERT ... ON CONFLICT DO NOTHING RETURNING OLD... NEW
-- in Postgres 18.
CREATE OR REPLACE FUNCTION getorcreate_token_source(typ VARCHAR, tok TEXT,
                                                    attr HSTORE, variants TEXT[],
                                                    OUT created BOOLEAN,
                                                    OUT token_id INTEGER,
                                                    OUT actual_variants TEXT[])
AS $$
BEGIN
  SELECT id, variants INTO token_id, actual_variants
    FROM token_source
    WHERE type = typ AND token = tok AND attributes = attr;

  created := token_id is NULL;

  IF created THEN
    SELECT id INTO token_id FROM (
      INSERT INTO token_source (type, token, attributes, variants)
        VALUES (typ, tok, attr, variants)
        RETURNING id) x;
  END IF;
END;
$$
LANGUAGE plpgsql;
