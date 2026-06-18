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
  SELECT (info->>'full')::INTEGER[]
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION token_get_name_search_partials(info JSONB)
  RETURNS tsvector
AS $$
  SELECT (info->>'part')::tsvector
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


-- Return the housenumber tokens applicable for the place.
CREATE OR REPLACE FUNCTION token_get_housenumber_search_tokens(info JSONB)
  RETURNS INTEGER[]
AS $$
  SELECT (info->>'hnrt')::INTEGER[]
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
  SELECT info->>'street' is not null or info->'addr'->>'place' is null;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION token_has_addr_street(info JSONB)
  RETURNS BOOLEAN
AS $$
  SELECT info->>'st' is not null and info->>'st' != '{}';
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION token_has_addr_place(info JSONB)
  RETURNS BOOLEAN
AS $$
  SELECT info->'addr'->>'place' is not null;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION token_matches_street(info JSONB, street_tokens INTEGER[])
  RETURNS BOOLEAN
AS $$
  SELECT (info->>'st')::INTEGER[] && street_tokens
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION token_addr_place_search_tokens(info JSONB)
  RETURNS INTEGER[]
AS $$
  SELECT (info->'addr'->'place'->>'full')::INTEGER[]
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION token_matches_place(info JSONB, tokens INTEGER[])
  RETURNS BOOLEAN
AS $$
  SELECT (info->'addr'->'place'->>'match')::INTEGER[] <@ tokens;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION token_addr_place_search_partials(info JSONB)
  RETURNS tsvector
AS $$
  SELECT (info->'addr'->'place'->>'part')::tsvector
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

DROP TYPE IF EXISTS TokenAddressInfo;
CREATE TYPE TokenAddressInfo AS (
  key TEXT,
  match_tokens INTEGER[],
  full_tokens INTEGER[],
  partials tsvector
);

CREATE OR REPLACE FUNCTION token_get_address_info(info JSONB)
  RETURNS SETOF TokenAddressInfo
AS $$
  SELECT addr.key as key,
         (addr.value->>'match')::INTEGER[] as match_tokens,
         (addr.value->>'full')::INTEGER[] as full_tokens,
         (addr.value->>'part')::tsvector as partials
    FROM jsonb_each(info->'addr') as addr WHERE addr.key != 'place';
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION token_matches_address(info JSONB, key TEXT, tokens INTEGER[])
  RETURNS BOOLEAN
AS $$
  SELECT (info->'addr'->key->>'match')::INTEGER[] <@ tokens;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION token_get_postcode(info JSONB)
  RETURNS TEXT
AS $$
  SELECT info->>'pc';
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
DECLARE
  rec RECORD;
BEGIN
  SELECT id, token_source.variants INTO token_id, actual_variants
    FROM token_source
    WHERE type = typ AND token = tok AND attributes = attr;

  created := token_id is NULL;

  IF created THEN
    FOR rec IN
      INSERT INTO token_source (type, token, attributes, variants)
        VALUES (typ, tok, attr, variants)
        RETURNING id
    LOOP
      token_id := rec.id;
    END LOOP;
  END IF;
END;
$$
LANGUAGE plpgsql;
