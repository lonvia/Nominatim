-- SPDX-License-Identifier: GPL-2.0-only
--
-- This file is part of Nominatim. (https://nominatim.org)
--
-- Copyright (C) 2025 by the Nominatim developer community.
-- For a full list of authors see the git log.
 
-- Functions for accessing the token index in search_name.

CREATE OR REPLACE FUNCTION search_name_lookup_tokens(tokens integer[])
  RETURNS integer[]
  AS $$
    SELECT array_agg(n) FROM unnest(tokens) n WHERE n >= 0
  $$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION search_name_all_tokens(tokens integer[])
  RETURNS integer[]
  AS $$
    SELECT ARRAY(SELECT abs(unnest(tokens)))
  $$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION make_restrict_tokens(tokens integer[])
  RETURNS integer[]
  AS $$
    SELECT ARRAY(SELECT -abs(unnest(tokens)));
  $$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
