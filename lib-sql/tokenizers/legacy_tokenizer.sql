CREATE OR REPLACE FUNCTION token_get_name_search_tokens(info JSONB)
  RETURNS INTEGER[]
AS $$
  SELECT (info->>'names')::INTEGER[]
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION token_strip_info(info JSONB)
  RETURNS JSONB
AS $$
  SELECT NULL::JSONB;
$$ LANGUAGE SQL IMMUTABLE STRICT;
