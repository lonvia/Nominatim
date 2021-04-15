DROP TABLE IF EXISTS word;
CREATE TABLE word (
  word_id INTEGER,
  word_token text NOT NULL,
  word text,
  class text,
  type text,
  country_code varchar(2),
  search_name_count INTEGER,
  operator TEXT
) {{db.tablespace.search_data}};

CREATE INDEX idx_word_word_token ON word
    USING BTREE (word_token) {{db.tablespace.search_index}};
GRANT SELECT ON word TO "{{config.DATABASE_WEBUSER}}";

DROP SEQUENCE IF EXISTS seq_word;
CREATE SEQUENCE seq_word start 1;
GRANT SELECT ON seq_word to "{{config.DATABASE_WEBUSER}}";

-- Used by getorcreate_word_id to ignore frequent partial words.
-- Must be set to a fixed number on import and then never changed.
CREATE OR REPLACE FUNCTION get_maxwordfreq()
  RETURNS integer AS
  $$ SELECT {{config.MAX_WORD_FREQUENCY}} as maxwordfreq $$
LANGUAGE SQL IMMUTABLE;
