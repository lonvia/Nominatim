-- Indices used for /search API.
-- These indices are created only after the indexing process is done.

CREATE INDEX idx_search_name_nameaddress_vector ON search_name USING GIN (nameaddress_vector) WITH (fastupdate = off) {ts:search-index};
CREATE INDEX idx_search_name_name_vector ON search_name USING GIN (name_vector) WITH (fastupdate = off) {ts:search-index};
CREATE INDEX idx_search_name_centroid ON search_name USING GIST (centroid) {ts:search-index};

CREATE INDEX idx_placex_classtype ON placex USING GIST(centroid) INCLUDE (type) {ts:search-index} WHERE rank_address = 30 and class not in ('place', 'building');
