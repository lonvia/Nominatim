-- Create all indexes on placex needed during import.

CREATE UNIQUE INDEX idx_place_id ON placex
  USING BTREE (place_id) {{db.tablespace.search_index}};

CREATE INDEX idx_placex_osmid ON placex
  USING BTREE (osm_type, osm_id) {{db.tablespace.search_index}};

CREATE INDEX idx_placex_linked_place_id ON placex
  USING BTREE (linked_place_id) {{db.tablespace.address_index}}
  WHERE linked_place_id IS NOT NULL;

CREATE INDEX idx_placex_rank_search ON placex
  USING BTREE (rank_search, geometry_sector) {{db.tablespace.address_index}};

CREATE INDEX idx_placex_geometry ON placex
  USING GIST (geometry) {{db.tablespace.search_index}};

CREATE INDEX idx_placex_geometry_buildings ON placex
  USING {{postgres.spgist_geom}} (geometry) {{db.tablespace.search_index}}
  WHERE address is not null and rank_search = 30
        and ST_GeometryType(geometry) in ('ST_Polygon','ST_MultiPolygon');

CREATE INDEX idx_placex_geometry_placenode ON placex
  USING {{postgres.spgist_geom}} (geometry) {{db.tablespace.search_index}}
  WHERE osm_type = 'N' and rank_search < 26
        and class = 'place' and type != 'postcode' and linked_place_id is null;

CREATE INDEX idx_placex_wikidata on placex
  USING BTREE ((extratags -> 'wikidata')) {{db.tablespace.address_index}}
  WHERE extratags ? 'wikidata' and class = 'place' and osm_type = 'N' and rank_search < 26;
