-- Create all indexes on placex needed during import.

CREATE UNIQUE INDEX idx_place_id ON placex
  USING BTREE (place_id) {{db.tablespace.search_index}};

CREATE INDEX idx_placex_osmid ON placex
  USING BTREE (osm_type, osm_id) {{db.tablespace.search_index}};

------ Indexes used by the indexer on import.
--
-- These are changed after the import to only contain pending objects.

CREATE INDEX idx_placex_rank_address_sector ON placex
  USING BTREE (rank_address, geometry_sector) {{db.tablespace.address_index}};

CREATE INDEX idx_placex_rank_boundaries_sector ON placex
  USING BTREE (rank_search, geometry_sector) {{db.tablespace.address_index}}
  WHERE class = 'boundary' and type = 'administrative';

------ Indexes used by the update trigger.

-- Usage: - removing linkage status on update
--        - lookup linked places for /details
CREATE INDEX idx_placex_linked_place_id ON placex
  USING BTREE (linked_place_id) {{db.tablespace.address_index}}
  WHERE linked_place_id IS NOT NULL;

-- Usage: - check that admin boundaries do not overtake each other rank-wise
--        - check that place node in a admin boundary with the same address level
CREATE INDEX idx_placex_geometry_admin_boundary ON placex
  USING {{postgres.spgist_geom}} (geometry) {{db.tablespace.address_index}}
  WHERE osm_type = 'R' and class = 'boundary' and type = 'administrative';

-- Usage: - boundary is not completely contained in a place area
CREATE INDEX idx_placex_geometry_place_areas ON placex
  USING GIST (geometry) {{db.tablespace.address_index}}
  WHERE class = 'place' and rank_address < 24
        and ST_GeometryType(geometry) in ('ST_Polygon','ST_MultiPolygon');

-- Usage: - linking of similar named places to boundaries
--        - linking of place nodes with same type to boundaries
--        - lookupPolygon()
CREATE INDEX idx_placex_geometry_place_nodes ON placex
  USING {{postgres.spgist_geom}} (geometry) {{db.tablespace.search_index}}
  WHERE osm_type = 'N' and rank_search < 26
        and class = 'place' and type != 'postcode' and linked_place_id is null;

-- Usage: - is node part of a way?
--        - find parent of interpolation spatially
CREATE INDEX idx_placex_geometry_lower_rank_ways ON placex
  USING {{postgres.spgist_geom}} (geometry) {{db.tablespace.address_index}}
  WHERE osm_type = 'W' and rank_search >= 26;

-- Usage: - parenting of large-area or unparentable features
CREATE INDEX idx_placex_geometry_addressable_areas ON placex
  USING GIST (geometry)  {{db.tablespace.address_index}}
  WHERE rank_address between 5 and 25
        and ST_GeometryType(geometry) in ('ST_Polygon','ST_MultiPolygon');

-- Usage: - POI is within building with housenumber
CREATE INDEX idx_placex_geometry_buildings ON placex
  USING {{postgres.spgist_geom}} (geometry) {{db.tablespace.search_index}}
  WHERE address is not null and rank_search = 30
        and ST_GeometryType(geometry) in ('ST_Polygon','ST_MultiPolygon');

-- Usage: - linking place nodes by wikidata tag to boundaries
CREATE INDEX idx_placex_wikidata on placex
  USING BTREE ((extratags -> 'wikidata')) {{db.tablespace.address_index}}
  WHERE extratags ? 'wikidata' and class = 'place' and osm_type = 'N' and rank_search < 26;
