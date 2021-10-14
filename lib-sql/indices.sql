-- Indices used only during search and update.
-- These indices are created only after the indexing process is done.

DROP INDEX IF EXISTS idx_placex_rank_address_sector;
DROP INDEX IF EXISTS idx_placex_rank_boundaries_sector;

CREATE INDEX IF NOT EXISTS idx_place_addressline_address_place_id
  ON place_addressline USING BTREE (address_place_id) {{db.tablespace.search_index}};

-- Usage: - queryCoutry()
--        - lookupInCountry()
CREATE INDEX IF NOT EXISTS idx_placex_countries
  ON placex USING BTREE(country_code) {{db.tablespace.search_index}}
  WHERE rank_search = 4;

-- Usage: - export()
CREATE INDEX IF NOT EXISTS idx_placex_rank_address
  ON placex USING BTREE (rank_address) {{db.tablespace.search_index}}
  WHERE rank_address not in (26, 30); -- index use for these levels not recommended

-- Usage: -- invalidation when deleting
          -- invalidation of housenumbers for name changes on streets
          -- queryHouseNumber()
          -- /details
CREATE INDEX IF NOT EXISTS idx_placex_parent_place_id
  ON placex USING BTREE (parent_place_id) {{db.tablespace.search_index}}
  WHERE parent_place_id IS NOT NULL;

-- Usage: - invalidation of spatially related objects of incomming and deleted places
--        - various reverse and search queries
CREATE INDEX IF NOT EXISTS idx_placex_geometry
  ON placex USING GIST (geometry) {{db.tablespace.search_index}};

-- Usage: - lookupPolygon()
CREATE INDEX IF NOT EXISTS idx_placex_geometry_reverse_lookupPolygon
  ON placex USING gist (geometry) {{db.tablespace.search_index}}
  WHERE St_GeometryType(geometry) in ('ST_Polygon', 'ST_MultiPolygon')
    AND rank_address between 5 and 25 AND type != 'postcode'
    AND name is not null AND linked_place_id is null;

CREATE INDEX IF NOT EXISTS idx_osmline_parent_place_id
  ON location_property_osmline USING BTREE (parent_place_id) {{db.tablespace.search_index}};

CREATE INDEX IF NOT EXISTS idx_osmline_parent_osm_id
  ON location_property_osmline USING BTREE (osm_id) {{db.tablespace.search_index}};

CREATE INDEX IF NOT EXISTS idx_postcode_postcode
  ON location_postcode USING BTREE (postcode) {{db.tablespace.search_index}};

-- Indices only needed for updating.

{% if drop %}
  DROP INDEX IF EXISTS idx_placex_geometry_admin_boundary;
  DROP INDEX IF EXISTS idx_placex_geometry_place_areas;
  DROP INDEX IF EXISTS idx_placex_geometry_lower_rank_ways;
  DROP INDEX IF EXISTS idx_placex_geometry_addressable_areas;
  DROP INDEX IF EXISTS idx_placex_geometry_buildings;
  DROP INDEX IF EXISTS idx_placex_wikidata;
{% else %}
  CREATE INDEX idx_placex_rank_address_pending
    ON placex USING BTREE (rank_address, geometry_sector) {{db.tablespace.address_index}}
    WHERE indexed_status > 0;

  CREATE INDEX idx_placex_rank_boundaries_pending
    ON placex USING BTREE (rank_search, geometry_sector) {{db.tablespace.address_index}}
    WHERE class = 'boundary' and type = 'administrative'
          AND indexed_status > 0;

  CREATE INDEX IF NOT EXISTS idx_location_area_country_place_id
    ON location_area_country USING BTREE (place_id) {{db.tablespace.address_index}};

  CREATE UNIQUE INDEX IF NOT EXISTS idx_place_osm_unique
    ON place USING btree(osm_id, osm_type, class, type) {{db.tablespace.address_index}};
{% endif %}

-- Indices only needed for search.

{% if 'search_name' in db.tables %}
  CREATE INDEX IF NOT EXISTS idx_search_name_nameaddress_vector
    ON search_name USING GIN (nameaddress_vector) WITH (fastupdate = off) {{db.tablespace.search_index}};
  CREATE INDEX IF NOT EXISTS idx_search_name_name_vector
    ON search_name USING GIN (name_vector) WITH (fastupdate = off) {{db.tablespace.search_index}};
  CREATE INDEX IF NOT EXISTS idx_search_name_centroid
    ON search_name USING GIST (centroid) {{db.tablespace.search_index}};

  {% if postgres.has_index_non_key_column %}
    -- Usage: - queryNamedPlace()
    CREATE INDEX IF NOT EXISTS idx_placex_housenumber
      ON placex USING btree (parent_place_id) INCLUDE (housenumber) WHERE housenumber is not null;
    CREATE INDEX IF NOT EXISTS idx_osmline_parent_osm_id_with_hnr
      ON location_property_osmline USING btree(parent_place_id) INCLUDE (startnumber, endnumber);
  {% endif %}
{% endif %}
