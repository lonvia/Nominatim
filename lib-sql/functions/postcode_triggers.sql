-- SPDX-License-Identifier: GPL-2.0-only
--
-- This file is part of Nominatim. (https://nominatim.org)
--
-- Copyright (C) 2025 by the Nominatim developer community.
-- For a full list of authors see the git log.

-- Trigger functions for location_postcode table.


-- Trigger for updates of location_postcode
--
-- Computes the parent object the postcode most likely refers to.
-- This will be the place that determines the address displayed when
-- searching for this postcode.
CREATE OR REPLACE FUNCTION postcode_update()
  RETURNS TRIGGER
  AS $$
DECLARE
  partition SMALLINT;
  location RECORD;
BEGIN
    IF NEW.indexed_status != 0 OR OLD.indexed_status = 0 THEN
        RETURN NEW;
    END IF;

    NEW.indexed_date = now();

    partition := get_partition(NEW.country_code);

    SELECT * FROM get_postcode_rank(NEW.country_code, NEW.postcode)
      INTO NEW.rank_search, NEW.rank_address;

    NEW.parent_place_id = 0;
    FOR location IN
      SELECT place_id
        FROM getNearFeatures(partition, NEW.geometry, NEW.geometry, NEW.rank_search)
        WHERE NOT isguess ORDER BY rank_address DESC, distance asc LIMIT 1
    LOOP
        NEW.parent_place_id = location.place_id;
    END LOOP;

    RETURN NEW;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION postcode_delete()
  RETURNS TRIGGER
  AS $$
BEGIN
  UPDATE placex p SET indexed_status = 2
   WHERE p.postcode = OLD.postcode AND ST_Intersects(OLD.geometry, p.geometry);

  RETURN OLD;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION postcode_insert()
  RETURNS TRIGGER
  AS $$
DECLARE
  existing RECORD;
BEGIN
  SELECT * INTO existing FROM location_postcode p
    WHERE p.country_code = NEW.country_code AND p.postcode = NEW.postcode;

  IF existing.postcode is NULL THEN
    UPDATE placex p SET indexed_status = 2
     WHERE ST_Intersects(NEW.geometry, p.geometry)
     AND p.rank_address >= 22 AND not p.address ? 'postcode';

    -- new entry, just insert
    existing.indexed_status := 1;
    RETURN NEW;
  END IF;

  -- update: only when there are changes
  IF coalesce(NEW.osm_id, -1) != coalesce(existing.osm_id, -1)
     OR (NEW.osm_id is not null AND NEW.geometry::text != existing.geometry::text)
     OR (NEW.osm_id is null
         AND (abs(ST_X(existing.centroid) - ST_X(NEW.centroid)) > 0.0000001
              OR abs(ST_Y(existing.centroid) - ST_Y(NEW.centroid)) > 0.0000001))
  THEN
    UPDATE placex p SET indexed_status = 2
      WHERE ST_Intersects(ST_Difference(NEW.geometry, existing.geometry), p.geometry)
      AND p.rank_address >= 22 AND not p.address ? 'postcode';

    UPDATE placex p SET indexed_status = 2
      WHERE ST_Intersects(ST_Difference(existing.geometry, NEW.geometry), p.geometry)
      AND p.postcode = OLD.postcode;

    UPDATE location_postcode p
      SET osm_id = NEW.osm_id,
          indexed_status = 2,
          centroid = NEW.centroid,
          geometry = NEW.geometry
      WHERE p.country_code = NEW.country_code AND p.postcode = NEW.postcode;
  END IF;

  RETURN NULL;
END;
$$
LANGUAGE plpgsql;

