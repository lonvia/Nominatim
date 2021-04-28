CREATE OR REPLACE FUNCTION test_getorcreate_amenity(lookup_word TEXT, normalized_word TEXT,
                                               lookup_class text, lookup_type text)
  RETURNS INTEGER
  AS $$
DECLARE
  lookup_token TEXT;
  return_word_id INTEGER;
BEGIN
  lookup_token := ' '||trim(lookup_word);
  SELECT min(word_id) FROM word
  WHERE word_token = lookup_token and word = normalized_word
        and class = lookup_class and type = lookup_type
  INTO return_word_id;
  IF return_word_id IS NULL THEN
    return_word_id := nextval('seq_word');
    INSERT INTO word VALUES (return_word_id, lookup_token, normalized_word,
                             lookup_class, lookup_type, null, 0);
  END IF;
  RETURN return_word_id;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION test_getorcreate_amenityoperator(lookup_word TEXT,
                                                       normalized_word TEXT,
                                                       lookup_class text,
                                                       lookup_type text,
                                                       op text)
  RETURNS INTEGER
  AS $$
DECLARE
  lookup_token TEXT;
  return_word_id INTEGER;
BEGIN
  lookup_token := ' '||trim(lookup_word);
  SELECT min(word_id) FROM word
  WHERE word_token = lookup_token and word = normalized_word
        and class = lookup_class and type = lookup_type and operator = op
  INTO return_word_id;
  IF return_word_id IS NULL THEN
    return_word_id := nextval('seq_word');
    INSERT INTO word VALUES (return_word_id, lookup_token, normalized_word,
                             lookup_class, lookup_type, null, 0, op);
  END IF;
  RETURN return_word_id;
END;
$$
LANGUAGE plpgsql;

SELECT test_getorcreate_amenity(make_standard_name('Aerodrome'), 'aerodrome', 'aeroway', 'aerodrome');
SELECT test_getorcreate_amenity(make_standard_name('Aerodromes'), 'aerodromes', 'aeroway', 'aerodrome');
SELECT test_getorcreate_amenityoperator(make_standard_name('Aerodrome in'), 'aerodrome in', 'aeroway', 'aerodrome', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Aerodromes in'), 'aerodromes in', 'aeroway', 'aerodrome', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Aerodrome near'), 'aerodrome near', 'aeroway', 'aerodrome', 'near');
SELECT test_getorcreate_amenityoperator(make_standard_name('Aerodromes near'), 'aerodromes near', 'aeroway', 'aerodrome', 'near');
SELECT test_getorcreate_amenity(make_standard_name('Airport'), 'airport', 'aeroway', 'aerodrome');
SELECT test_getorcreate_amenity(make_standard_name('Airports'), 'airports', 'aeroway', 'aerodrome');
SELECT test_getorcreate_amenityoperator(make_standard_name('Airport in'), 'airport in', 'aeroway', 'aerodrome', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Airports in'), 'airports in', 'aeroway', 'aerodrome', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Airport near'), 'airport near', 'aeroway', 'aerodrome', 'near');
SELECT test_getorcreate_amenityoperator(make_standard_name('Airports near'), 'airports near', 'aeroway', 'aerodrome', 'near');
SELECT test_getorcreate_amenity(make_standard_name('Bar'), 'bar', 'amenity', 'bar');
SELECT test_getorcreate_amenity(make_standard_name('Bars'), 'bars', 'amenity', 'bar');
SELECT test_getorcreate_amenityoperator(make_standard_name('Bar in'), 'bar in', 'amenity', 'bar', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Bars in'), 'bars in', 'amenity', 'bar', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Bar near'), 'bar near', 'amenity', 'bar', 'near');
SELECT test_getorcreate_amenityoperator(make_standard_name('Bars near'), 'bars near', 'amenity', 'bar', 'near');
SELECT test_getorcreate_amenity(make_standard_name('Bar'), 'bar', 'amenity', 'pub');
SELECT test_getorcreate_amenity(make_standard_name('Bars'), 'bars', 'amenity', 'pub');
SELECT test_getorcreate_amenityoperator(make_standard_name('Bar in'), 'bar in', 'amenity', 'pub', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Bars in'), 'bars in', 'amenity', 'pub', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Bar near'), 'bar near', 'amenity', 'pub', 'near');
SELECT test_getorcreate_amenityoperator(make_standard_name('Bars near'), 'bars near', 'amenity', 'pub', 'near');
SELECT test_getorcreate_amenity(make_standard_name('Food'), 'food', 'amenity', 'restaurant');
SELECT test_getorcreate_amenity(make_standard_name('Food'), 'food', 'amenity', 'restaurant');
SELECT test_getorcreate_amenityoperator(make_standard_name('Food in'), 'food in', 'amenity', 'restaurant', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Food in'), 'food in', 'amenity', 'restaurant', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Food near'), 'food near', 'amenity', 'restaurant', 'near');
SELECT test_getorcreate_amenityoperator(make_standard_name('Food near'), 'food near', 'amenity', 'restaurant', 'near');
SELECT test_getorcreate_amenity(make_standard_name('Pub'), 'pub', 'amenity', 'bar');
SELECT test_getorcreate_amenity(make_standard_name('Pubs'), 'pubs', 'amenity', 'bar');
SELECT test_getorcreate_amenityoperator(make_standard_name('Pub in'), 'pub in', 'amenity', 'bar', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Pubs in'), 'pubs in', 'amenity', 'bar', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Pub near'), 'pub near', 'amenity', 'bar', 'near');
SELECT test_getorcreate_amenityoperator(make_standard_name('Pubs near'), 'pubs near', 'amenity', 'bar', 'near');
SELECT test_getorcreate_amenity(make_standard_name('Pub'), 'pub', 'amenity', 'pub');
SELECT test_getorcreate_amenity(make_standard_name('Pubs'), 'pubs', 'amenity', 'pub');
SELECT test_getorcreate_amenityoperator(make_standard_name('Pub in'), 'pub in', 'amenity', 'pub', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Pubs in'), 'pubs in', 'amenity', 'pub', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Pub near'), 'pub near', 'amenity', 'pub', 'near');
SELECT test_getorcreate_amenityoperator(make_standard_name('Pubs near'), 'pubs near', 'amenity', 'pub', 'near');
SELECT test_getorcreate_amenity(make_standard_name('Restaurant'), 'restaurant', 'amenity', 'restaurant');
SELECT test_getorcreate_amenity(make_standard_name('Restaurants'), 'restaurants', 'amenity', 'restaurant');
SELECT test_getorcreate_amenityoperator(make_standard_name('Restaurant in'), 'restaurant in', 'amenity', 'restaurant', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Restaurants in'), 'restaurants in', 'amenity', 'restaurant', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Restaurant near'), 'restaurant near', 'amenity', 'restaurant', 'near');
SELECT test_getorcreate_amenityoperator(make_standard_name('Restaurants near'), 'restaurants near', 'amenity', 'restaurant', 'near');
SELECT test_getorcreate_amenity(make_standard_name('Mural'), 'mural', 'artwork_type', 'mural');
SELECT test_getorcreate_amenity(make_standard_name('Murals'), 'murals', 'artwork_type', 'mural');
SELECT test_getorcreate_amenityoperator(make_standard_name('Mural in'), 'mural in', 'artwork_type', 'mural', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Murals in'), 'murals in', 'artwork_type', 'mural', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Mural near'), 'mural near', 'artwork_type', 'mural', 'near');
SELECT test_getorcreate_amenityoperator(make_standard_name('Murals near'), 'murals near', 'artwork_type', 'mural', 'near');
SELECT test_getorcreate_amenity(make_standard_name('Sculpture'), 'sculpture', 'artwork_type', 'sculpture');
SELECT test_getorcreate_amenity(make_standard_name('Sculptures'), 'sculptures', 'artwork_type', 'sculpture');
SELECT test_getorcreate_amenityoperator(make_standard_name('Sculpture in'), 'sculpture in', 'artwork_type', 'sculpture', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Sculptures in'), 'sculptures in', 'artwork_type', 'sculpture', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Sculpture near'), 'sculpture near', 'artwork_type', 'sculpture', 'near');
SELECT test_getorcreate_amenityoperator(make_standard_name('Sculptures near'), 'sculptures near', 'artwork_type', 'sculpture', 'near');
SELECT test_getorcreate_amenity(make_standard_name('Statue'), 'statue', 'artwork_type', 'statue');
SELECT test_getorcreate_amenity(make_standard_name('Statues'), 'statues', 'artwork_type', 'statue');
SELECT test_getorcreate_amenityoperator(make_standard_name('Statue in'), 'statue in', 'artwork_type', 'statue', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Statues in'), 'statues in', 'artwork_type', 'statue', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Statue near'), 'statue near', 'artwork_type', 'statue', 'near');
SELECT test_getorcreate_amenityoperator(make_standard_name('Statues near'), 'statues near', 'artwork_type', 'statue', 'near');
SELECT test_getorcreate_amenity(make_standard_name('ATM'), 'atm', 'atm', 'yes');
SELECT test_getorcreate_amenity(make_standard_name('ATMs'), 'atms', 'atm', 'yes');
SELECT test_getorcreate_amenityoperator(make_standard_name('ATM in'), 'atm in', 'atm', 'yes', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('ATMs in'), 'atms in', 'atm', 'yes', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('ATM near'), 'atm near', 'atm', 'yes', 'near');
SELECT test_getorcreate_amenityoperator(make_standard_name('ATMs near'), 'atms near', 'atm', 'yes', 'near');
SELECT test_getorcreate_amenity(make_standard_name('National Park'), 'national park', 'boundary', 'national_park');
SELECT test_getorcreate_amenity(make_standard_name('National Parks'), 'national parks', 'boundary', 'national_park');
SELECT test_getorcreate_amenityoperator(make_standard_name('National Park in'), 'national park in', 'boundary', 'national_park', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('National Parks in'), 'national parks in', 'boundary', 'national_park', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('National Park near'), 'national park near', 'boundary', 'national_park', 'near');
SELECT test_getorcreate_amenityoperator(make_standard_name('National Parks near'), 'national parks near', 'boundary', 'national_park', 'near');
SELECT test_getorcreate_amenity(make_standard_name('Changing table'), 'changing table', 'changing_table', 'yes');
SELECT test_getorcreate_amenity(make_standard_name('Changing tables'), 'changing tables', 'changing_table', 'yes');
SELECT test_getorcreate_amenityoperator(make_standard_name('Changing table in'), 'changing table in', 'changing_table', 'yes', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Changing tables in'), 'changing tables in', 'changing_table', 'yes', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Changing table near'), 'changing table near', 'changing_table', 'yes', 'near');
SELECT test_getorcreate_amenityoperator(make_standard_name('Changing tables near'), 'changing tables near', 'changing_table', 'yes', 'near');
SELECT test_getorcreate_amenity(make_standard_name('Roundabout'), 'roundabout', 'junction', 'roundabout');
SELECT test_getorcreate_amenity(make_standard_name('Roundabouts'), 'roundabouts', 'junction', 'roundabout');
SELECT test_getorcreate_amenityoperator(make_standard_name('Roundabout in'), 'roundabout in', 'junction', 'roundabout', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Roundabouts in'), 'roundabouts in', 'junction', 'roundabout', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Roundabout near'), 'roundabout near', 'junction', 'roundabout', 'near');
SELECT test_getorcreate_amenityoperator(make_standard_name('Roundabouts near'), 'roundabouts near', 'junction', 'roundabout', 'near');
SELECT test_getorcreate_amenity(make_standard_name('Plaque'), 'plaque', 'memorial', 'plaque');
SELECT test_getorcreate_amenity(make_standard_name('Plaques'), 'plaques', 'memorial', 'plaque');
SELECT test_getorcreate_amenityoperator(make_standard_name('Plaque in'), 'plaque in', 'memorial', 'plaque', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Plaques in'), 'plaques in', 'memorial', 'plaque', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Plaque near'), 'plaque near', 'memorial', 'plaque', 'near');
SELECT test_getorcreate_amenityoperator(make_standard_name('Plaques near'), 'plaques near', 'memorial', 'plaque', 'near');
SELECT test_getorcreate_amenity(make_standard_name('Statue'), 'statue', 'memorial', 'statue');
SELECT test_getorcreate_amenity(make_standard_name('Statues'), 'statues', 'memorial', 'statue');
SELECT test_getorcreate_amenityoperator(make_standard_name('Statue in'), 'statue in', 'memorial', 'statue', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Statues in'), 'statues in', 'memorial', 'statue', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Statue near'), 'statue near', 'memorial', 'statue', 'near');
SELECT test_getorcreate_amenityoperator(make_standard_name('Statues near'), 'statues near', 'memorial', 'statue', 'near');
SELECT test_getorcreate_amenity(make_standard_name('Stolperstein'), 'stolperstein', 'memorial', 'stolperstein');
SELECT test_getorcreate_amenity(make_standard_name('Stolpersteins'), 'stolpersteins', 'memorial', 'stolperstein');
SELECT test_getorcreate_amenity(make_standard_name('Stolpersteine'), 'stolpersteine', 'memorial', 'stolperstein');
SELECT test_getorcreate_amenityoperator(make_standard_name('Stolperstein in'), 'stolperstein in', 'memorial', 'stolperstein', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Stolpersteins in'), 'stolpersteins in', 'memorial', 'stolperstein', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Stolpersteine in'), 'stolpersteine in', 'memorial', 'stolperstein', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('Stolperstein near'), 'stolperstein near', 'memorial', 'stolperstein', 'near');
SELECT test_getorcreate_amenityoperator(make_standard_name('Stolpersteins near'), 'stolpersteins near', 'memorial', 'stolperstein', 'near');
SELECT test_getorcreate_amenityoperator(make_standard_name('Stolpersteine near'), 'stolpersteine near', 'memorial', 'stolperstein', 'near');
SELECT test_getorcreate_amenity(make_standard_name('War Memorial'), 'war memorial', 'memorial', 'war_memorial');
SELECT test_getorcreate_amenity(make_standard_name('War Memorials'), 'war memorials', 'memorial', 'war_memorial');
SELECT test_getorcreate_amenityoperator(make_standard_name('War Memorial in'), 'war memorial in', 'memorial', 'war_memorial', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('War Memorials in'), 'war memorials in', 'memorial', 'war_memorial', 'in');
SELECT test_getorcreate_amenityoperator(make_standard_name('War Memorial near'), 'war memorial near', 'memorial', 'war_memorial', 'near');
SELECT test_getorcreate_amenityoperator(make_standard_name('War Memorials near'), 'war memorials near', 'memorial', 'war_memorial', 'near');
CREATE INDEX idx_placex_classtype ON placex (class, type);CREATE TABLE place_classtype_aeroway_aerodrome AS SELECT place_id AS place_id,st_centroid(geometry) AS centroid FROM placex WHERE class = 'aeroway' AND type = 'aerodrome';
CREATE INDEX idx_place_classtype_aeroway_aerodrome_centroid ON place_classtype_aeroway_aerodrome USING GIST (centroid);
CREATE INDEX idx_place_classtype_aeroway_aerodrome_place_id ON place_classtype_aeroway_aerodrome USING btree(place_id);
GRANT SELECT ON place_classtype_aeroway_aerodrome TO "www-data";
CREATE TABLE place_classtype_amenity_bar AS SELECT place_id AS place_id,st_centroid(geometry) AS centroid FROM placex WHERE class = 'amenity' AND type = 'bar';
CREATE INDEX idx_place_classtype_amenity_bar_centroid ON place_classtype_amenity_bar USING GIST (centroid);
CREATE INDEX idx_place_classtype_amenity_bar_place_id ON place_classtype_amenity_bar USING btree(place_id);
GRANT SELECT ON place_classtype_amenity_bar TO "www-data";
CREATE TABLE place_classtype_amenity_pub AS SELECT place_id AS place_id,st_centroid(geometry) AS centroid FROM placex WHERE class = 'amenity' AND type = 'pub';
CREATE INDEX idx_place_classtype_amenity_pub_centroid ON place_classtype_amenity_pub USING GIST (centroid);
CREATE INDEX idx_place_classtype_amenity_pub_place_id ON place_classtype_amenity_pub USING btree(place_id);
GRANT SELECT ON place_classtype_amenity_pub TO "www-data";
CREATE TABLE place_classtype_amenity_restaurant AS SELECT place_id AS place_id,st_centroid(geometry) AS centroid FROM placex WHERE class = 'amenity' AND type = 'restaurant';
CREATE INDEX idx_place_classtype_amenity_restaurant_centroid ON place_classtype_amenity_restaurant USING GIST (centroid);
CREATE INDEX idx_place_classtype_amenity_restaurant_place_id ON place_classtype_amenity_restaurant USING btree(place_id);
GRANT SELECT ON place_classtype_amenity_restaurant TO "www-data";
CREATE TABLE place_classtype_artwork_type_mural AS SELECT place_id AS place_id,st_centroid(geometry) AS centroid FROM placex WHERE class = 'artwork_type' AND type = 'mural';
CREATE INDEX idx_place_classtype_artwork_type_mural_centroid ON place_classtype_artwork_type_mural USING GIST (centroid);
CREATE INDEX idx_place_classtype_artwork_type_mural_place_id ON place_classtype_artwork_type_mural USING btree(place_id);
GRANT SELECT ON place_classtype_artwork_type_mural TO "www-data";
CREATE TABLE place_classtype_artwork_type_sculpture AS SELECT place_id AS place_id,st_centroid(geometry) AS centroid FROM placex WHERE class = 'artwork_type' AND type = 'sculpture';
CREATE INDEX idx_place_classtype_artwork_type_sculpture_centroid ON place_classtype_artwork_type_sculpture USING GIST (centroid);
CREATE INDEX idx_place_classtype_artwork_type_sculpture_place_id ON place_classtype_artwork_type_sculpture USING btree(place_id);
GRANT SELECT ON place_classtype_artwork_type_sculpture TO "www-data";
CREATE TABLE place_classtype_artwork_type_statue AS SELECT place_id AS place_id,st_centroid(geometry) AS centroid FROM placex WHERE class = 'artwork_type' AND type = 'statue';
CREATE INDEX idx_place_classtype_artwork_type_statue_centroid ON place_classtype_artwork_type_statue USING GIST (centroid);
CREATE INDEX idx_place_classtype_artwork_type_statue_place_id ON place_classtype_artwork_type_statue USING btree(place_id);
GRANT SELECT ON place_classtype_artwork_type_statue TO "www-data";
CREATE TABLE place_classtype_atm_yes AS SELECT place_id AS place_id,st_centroid(geometry) AS centroid FROM placex WHERE class = 'atm' AND type = 'yes';
CREATE INDEX idx_place_classtype_atm_yes_centroid ON place_classtype_atm_yes USING GIST (centroid);
CREATE INDEX idx_place_classtype_atm_yes_place_id ON place_classtype_atm_yes USING btree(place_id);
GRANT SELECT ON place_classtype_atm_yes TO "www-data";
CREATE TABLE place_classtype_boundary_national_park AS SELECT place_id AS place_id,st_centroid(geometry) AS centroid FROM placex WHERE class = 'boundary' AND type = 'national_park';
CREATE INDEX idx_place_classtype_boundary_national_park_centroid ON place_classtype_boundary_national_park USING GIST (centroid);
CREATE INDEX idx_place_classtype_boundary_national_park_place_id ON place_classtype_boundary_national_park USING btree(place_id);
GRANT SELECT ON place_classtype_boundary_national_park TO "www-data";
CREATE TABLE place_classtype_changing_table_yes AS SELECT place_id AS place_id,st_centroid(geometry) AS centroid FROM placex WHERE class = 'changing_table' AND type = 'yes';
CREATE INDEX idx_place_classtype_changing_table_yes_centroid ON place_classtype_changing_table_yes USING GIST (centroid);
CREATE INDEX idx_place_classtype_changing_table_yes_place_id ON place_classtype_changing_table_yes USING btree(place_id);
GRANT SELECT ON place_classtype_changing_table_yes TO "www-data";
CREATE TABLE place_classtype_junction_roundabout AS SELECT place_id AS place_id,st_centroid(geometry) AS centroid FROM placex WHERE class = 'junction' AND type = 'roundabout';
CREATE INDEX idx_place_classtype_junction_roundabout_centroid ON place_classtype_junction_roundabout USING GIST (centroid);
CREATE INDEX idx_place_classtype_junction_roundabout_place_id ON place_classtype_junction_roundabout USING btree(place_id);
GRANT SELECT ON place_classtype_junction_roundabout TO "www-data";
CREATE TABLE place_classtype_memorial_plaque AS SELECT place_id AS place_id,st_centroid(geometry) AS centroid FROM placex WHERE class = 'memorial' AND type = 'plaque';
CREATE INDEX idx_place_classtype_memorial_plaque_centroid ON place_classtype_memorial_plaque USING GIST (centroid);
CREATE INDEX idx_place_classtype_memorial_plaque_place_id ON place_classtype_memorial_plaque USING btree(place_id);
GRANT SELECT ON place_classtype_memorial_plaque TO "www-data";
CREATE TABLE place_classtype_memorial_statue AS SELECT place_id AS place_id,st_centroid(geometry) AS centroid FROM placex WHERE class = 'memorial' AND type = 'statue';
CREATE INDEX idx_place_classtype_memorial_statue_centroid ON place_classtype_memorial_statue USING GIST (centroid);
CREATE INDEX idx_place_classtype_memorial_statue_place_id ON place_classtype_memorial_statue USING btree(place_id);
GRANT SELECT ON place_classtype_memorial_statue TO "www-data";
CREATE TABLE place_classtype_memorial_stolperstein AS SELECT place_id AS place_id,st_centroid(geometry) AS centroid FROM placex WHERE class = 'memorial' AND type = 'stolperstein';
CREATE INDEX idx_place_classtype_memorial_stolperstein_centroid ON place_classtype_memorial_stolperstein USING GIST (centroid);
CREATE INDEX idx_place_classtype_memorial_stolperstein_place_id ON place_classtype_memorial_stolperstein USING btree(place_id);
GRANT SELECT ON place_classtype_memorial_stolperstein TO "www-data";
CREATE TABLE place_classtype_memorial_war_memorial AS SELECT place_id AS place_id,st_centroid(geometry) AS centroid FROM placex WHERE class = 'memorial' AND type = 'war_memorial';
CREATE INDEX idx_place_classtype_memorial_war_memorial_centroid ON place_classtype_memorial_war_memorial USING GIST (centroid);
CREATE INDEX idx_place_classtype_memorial_war_memorial_place_id ON place_classtype_memorial_war_memorial USING btree(place_id);
GRANT SELECT ON place_classtype_memorial_war_memorial TO "www-data";
DROP INDEX idx_placex_classtype;

DROP FUNCTION test_getorcreate_amenity;
DROP FUNCTION test_getorcreate_amenityoperator;
