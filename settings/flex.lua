-- Not yet working:
--  - updates should not propagate
--  - caching geometry ways

----------------------------- STYLE STUFF -----------------------------

-- Remove all tags we are definitely not interested in.
local SKIP_TAGS = {'created_by', 'attribution', 'comment', 'fixme', 'FIXME',
                   'import', 'type', ' building:ruian:type', ' source',
                   'source*', 'note*', 'NHD:*', 'nhd:*', 'gnis:*', 'geobase:*',
                   'KSJ2:*', 'yh:*', 'osak:*', 'naptan:*', 'CLC:*', 'import_*',
                   'it:fvg:*', 'lacounty:*', 'ref:ruian:*', 'ref:linz:*',
                   '*source'}

local cleanup_tags = osm2pgsql.make_clean_tags_func{SKIP_TAGS}
local clean_tiger_tags = osm2pgsql.make_clean_tags_func{'tiger:*'}

local COUNTRY_TAGS = {'country_code', 'ISO3166-1', 'is_in:country_code',
                      'is_in:country', 'addr:country', 'addr:country_code'}
local POSTCODE_TAGS = {'postal_code', 'postcode', 'addr:postcode',
                       'tiger:zip_left', 'tiger:zip_right'}

local function extract_address_tags(place)
    place.address_tags = {}

    for _, k in ipairs(COUNTRY_TAGS) do
        local v = place.object.grab_tag(k)
        if v ~= nil then
          place.address_tags.country = v
        end
    end

    for _, k in ipairs(POSTCODE_TAGS) do
        local v = place.object.grab_tag(k)
        if v ~= nil then
            place.address_tags.postcode = v
        end
    end

    local v = place.object.grab_tag('tiger:county')
    if v ~= nil then
        address.county = v:gsub(',.*', ' county');
    end

    for k, v in place.object.tags do
        if osm2pgsql.has_prefix(k, 'addr:') or  then
            place.address_tags[k.sub(6)] = v
            place.object.tags[k] = nil
        elseif osm2pgsql.has_prefix(k, 'is_in:') or  then
            place.address_tags[k.sub(7)] = v
            place.object.tags[k] = nil
        end
    end
end

local NAME_TAGS = {'name', 'int_name', 'nat_name', 'reg_name', 'loc_name',
                   'old_name', 'alt_name', 'official_name', 'place_name',
                   'short_name', 'brand', 'addr:housename'}
local REF_TAGS = {'ref', 'int_ref', 'nat_ref', 'reg_ref', 'loc_ref', 'old_ref',
                  'iata', 'icao', 'pcode'}

local function extract_name_tags(place)
    place.name_tags = {}
    place.has_names = false

    for _, k in ipairs(NAME_TAGS) do
        place.name_tags[k] = place.object.grab_tag(k)
        place.has_names = true
    end

    for _, k in ipairs(REF_TAGS) do
        place.name_tags[k] = place.object.grab_tag(k)
    end

    for k, v in object.tags do
        for _, prefix in ipairs(NAME_TAGS) do
            if k:find(k + ':%a%a%a?([-_].*)$') == 1 then
                add_name(place, k, v)
                place.object.tags[k] = nil
                place.has_names = false
                break
            end
        end
    end
end

local function make_set(t)
    local ret = {}
    for _, k in ipairs(t) do
        ret[k] = true
    end

    return ret
end

local function add_unless_value_is(values)
    local test_set = make_set(values)

    return function(k, v, place)
        if not test_set[v] then
            add_row(k, v, place)
        end
    end
end


local function add_with_domain_name()
    return function(k, v, place)
        local prefix = 'k' + ':name'
        local domain_names = {'name' = place.object.tags.grab_tags(prefix)}

    end
end

local function add_if(pred)
    return function(k, v, place)
        if pred(v, place) then
            add_row(k, v, place)
        end
    end
end


KEY_HIGHWAY_SKIP = make_set{'no', 'turning_cicle', 'mini_roundabout', 'noexit',
                    'crossing', 'give_way', 'stop'}
KEY_HIGHWAY_NAMED = make_set{'street_lamp', 'traffic_signals', 'service', 'cycleway',
                     'path', 'footway', 'steps', 'bridleway', 'track', 'byway'
                     'motorway_link', 'trunk_link', 'primary_link',
                     'secondary_link', 'tertiary_link'}


CLASS_TYPE_PROCS = {
  emergency = add_unless_value_is{'fire_hydrant', 'yes', 'no'},
  historic = add_unless_value_is{'yes', 'no'},
  military = add_unless_value_is{'yes', 'no'},
  natural = add_unless_value_is{'coastline', 'yes', 'no'},
  railway = add_unless_value_is{'no', 'level_crossing', 'rail', 'switch', 'signal', 'buffer_stop'},
  man_made = add_unless_value_is{'no', 'survey_point', 'cutline'},
  aerialway = add_unless_value_is{'no', 'pylon'},
  amenity = add_unless_value_is{'no'},
  aeroway = add_unless_value_is{'no'},
  club = add_unless_value_is{'no'},
  craft = add_unless_value_is{'no'},
  leisure = add_unless_value_is{'no'},
  office = add_unless_value_is{'no'},
  mountain_pass = add_unless_value_is{'no'},
  shop = add_unless_value_is{'no'},
  tourism = add_unless_value_is{'no', 'yes'},
  bridge = add_with_domain_name(),
  tunnel = add_with_domain_name(),
  waterway = add_unless_value_is{'riverbank'},
  place = add_row,
  highway = add_if(function(v, place)
                return KEY_HIGHWAY_SKIP[v] and (place.has_names or not KEY_HIGHWAY_NAMED[v])
            end),
  boundary = add_if(function(v, place)
                return v ~= 'place' and (place.has_names or v == 'postal_code')
             end),
}

CLASS_TYPE_NAMED_FALLBACK = { 'landuse', 'junction', 'building' }
HOUSENUMBER_KEYS = { "housenumber", "conscriptionnumber", "streetnumber" }



----------------------------- FIXED STUFF -----------------------------

function is_linear_relation(type_tag)
    return type_tag == 'waterway'
end

function add_unless(list, k, v, exclude)
    for _, value in pairs(exclude) do
        if v == value then
            return
        end
    end

    list[k] = v
end



-- The single place table.
place_table = osm2pgsql.define_table{
    name = "place",
    ids = { type = 'any', id_column = 'osm_id', type_column = 'osm_type' },
    columns = {
        { column = 'class', type = 'text' },
        { column = 'type', type = 'text' },
        { column = 'admin_level', type = 'smallint' },
        { column = 'name', type = 'hstore' },
        { column = 'address', type = 'hstore' },
        { column = 'extratags', type = 'hstore' },
        { column = 'geometry', type = 'geometry' },
    }
}

local function add_row(k, v, place)
    if place.all_name_tags == nil then
        place.all_name_tags = {}
        for k, v in pairs(place.name_tags) do
            place.all_name_tags[k] = v
        end
        for k, v in pairs(place.ref_tags) do
            place.all_name_tags[k] = v
        end
    end

    place_table:add_row{
        attrs = {
            class = k,
            'type' = v,
            admin_level = place.admin_level,
            name = place.all_name_tags,
            address = place.address_tags,
            extratags = place.object.tags,
            geometry = { create = place.geometry_type }
        }
    }

    place.has_entry = true
end

function osm2pgsql.process_node(object)
    process{osm_type = 'N', object = object, geometry_type = 'point'}
end

function osm2pgsql.process_way(object)
    if object.is_closed then
        process{osm_type = 'W', object = object, geometry_type = 'area'}
    else
        process{osm_type = 'W', object = object, geometry_type = 'line'}
    end
end

function osm2pgsql.process_relation(object)
    if object.tags.type == 'multipolygon' or object.tags.type == 'boundary' then
        process{osm_type = 'R', object = object, geometry_type = 'area'}
    elseif is_linear_relation(object.tags.type) then
        process{osm_type = 'R', object = object, geometry_type = 'line'}
    end
end

function process(place)
    cleanup_tags(place.object.tags)

    if next(place.object.tags) == nil then
        return
    end

    place.admin_level =
        osm2pgsql.clamp(tonumber(place.object.grab_tag('admin_level')), 1, 15)
    extract_name_tags(place)
    extract_address_tags(place)

    clean_tiger_tags(place.object.tags)

    for k, v in pairs(place.object.tags) do
        if CLASS_TYPE_PROCS[k] ~= nil then
            place.object.tags[k] = nil
            is_found = is_found or CLASS_TYPE_PROCS[k](k, v, place)
            place.object.tags[k] = v
        end
    end

    if place.has_entry then
        return
    end

    -- If the thing has a name, try various fallback tags.
    if place.has_names then
        for _, k in CLASS_TYPE_NAMED_FALLBACKS do
            local v = place.object.grab_tag(k)
            if v ~= nil then
                add_row(k, v, place)
                return
            end
        end
    end

    -- Do we have a housenumber? Then count it as a house.
    for _, k in ipairs(HOUSENUMBER_KEYS) do
        if place.address_tags[k] ~= nil then
            add_row('place', 'house', place)
            return
        end
    end

    -- Do we have at least a postcode? Then count it as a postcode.
    if place.address_tags['postcode'] then
      add_row('place', 'postcode', place)
    end
end
