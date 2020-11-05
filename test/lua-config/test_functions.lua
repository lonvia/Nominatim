-- fake "osm2pgsql" table for test, usually created by the main C++ program
osm2pgsql = {
    define_table = function()
        return {
            rows = {},
            clear = function(self) self.rows = {} end,
            add_row = function(self, data) self.rows[#self.rows + 1] = data end
        }
    end
}

-- load the init.lua script that is normally run by the main C++ program
package.path = '../../osm2pgsql/src/?.lua'
require('init')

print("Running Lua tests...")

local o = osm2pgsql

dofile('../../settings/flex.lua')

function place(o)
    o.object = { tags = o.tags }
    o.tags = nil
    o.geometry_type = o.geometry_type or 'point'
    o.admin_level = o.admin_level or 15
    o.name_tags = o.name_tags or {}
    o.address_tags = o.address_tags or {}

    return o
end

function osm(tags)
    return {
        object = {
            tags = tags,
            grab_tag = function(self, key)
                local v = self.tags[key]
                self.tags[key] = nil
                return v
            end
        }
    }
end

function assert_table(expected, actual)
    for k, v in pairs(expected) do
        assert(actual[k] == v)
    end
    for k, v in pairs(actual) do
        assert(expected[k] == v)
    end
end
-- ---------------------------------------------------------------------------

-- test add_row
do
    local p = place{name = {name = 'The the'}}
    add_row('place', 'city', p)
    assert(1 == #place_table.rows)
    assert('place' == place_table.rows[1].attrs.class)
    assert('city' == place_table.rows[1].attrs.type)
    assert(p.has_entry)
end

-- extract_address_tags
do
    local o = osm{country_code = 'de', ['addr:country'] = 'Deutschland'}
    extract_address_tags(o)
    assert(nil ~= o.address_tags.country)
    assert(next(o.object.tags) == nil)
end

do
    local o = osm{postal_code = '123', ['addr:postcode'] = '123'}
    extract_address_tags(o)
    assert_table({postcode = '123'}, o.address_tags)
    assert(next(o.object.tags) == nil)
end

do
    local o = osm{name = 'bar', ['tiger:county'] = 'Nowhere, CO'}
    extract_address_tags(o)
    assert_table({county = 'Nowhere county'}, o.address_tags)
    assert_table({name = 'bar'}, o.object.tags)
end

do
    local o = osm{['addr:city'] = 'A', ['is_in:county'] = 'B', addr_place = 'C'}
    extract_address_tags(o)
    assert_table({city = 'A', county = 'B'}, o.address_tags)
    assert_table({addr_place = 'C'}, o.object.tags)
end
