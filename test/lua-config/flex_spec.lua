local osm2pgsql = require('osm2pgsql_mock')

dofile('../../settings/flex.lua')

describe("add_row()", function()
    it('adds the row', function ()
        add_row('place', 'city', 
                {
                    object = { tags = {foo = 'bar'} },
                    geometry_type = 'point',
                    admin_level = 15,
                    name_tags = { name = 'First', ['name:de'] = 'Second' },
                    address_tags = {}
                })

        assert.spy(place_table.add_row).was_called_with(place_table, {
            attrs = {
                class = 'place',
                type = 'city',
                admin_level = 15,
                name = { name = 'First', ['name:de'] = 'Second' },
                address = {},
                extratags = {foo = 'bar'},
                geometry = { create = 'point' }
            }
        })
    end)
end)

describe("extract_address_tags()", function ()
    local call_with = function(tags)
        local o = { object = osm2pgsql.create_test_object{tags = tags} }
        extract_address_tags(o)
        return o
    end

    it("extracts country tags", function ()
        local o = call_with{country_code = 'de', ['addr:country'] = 'Deutschland'}

        assert.truthy(o.address_tags.country)
        assert.same({}, o.object.tags)
    end)

    it("extracts postcodes", function ()
        local o = call_with{postal_code = '123', ['addr:postcode'] = '123'}

        assert.same({postcode = '123'}, o.address_tags)
        assert.same({}, o.object.tags)
    end)

    it("handles tiger:county tags", function ()
        local o = call_with{name = 'bar', ['tiger:county'] = 'Nowhere, CO'}

        assert.same({county = 'Nowhere county'}, o.address_tags)
        assert.same({name = 'bar'}, o.object.tags)
    end)

    it("handles addr:* and is_in:* tags", function ()
        local o = call_with{['addr:city'] = 'A', ['is_in:county'] = 'B', addr_place = 'C'}

        assert.same({city = 'A', county = 'B'}, o.address_tags)
        assert.same({addr_place = 'C'}, o.object.tags)
    end)
end)

describe("extract_name_tags()", function ()
    local call_with = function(tags)
        local o = { object = osm2pgsql.create_test_object{tags = tags} }
        extract_name_tags(o)
        return o
    end

    it("handles name tags", function ()
        local o = call_with{name = 'Something', brand = 'Else', other = '1'}

        assert.same({name = 'Something', brand = 'Else'}, o.name_tags)
        assert.same({other = '1'}, o.object.tags)
        assert.True(o.has_names)
    end)

    it("handles localized name tags", function ()
        local o = call_with{['old_name:de'] = 'A', ['name:xyz'] = 'B',
                            ['brand:wikidata'] = 'C', ['short_name:1'] = 'D',
                            ['loc_name:zh_Hunt'] = 'E', ['name:zh-Hunt'] = 'F'}

        assert.same({['old_name:de'] = 'A', ['name:xyz'] = 'B',
                     ['loc_name:zh_Hunt'] = 'E', ['name:zh-Hunt'] = 'F'},
                     o.name_tags)
        assert.same({['brand:wikidata'] = 'C', ['short_name:1'] = 'D'},
                    o.object.tags)
        assert.True(o.has_names)
    end)

    it("handles reference tags", function ()
       local o = call_with{ref = '1', refs = '34;12', iata = 'XDS'}

        assert.same({ref = '1', iata = 'XDS'}, o.name_tags)
        assert.same({refs = '34;12'}, o.object.tags)
        assert.False(o.has_names)
    end)
end)

describe("add_unless_value_is()", function ()
    local func = add_unless_value_is{'yes', 'no'}
    local dummy = {object = { tags = {} } }

    after_each(function () place_table.add_row:clear() end)

    it("allows most keys", function ()
        func('amenity', 'prison', dummy)
        assert.spy(place_table.add_row).was_called()
    end)

    it("rejects certain keys", function ()
        func('amenity', 'yes', dummy)
        assert.spy(place_table.add_row).was_not_called()
    end)
end)

describe("add_named_unless_value_is()", function ()
    local func = add_named_unless_value_is{'yes', 'no'}
    local dummy = function(has_names)
        return {object = { tags = {} }, has_names = has_names}
    end

    after_each(function () place_table.add_row:clear() end)

    it("allows most keys when there is a name", function ()
        func('amenity', 'prison', dummy(true))
        assert.spy(place_table.add_row).was_called()
    end)

    it("allows no keys when there is no name", function ()
        func('amenity', 'prison', dummy(false))
        assert.spy(place_table.add_row).was_not_called()
    end)

     it("rejects certain keys", function ()
        func('amenity', 'yes', dummy(true))
        assert.spy(place_table.add_row).was_not_called()
    end)
end)


describe("add_with_domain_name()" , function ()
    after_each(function () place_table.add_row:clear() end)

    function place(tags, names)
        return {
            object = osm2pgsql.create_test_object{tags = tags},
            admin_level = 15,
            name_tags = names,
            address_tags = {},
            geometry_type = 'point',
        }
    end

    it("sets special key-dependent names", function ()
        local o = place({['bridge:name'] = 'A bridge'},
                        {name = 'Street name'})

        add_with_domain_name('bridge', 'yes', o)

        assert.spy(place_table.add_row).was_called_with({
            attrs = {
                class = 'bridge',
                type = 'yes',
                admin_level = 15,
                name = {name = 'A bridge'},
                address = {},
                extratags = {},
                geometry = { create = 'point' }
            }
        })
        assert.same({['bridge:name'] = 'A bridge'}, o.object.tags)
        assert.same({name = 'Street name'}, o.name_tags)
    end)
end)
