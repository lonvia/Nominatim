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
