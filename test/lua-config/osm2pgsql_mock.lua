busted = require('busted')

-- fake "osm2pgsql" table for test, usually created by the main C++ program
osm2pgsql = {
    define_table = function()
        return { add_row = busted.spy.new(function(self, k) end) }
    end,

    create_test_object = function(o)
        o.id = o.id or 1234
        o.tags = o.tags or {}
        o.grab_tag = function(self, key)
            local v = self.tags[key]
            self.tags[key] = nil
             return v
        end

        return o
    end
}

-- load the init.lua script that is normally run by the main C++ program
package.path = '../../osm2pgsql/src/?.lua'
require('init')

return osm2pgsql
