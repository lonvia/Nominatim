# Importing the Database

The following instructions explain how to create a Nominatim database
from an OSM planet file and how to keep the database up to date. It
is assumed that you have already successfully installed the Nominatim
software itself, if not return to the [installation page](Installation.md).

## Configuration setup in settings/local.php

The Nominatim server can be customized via the file `settings/local.php`
in the build directory. Note that this is a PHP file, so it must always
start like this:

    <?php

without any leading spaces.

There are lots of configuration settings you can tweak. Have a look
at `settings/default.php` for a full list. Most should have a sensible default.

#### Flatnode files

If you plan to import a large dataset (e.g. Europe, North America, planet),
you should also enable flatnode storage of node locations. With this
setting enabled, node coordinates are stored in a simple file instead
of the database. This will save you import time and disk storage.
Add to your `settings/local.php`:

    @define('CONST_Osm2pgsql_Flatnode_File', '/path/to/flatnode.file');

Replace the second part with a suitable path on your system and make sure
the directory exists. There should be at least 75GB of free space.

## Downloading additional data

### Wikipedia/Wikidata rankings

Wikipedia can be used as an optional auxiliary data source to help indicate
the importance of OSM features. Nominatim will work without this information
but it will improve the quality of the results if this is installed.
This data is available as a binary download:

    cd $NOMINATIM_SOURCE_DIR/data
    wget https://www.nominatim.org/data/wikimedia-importance.sql.gz

The file is about 400MB and adds around 4GB to the Nominatim database.

!!! tip
    If you forgot to download the wikipedia rankings, you can also add
    importances after the import. Download the files, then run
    `./utils/setup.php --import-wikipedia-articles`
    and `./utils/update.php --recompute-importance`.

### Great Britain, USA postcodes

Nominatim can use postcodes from an external source to improve searches that
involve a GB or US postcode. This data can be optionally downloaded:

    cd $NOMINATIM_SOURCE_DIR/data
    wget https://www.nominatim.org/data/gb_postcode_data.sql.gz
    wget https://www.nominatim.org/data/us_postcode_data.sql.gz

## Choosing the data to import

In its default setup Nominatim is configured to import the full OSM data
set for the entire planet. Such a setup requires a powerful machine with
at least 64GB of RAM and around 900GB of SSD hard disks. Depending on your
use case there are various ways to reduce the amount of data imported. This
section discusses these methods. They can also be combined.

### Using an extract

If you only need geocoding for a smaller region, then precomputed OSM extracts
are a good way to reduce the database size and import time.
[Geofabrik](https://download.geofabrik.de) offers extracts for most countries.
They even have daily updates which can be used with the update process described
[in the next section](../Update). There are also
[other providers for extracts](https://wiki.openstreetmap.org/wiki/Planet.osm#Downloading).

Please be aware that some extracts are not cut exactly along the country
boundaries. As a result some parts of the boundary may be missing which means
that Nominatim cannot compute the areas for some administrative areas.

### Dropping Data Required for Dynamic Updates

About half of the data in Nominatim's database is not really used for serving
the API. It is only there to allow the data to be updated from the latest
changes from OSM. For many uses these dynamic updates are not really required.
If you don't plan to apply updates, the dynamic part of the database can be
safely dropped using the following command:

```
./utils/setup.php --drop
```

Note that you still need to provide for sufficient disk space for the initial
import. So this option is particularly interesting if you plan to transfer the
database or reuse the space later.

### Reverse-only Imports

If you only want to use the Nominatim database for reverse lookups or
if you plan to use the installation only for exports to a
[photon](https://photon.komoot.de/) database, then you can set up a database
without search indexes. Add `--reverse-only` to your setup command above.

This saves about 5% of disk space.

### Filtering Imported Data

Nominatim normally sets up a full search database containing administrative
boundaries, places, streets, addresses and POI data. There are also other
import styles available which only read selected data:

* **settings/import-admin.style**
  Only import administrative boundaries and places.
* **settings/import-street.style**
  Like the admin style but also adds streets.
* **settings/import-address.style**
  Import all data necessary to compute addresses down to house number level.
* **settings/import-full.style**
  Default style that also includes points of interest.
* **settings/import-extratags.style**
  Like the full style but also adds most of the OSM tags into the extratags
  column.

The style can be changed with the configuration `CONST_Import_Style`.

To give you an idea of the impact of using the different styles, the table
below gives rough estimates of the final database size after import of a
2020 planet and after using the `--drop` option. It also shows the time
needed for the import on a machine with 64GB RAM, 4 CPUS and NVME disks.
Note that the given sizes are just an estimate meant for comparison of
style requirements. Your planet import is likely to be larger as the
OSM data grows with time.

style     | Import time  |  DB size   |  after drop
----------|--------------|------------|------------
admin     |    4h        |  215 GB    |   20 GB
street    |   22h        |  440 GB    |  185 GB
address   |   36h        |  545 GB    |  260 GB
full      |   54h        |  640 GB    |  330 GB
extratags |   54h        |  650 GB    |  340 GB

You can also customize the styles further.
A [description of the style format](../develop/Import.md#configuring-the-import) 
can be found in the development section.

## Initial import of the data

!!! danger "Important"
    First try the import with a small extract, for example from
    [Geofabrik](https://download.geofabrik.de).

Download the data to import. Then issue the following command
from the **build directory** to start the import:

```sh
./utils/setup.php --osm-file <data file> --all 2>&1 | tee setup.log
```

### Notes on full planet imports

Even on a perfectly configured machine
the import of a full planet takes around 2 days. Once you see messages
with `Rank .. ETA` appear, the indexing process has started. This part takes
the most time. There are 30 ranks to process. Rank 26 and 30 are the most complex.
They take each about a third of the total import time. If you have not reached
rank 26 after two days of import, it is worth revisiting your system
configuration as it may not be optimal for the import.

### Notes on memory usage

In the first step of the import Nominatim uses [osm2pgsql](https://osm2pgsql.org)
to load the OSM data into the PostgreSQL database. This step is very demanding
in terms of RAM usage. osm2pgsql and PostgreSQL are running in parallel at 
this point. PostgreSQL blocks at least the part of RAM that has been configured
with the `shared_buffers` parameter during
[PostgreSQL tuning](Installation#postgresql-tuning)
and needs some memory on top of that. osm2pgsql needs at least 2GB of RAM for
its internal data structures, potentially more when it has to process very large
relations. In addition it needs to maintain a cache for node locations. The size
of this cache can be configured with the parameter `--osm2pgsql-cache`.

When importing with a flatnode file, it is best to disable the node cache
completely and leave the memory for the flatnode file. Nominatim will do this
by default, so you do not need to configure anything in this case.

For imports without a flatnode file, set `--osm2pgsql-cache` approximately to
the size of the OSM pbf file you are importing. The size needs to be given in
MB. Make sure you leave enough RAM for PostgreSQL and osm2pgsql as mentioned
above. If the system starts swapping or you are getting out-of-memory errors,
reduce the cache size or even consider using a flatnode file.

### Verify the import

Run this script to verify all required tables and indices got created successfully.

```sh
./utils/check_import_finished.php
```

### Setting up the website

Run the following command to set up the configuration file for the API frontend
`settings/settings-frontend.php`. These settings are used in website/*.php files.

```sh
./utils/setup.php --setup-website
```
!!! Note
    This step is not necessary if you use `--all` option while setting up the DB.

Now you can try out your installation by running:

```sh
make serve
```

This runs a small test server normally used for development. You can use it
to verify that your installation is working. Go to
`http://localhost:8088/status.php` and you should see the message `OK`.
You can also run a search query, e.g. `http://localhost:8088/search.php?q=Berlin`.

To run Nominatim via webservers like Apache or nginx, please read the
[Deployment chapter](Deployment.md).

## Tuning the database

Accurate word frequency information for search terms helps PostgreSQL's query
planner to make the right decisions. Recomputing them can improve the performance
of forward geocoding in particular under high load. To recompute word counts run:

```sh
./utils/update.php --recompute-word-counts
```

This will take a couple of hours for a full planet installation. You can
also defer that step to a later point in time when you realise that
performance becomes an issue. Just make sure that updates are stopped before
running this function.

If you want to be able to search for places by their type through
[special key phrases](https://wiki.openstreetmap.org/wiki/Nominatim/Special_Phrases)
you also need to enable these key phrases like this:

    ./utils/specialphrases.php --wiki-import > specialphrases.sql
    psql -d nominatim -f specialphrases.sql

Note that this command downloads the phrases from the wiki link above. You
need internet access for the step.


## Installing Tiger housenumber data for the US

Nominatim is able to use the official [TIGER](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html)
address set to complement the OSM house number data in the US. You can add
TIGER data to your own Nominatim instance by following these steps. The
entire US adds about 10GB to your database.

  1. Get preprocessed TIGER 2019 data and unpack it into the
     data directory in your Nominatim sources:

        cd Nominatim/data
        wget https://nominatim.org/data/tiger2019-nominatim-preprocessed.tar.gz
        tar xf tiger2019-nominatim-preprocessed.tar.gz

  2. Import the data into your Nominatim database:

        ./utils/setup.php --import-tiger-data

     If you have unpacked the data into a different directory you may change
     the source path with the `--tiger-data-path` parameter.

  3. Enable use of the Tiger data in your `settings/local.php` by adding:

         @define('CONST_Use_US_Tiger_Data', true);

  4. Apply the new settings:

```sh
    ./utils/setup.php --create-functions --enable-diff-updates --create-partition-functions
```


See the [developer's guide](../develop/data-sources.md#us-census-tiger) for more
information on how the data got preprocessed.

