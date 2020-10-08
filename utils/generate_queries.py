#!/usr/bin/python3
# SPDX-License-Identifier: GPL-2.0-only
#
# This file is part of Nominatim.
# Copyright (C) 2020 Sarah Hoffmann

from argparse import ArgumentParser, RawDescriptionHelpFormatter, ArgumentTypeError
import psycopg2
import getpass
import sys
import re
import random

def print_cmd(**kwargs):
    cmd = './utils/query.php'
    for k, v in kwargs.items():
        cmd += f" --{k} '{v}'"
    print(cmd)


class QueryGenerator(object):

    def __init__(self, options):
        password = None
        if options.password_prompt:
            password = getpass.getpass("Database password: ")

        self.options = options
        self.run = getattr(self, options.querytype)
        self.conn = psycopg2.connect(dbname=options.dbname,
                                     user=options.user,
                                     password=password,
                                     host=options.host,
                                     port=options.port)

        random.seed()


    def classtype(self):
        c = self.conn.cursor()
        # get all possible classtype terms
        c.execute("""SELECT distinct word FROM word
                     WHERE class is not null AND operator is not null
                       AND word is not null
                       AND class not in ('place', 'building')""")
        words = [r[0].replace("'", "") for r in c]

        # get names for all place nodes
        c.execute("""SELECT distinct name->'name' FROM placex
                     WHERE osm_type = 'N' AND class = 'place'
                       AND name ? 'name' and rank_address between 16 and 25""");
        places = [r[0].replace("'", "") for r in c]

        c.close()

        # Generate queries as a random combination of word and place.
        for i in range(self.options.num):
            print_cmd(search=f'{random.choice(words)} {random.choice(places)}');


if __name__ == '__main__':
    def h(s):
        return re.sub("\s\s+" , " ", s)

    p = ArgumentParser(description="Query generator for testing.",
                       formatter_class=RawDescriptionHelpFormatter)

    p.add_argument('querytype', choices=['classtype'],
                   help='Type of queries to generate.')
    p.add_argument('-n', '--number', dest='num', type=int, default=100,
                   help='number of queries to generate')
    p.add_argument('-d', '--database',
                   dest='dbname', action='store', default='nominatim',
                   help='Name of the PostgreSQL database to connect to.')
    p.add_argument('-U', '--username',
                   dest='user', action='store',
                   help='PostgreSQL user name.')
    p.add_argument('-W', '--password',
                   dest='password_prompt', action='store_true',
                   help='Force password prompt.')
    p.add_argument('-H', '--host',
                   dest='host', action='store',
                   help='PostgreSQL server hostname or socket location.')
    p.add_argument('-P', '--port',
                   dest='port', action='store',
                   help='PostgreSQL server port')

    QueryGenerator(p.parse_args(sys.argv[1:])).run()
