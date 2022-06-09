#!/usr/bin/python3

# script to ingest gcmd_platforms from csv file into gcmd_platforms table
# to run form main dir: >python bin/ingestGCMDPlatforms

import psycopg2
import psycopg2.extras
import json
import csv

config = json.loads(open('config.json', 'r').read())
platforms_file = 'platforms.csv'


def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER_CURATOR'],
                            password=info['PASSWORD_CURATOR'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


conn, cur = connect_to_db()


# read in csv file
rownum = 0
with open(platforms_file, 'r', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile, delimiter=",")
    for row in reader:
        if rownum > 1:
        # generate ID
            platform_id = row[0].upper()
            if row[1] and row[1] != '':
                platform_id += ' > '+row[1].upper()
            if row[2] and row[2] != '':
                platform_id += ' > '+row[2].upper()
            if row[3] and row[3] != '':
                platform_id += ' > '+row[3].upper()            
            # add to database
            sql = "INSERT INTO gcmd_platform (id, basis, category, sub_category, short_name, long_name) VALUES (%s, %s, %s, %s, %s, %s);"
                    
            cur.execute(sql, (platform_id, row[0], row[1], row[2], row[3], row[4]))
        rownum += 1
cur.execute('COMMIT;')
