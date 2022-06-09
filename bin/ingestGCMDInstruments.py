#!/usr/bin/python3

# script to ingest gcmd_instruments from csv file into gcmd_instruments table
# to run form main dir: >python bin/ingestGCMDInstruments

import psycopg2
import psycopg2.extras
import json
import csv

config = json.loads(open('config.json', 'r').read())
in_file = 'instruments.csv'


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
with open(in_file, 'r', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile, delimiter=",")
    for row in reader:
        if rownum > 1:
        # generate ID
            id = row[0].upper()
            if row[1] and row[1] != '':
                id += ' > '+row[1].upper()
            if row[2] and row[2] != '':
                id += ' > '+row[2].upper()
            if row[3] and row[3] != '':
                id += ' > '+row[3].upper()  
            if row[4] and row[4] != '':
                id += ' > '+row[4].upper()           
            # add to database
            sql = "INSERT INTO gcmd_instrument (id, category, class, type, subtype, short_name, long_name) VALUES (%s, %s, %s, %s, %s, %s, %s);"
                    
            cur.execute(sql, (id, row[0], row[1], row[2], row[3], row[4], row[5]))
        rownum += 1
cur.execute('COMMIT;')
