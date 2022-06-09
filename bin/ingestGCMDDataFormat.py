#!/usr/bin/python3

# script to ingest gcmd data format from csv file into gcmd_data_format table
# to run form main dir: >python bin/ingestGCMDDataFormat

import psycopg2
import psycopg2.extras
import json
import csv

config = json.loads(open('config.json', 'r').read())
in_file = 'DataFormat.csv'


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
        
            # add to database
            sql = "INSERT INTO gcmd_data_format (short_name, long_name) VALUES (%s, %s);"
                    
            cur.execute(sql, (row[0], row[1]))
        rownum += 1
cur.execute('COMMIT;')
