#!/usr/bin/python3

# script to ingest gcmd_paleo_time from csv file into gcmd_paleo_time table
# to run form main dir: >python bin/ingestGCMDTime

import psycopg2
import psycopg2.extras
import json
import csv

config = json.loads(open('config.json', 'r').read())
in_file = 'chronounits_with_dates.csv'


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
            for col in range(1,6):
                if row[col] and row[col] != '':
                    id += ' > '+row[col].upper()
        
            # add to database
            sql = "INSERT INTO gcmd_paleo_time (id, eon, era, period, epoch, age, sub_age, start_date_Ma, end_date_Ma) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
                    
            cur.execute(sql, (id, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]))
        rownum += 1
cur.execute('COMMIT;')
