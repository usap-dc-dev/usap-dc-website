#!/usr/bin/python3

# script to ingest gcmd_instruments from csv file into gcmd_instruments table
# to run form main dir: >python bin/ingestGCMDInstruments

import util
import csv

in_file = 'instruments.csv'
csv_url = "https://gcmd.earthdata.nasa.gov/kms/concepts/concept_scheme/instruments/?format=csv&page_num=1&page_size=2000"

conn, cur = util.connect_to_db()

def update_db():
    if util.download_to_file(csv_url, in_file):
        # read in csv file
        rownum = 0
        rows_added = 0
        ids = []
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
                    check_sql = "select count(id) as c from gcmd_instrument where id = %s"
                    cur.execute(check_sql, (id,))
                    count = int(cur.fetchone()['c'])
                    if count == 0:
                        rows_added += 1
                        # add to database
                        sql = "INSERT INTO gcmd_instrument (id, category, class, type, subtype, short_name, long_name) VALUES (%s, %s, %s, %s, %s, %s, %s);"        
                        cur.execute(sql, (id, row[0], row[1], row[2], row[3], row[4], row[5]))
                        ids.append("\'%s\'" % id)
                rownum += 1
        cur.execute('COMMIT;')
        if rows_added == 1:
            return "Added 1 entry to gcmd_instrument. Its ID is %s." % ids[0]
        else:
            note = "." if rows_added == 0 else ", with the following IDs:<br>%s" % "<br>".join(ids)
            return "Added %d entries to gcmd_instrument%s" % (rows_added, note)
    else:
        return "Error downloading %s from %s", in_file, csv_url

#print(update_db())