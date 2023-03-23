#!/usr/bin/python3

# script to ingest gcmd_paleo_time from csv file into gcmd_paleo_time table
# to run form main dir: >python bin/ingestGCMDTime

import util
import csv

in_file = 'chronounits_with_dates.csv'
csv_url = "https://gcmd.earthdata.nasa.gov/kms/concepts/concept_scheme/chronounits/?format=csv&page_num=1&page_size=2000"

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
                    for col in range(1,6):
                        if row[col] and row[col] != '':
                            id += ' > '+row[col].upper()
                    check_sql = "select count(id) as c from gcmd_paleo_time where id = '%s'"
                    cur.execute(check_sql % (id,))
                    count = cur.fetchone()['c']
                    if count == 0:
                        # add to database
                        rows_added += 1
                        sql = "INSERT INTO gcmd_paleo_time (id, eon, era, period, epoch, age, sub_age) VALUES (%s, %s, %s, %s, %s, %s, %s);"
                        cur.execute(sql, (id, row[0], row[1], row[2], row[3], row[4], row[5]))
                        ids.append("\'%s\'" % id)
                rownum += 1
        cur.execute('COMMIT;')
        if rows_added == 1:
            return "Added 1 entry to gcmd_paleo_time. Please update the \"start_date_Ma\" and \"end_date_Ma\" columns manually for the new entry, which has ID %s." % ids[0]
        else:
            note = "" if rows_added == 0 else " Please update the \"start_date_Ma\" and \"end_date_Ma\" columns manually for the new entries, which have the following IDs:<br>%s" % "<br>".join(ids)
            return "Added %d entries to gcmd_paleo_time.%s" % (rows_added, note)
    else:
        return "Error downloading %s from %s" % (in_file, csv_url)

#print(update_db())