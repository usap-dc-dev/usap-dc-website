#!/usr/bin/python3

# script to ingest gcmd_platforms from csv file into gcmd_platforms table
# to run form main dir: >python bin/ingestGCMDPlatforms

import util
import csv

platforms_file = 'platforms.csv'
csv_url = "https://gcmd.earthdata.nasa.gov/kms/concepts/concept_scheme/platforms/?format=csv&page_num=1&page_size=2000"


conn, cur = util.connect_to_db()

def update_db():
    if util.download_to_file(csv_url, platforms_file):
        # read in csv file
        rownum = 0
        rows_added = 0
        ids = []
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
                    check_sql = "select count(id) as c from gcmd_platform where id = %s"
                    cur.execute(check_sql, (platform_id,))
                    count = cur.fetchone()['c']
                    if count == 0:
                        # add to database
                        sql = "INSERT INTO gcmd_platform (id, basis, category, sub_category, short_name, long_name) VALUES (%s, %s, %s, %s, %s, %s);"
                        cur.execute(sql, (platform_id, row[0], row[1], row[2], row[3], row[4]))
                        ids.append("\'%s\'" % platform_id)
                        rows_added += 1
                rownum += 1
        cur.execute('COMMIT;')
        if rows_added == 1:
            return "Added 1 entry to gcmd_platform. Please update the \"type\" column manually for the new entry, which has ID %s." % ids[0]
        else:
            note = "" if rows_added == 0 else " Please update the \"type\" column manually for the new entries, with the following IDs:" % "<br>".join(ids)
            return "Added %d entries to gcmd_platform.%s" % (rows_added, note)
    else:
        return "Error downloading %s from %s" % (platforms_file, csv_url)

#print(update_db())