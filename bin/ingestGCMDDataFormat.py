#!/usr/bin/python3

# script to ingest gcmd data format from csv file into gcmd_data_format table
# to run form main dir: >python bin/ingestGCMDDataFormat

import util
import csv


# config = json.loads(open('../config.json', 'r').read())
in_file = 'DataFormat.csv'

# download CSV file
csv_url = "https://gcmd.earthdata.nasa.gov/kms/concepts/concept_scheme/DataFormat/?format=csv&page_num=1&page_size=2000"


conn, cur = util.connect_to_db()

def update_db():
    if util.download_to_file(csv_url, in_file):
        rownum = 0
        rows_added = 0
        ids = []
        with open(in_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=",")
            for row in reader:
                if rownum > 1:
                    # check if it's in the database first
                    # short_name is the primary key for gcmd_data_format
                    check_sql = "select count(short_name) as c from gcmd_data_format where short_name = %s"
                    cur.execute(check_sql, (row[0],))
                    count = int(cur.fetchone()['c'])
                    if count == 0:
                        rows_added += 1
                        print("Adding (short_name, long_name) ('%s', '%s') to gcmd_data_format" % (row[0], row[1]))
                        # if not, add to database
                        sql = "INSERT INTO gcmd_data_format (short_name, long_name) VALUES (%s, %s);"
                        cur.execute(sql, (row[0], row[1]))
                        ids.append("\'%s\'" % row[0])
                rownum += 1
        cur.execute('COMMIT;')
        if rows_added == 1:
            return "Added 1 new entry to gcmd_data_format. Its short_name is %s." % ids[0]
        else:
            return "Added %d new entries to gcmd_data_format, with the following short_names:<br>" % (rows_added, "<br>".join(ids))
    else:
        return "Error downloading file from \"%s\" to \"%s\"" % (csv_url, in_file)

#print(update_db())
