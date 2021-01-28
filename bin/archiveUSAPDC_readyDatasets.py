#!/opt/rh/python27/root/usr/bin/python
"""
 Run achiveUSAPDC for all datasets marked 'Ready To Archive'
 e.g.
 python archiveUSAPDC_readyDatasets.py
"""

import os
import json
import psycopg2
import psycopg2.extras

config = json.loads(open('../config.json', 'r').read())

ROOT_DIR = "/web/usap-dc/htdocs"
FILE_PATH = "dataset"
OUT_DIR = "archive"


def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER'],
                            password=info['PASSWORD'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


conn, cur = connect_to_db()
query = "SELECT dataset_id FROM dataset_archive WHERE status = 'Ready To Be Archived';"
cur.execute(query)
res = cur.fetchall()

for row in res:
    ds_id = row['dataset_id']
    print("Archiving %s" % ds_id)
    os.system("./archiveUSAPDC.py -r %s -p %s -o %s -i %s -e True" % (ROOT_DIR, FILE_PATH, OUT_DIR, ds_id))

print("DONE")
