#!/opt/rh/python27/root/usr/bin/python
"""
 Run achiveUSAPDC for all datasets between a start id and an end id
 e.g.
 python /bin/archiveUSAPDC_batch.py 601021 601105
"""

import sys
import os
import json
import psycopg2
import psycopg2.extras

start_range = sys.argv[1]
end_range = sys.argv[2]

config = json.loads(open('config.json', 'r').read())

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
query = "SELECT id FROM dataset WHERE id::integer BETWEEN %s AND %s ORDER BY id::integer;" % (start_range, end_range)
cur.execute(query)
res = cur.fetchall()

for row in res:
    ds_id = row['id']
    print("Archiving %s" % ds_id)
    os.system("bin/archiveUSAPDC.py -r %s -p %s -o %s -i %s" % (ROOT_DIR, FILE_PATH, OUT_DIR, ds_id))

print("DONE")
