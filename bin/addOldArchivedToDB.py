#!/root/usr/bin/python3
from time import strftime
import json
import psycopg2
import psycopg2.extras

bagitDate = bagitDate = strftime("2017-04-22 00:00:00")
config = json.loads(open('config.json', 'r').read())


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
with open('inlist') as f:
    for line in f:
        checksum, bagit_name = line.split('  ')
        ds_id = bagit_name.split('_')[0]

        query = """INSERT INTO dataset_archive (dataset_id, archived_date, bagit_file_name, sha256_checksum) VALUES ('%s', '%s', '%s', '%s');""" % \
                (ds_id, bagitDate, bagit_name, checksum)
        print(query)
        cur.execute(query)

cur.execute("COMMIT;")
print("DONE")
