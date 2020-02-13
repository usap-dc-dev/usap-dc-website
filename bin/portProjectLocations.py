#!/opt/rh/python27/root/usr/bin/python

# script to port gcmd locations to project_keyword_map
# and to add keyword_id to the gcmd_locations table
# to run form main dir: >python bin/portProjectLocations 

import psycopg2
import psycopg2.extras
import json

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
query = "SELECT * FROM project_gcmd_location_map ORDER BY proj_uid"
cur.execute(query)
res = cur.fetchall()
for p in res:
    uid = p['proj_uid']
    location = p['loc_id'].split('>')[-1].replace(' ', '', 1).lower()
    query = "SELECT * from keyword_usap WHERE keyword_type_id='kt-0006' AND keyword_label ILIKE '%s'" % location
    cur.execute(query)
    kw = cur.fetchall()
    if len(kw) < 1:
        print('Keyword location not found for %s: %s.') % (uid, location)
    else:
        try:
            print('%s: %s -> %s' % (uid, location, kw[0]['keyword_label']))
            cmd = "INSERT INTO project_keyword_map (proj_uid, keyword_id) VALUES ('%s', '%s');" % (uid, kw[0]['keyword_id'])
            cur.execute(cmd)
        except Exception as e:
            print(str(e))
cur.execute('COMMIT;')

# add keyword_id to gcmd_locations
query = "SELECT * FROM gcmd_location"
cur.execute(query)
res = cur.fetchall()
for l in res:
    location = l['id'].split('>')[-1].replace(' ', '', 1).lower()
    query = "SELECT * from keyword_usap WHERE keyword_type_id='kt-0006' AND keyword_label ILIKE '%s'" % location
    cur.execute(query)
    kw = cur.fetchall()
    if len(kw) < 1:
        print('Keyword location not found for %s.') % (l['id'])
    else:
        cmd = "UPDATE gcmd_location SET keyword_id = '%s' WHERE id = '%s';" % (kw[0]['keyword_id'], l['id'])
        cur.execute(cmd)
cur.execute('COMMIT;')
