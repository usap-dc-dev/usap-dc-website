import psycopg2
import psycopg2.extras
import json
import csv

config = json.loads(open('config.json', 'r').read())


def connect_to_prod_db():
    info = config['PROD_DATABASE']
    user = info['USER']
    password = info['PASSWORD']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=user,
                            password=password)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


if __name__ == '__main__':
    difs = []
    with open('inc/edsc_collection_results_export.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            difs.append(row['Short Name'])

    (conn, cur) = connect_to_prod_db()
    for dif in difs:
        query = "SELECT COUNT(*) FROM project_dif_map WHERE dif_id ~* '%s';" % dif.strip()
        cur.execute(query)
        res = cur.fetchone()
        if res['count'] >1:
            print('%s' % (dif))
