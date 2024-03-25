import json
import psycopg2
import psycopg2.extras

config = json.loads(open('config.json', 'r').read())
info = config['DATABASE']
user = info['USER']
password = info['PASSWORD']
conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=user,
                            password=password)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

query = "select * from dataset_fairness_score order by dataset_id;"
cur.execute(query)
rows = cur.fetchall()
records = list(map(lambda x : (x['dataset_id'], x['score'], x['max_score'], -1 if int(x['max_score']) == 0 else round(2 * float(x['score'])/float(x['max_score']), 2)), rows))
print('dataset_id, score_count, max_count, score')
for record in records:
    print('%s, %s, %s, %s' % (record[0], record[1], record[2], record[3]))