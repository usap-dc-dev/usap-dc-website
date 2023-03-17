import psycopg2
import psycopg2.extras
import json

config = json.loads(open('/web/usap-dc/htdocs/config.json', 'r').read())


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

def get_keyword_dict():
    query = "select keyword_ieda.keyword_id as ieda_id, keyword_usap.keyword_id as usap_id from keyword_ieda inner join keyword_usap on keyword_ieda.keyword_label = keyword_usap.keyword_label where keyword_usap.keyword_id like 'u%';"
    keyword_map = {}
    cur.execute(query)
    keywords = cur.fetchall()
    for kw in keywords:
        keyword_map[kw['ieda_id']] = kw['usap_id']
    return keyword_map

def update_old_keywords(keyword_dict):
    for kw in keyword_dict:
        dataset_query = "UPDATE dataset_keyword_map SET keyword_id = %s where keyword_id = %s; COMMIT;"
        project_query = "UPDATE project_keyword_map SET keyword_id = %s where keyword_id = %s; COMMIT;"
        cur.execute(dataset_query, (keyword_dict[kw], kw))
        cur.execute(project_query, (keyword_dict[kw], kw))

keyword_mapping = get_keyword_dict()
update_old_keywords(keyword_mapping)