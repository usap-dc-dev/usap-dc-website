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


if __name__ == '__main__':
    conn, cur = connect_to_db()
    query = """REFRESH MATERIALIZED VIEW project_view; 
        REFRESH MATERIALIZED VIEW dataset_view;
        REFRESH MATERIALIZED VIEW access_views_ip_date_matview;
        REFRESH MATERIALIZED VIEW access_views_projects_matview;
        COMMIT;"""
    cur.execute(query)
    print("DONE")
