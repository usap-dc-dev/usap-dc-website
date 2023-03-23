import psycopg2
import psycopg2.extras
import json
import requests

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

def download_to_file(url, filename):
    response = requests.get(url)
    if response.status_code == 200:
        data = response.text
        out_file = open(filename, "w")
        out_file.write(data)
        out_file.close()
        return True
    return False