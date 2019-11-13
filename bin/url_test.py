#!/opt/rh/python27/root/usr/bin/python

"""
Program to test landing pages and edit pages 
makes sure they all return a status_code of 200 and do not redirect to the not_found page.

To run:
Need to comment out login and authentication code in usap.py first.
For datasets ~500-506
For projects ~1215-1222
> python url_test.py
"""

import json
import psycopg2
import psycopg2.extras
import requests


config = json.loads(open('/web/usap-dc/htdocs/config.json', 'r').read())
ROOT = "http://www-dev.usap-dc.org"
NOTFOUND = "http://www-dev.usap-dc.org/not_found"
LOGIN = "http://www-dev.usap-dc.org/login"


def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER'],
                            password=info['PASSWORD'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


def testUrl(url):
    r = requests.get(url)
    if r.url == LOGIN:
        print('%s: TEST ERROR - Login page. Please bypass authentication.' % url)
        exit() 
    if r.url == NOTFOUND or r.url != url:
        print('%s: ERROR - page not found' % url)
        exit()
    if r.status_code == 200:
        print('%s: OK' % url)
        pass
    else:
        print('%s: ERROR - status_code %s' % (url, r.status_code))
        exit()


if __name__ == '__main__':
    conn, cur = connect_to_db()

    query = "SELECT id from dataset;"
    cur.execute(query)
    res = cur.fetchall()
    for line in res:
        uid = line.get('id')
        url = ROOT + '/view/dataset/%s' % uid
        testUrl(url)
        url = ROOT + '/edit/dataset/%s' % uid
        testUrl(url)

    query = "SELECT proj_uid from project;"
    cur.execute(query)
    res = cur.fetchall()
    for line in res:
        uid = line.get('proj_uid')
        url = ROOT + '/view/project/%s' % uid
        testUrl(url)
        url = ROOT + '/edit/project/%s' % uid
        testUrl(url)
