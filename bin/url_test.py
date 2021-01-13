#!/opt/rh/python27/root/usr/bin/python

"""
Program to test landing pages and edit pages 
makes sure they all return a status_code of 200 and do not redirect to the not_found page.

NSS 12/11/20: update to test all URLS in the database and to be callable by weeklyReport

To run:
Need to comment out login and authentication code in usap.py first. (only if testing edit pages)
For datasets ~500-506
For projects ~1215-1222
> python url_test.py
"""

import json
import psycopg2
import psycopg2.extras
import requests
import urllib3

# disable warnings about sites with SSL certificate issues
urllib3.disable_warnings()

config = json.loads(open('/web/usap-dc/htdocs/config.json', 'r').read())
ROOT = config['USAP_DOMAIN']
NOTFOUND = ROOT + "not_found"
LOGIN = ROOT + "login"


def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER_CURATOR'],
                            password=info['PASSWORD_CURATOR'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


def testUrl(url, verbose):
    r = requests.get(url)
    if r.url == LOGIN:
        if verbose: print('%s: TEST ERROR - Login page. Please bypass authentication.' % url)
        return('%s: TEST ERROR - Login page. Please bypass authentication.' % url)
        return('<li><a href="%s">%s</a> TEST ERROR - Login page. Please bypass authentication.</li>' % (url, url))
    if r.url == NOTFOUND or r.url != url:
        if verbose: print('%s: ERROR - page not found' % url)
        return('<li><a href="%s">%s</a> ERROR - landing page not found</li>' % (url, url))
    if r.status_code == 200:
        if verbose: print('%s: OK' % url)
        return("")
    else:
        if verbose: print('%s: ERROR - status_code %s' % (url, r.status_code))
        return('<li><a href="%s">%s</a>: ERROR - problem loading landing page. Status code %s</li>' % (url, url, r.status_code))


# check urls using requests module.  Disable ssl verification as some sites have SSL issues
def testUrlFromTable(url, bad_url_col, bad_url_count, uid_col, uid, table, cur, verbose):
    if not url or url[:3]=='ftp':
        return("")
    try:
        r = requests.get(url, verify=False, timeout=30)
        if r.status_code != 404:
            if verbose: print('%s: OK' % url)

            # if a previously broken url starts working again, update status in table
            if bad_url_col and bad_url_count >=1:
                query = "UPDATE %s SET status='exist', %s=0 WHERE %s='%s';" % (table, bad_url_col, uid_col, uid)
                cur.execute(query)

            return("")
        else:
            if verbose: print('%s: ERROR in table %s - status_code: %s' % (url, table, r.status_code))
            # if keeping count of how many times a URL is reported as broken, then update the database now
            if bad_url_col:
                return(incrementBadUrlCount(cur, table, url, bad_url_col, bad_url_count, uid_col, uid))
            else:
                return('<li><a href="%s">%s</a>: ERROR in table %s - broken link: %s</li>' % (url, url, table))
    except Exception as e:
        if verbose: print('%s: ERROR in table %s- %s') % (url, table, str(e))
        # if keeping count of how many times a URL is reported as broken, then update the database now
        if bad_url_col:
            return(incrementBadUrlCount(cur, table, url, bad_url_col, bad_url_count, uid_col, uid))
        else:
            return('<li><a href="%s">%s</a>: ERROR in table %s - : broken link</li>' % (url, url, table))


def incrementBadUrlCount(cur, table, url, bad_url_col, bad_url_count, uid_col, uid):
    if bad_url_count >= 3:
        return("")
    if not bad_url_count:
        bad_url_count = 1
    else:
        bad_url_count += 1
    # after 3 failed tests, set status to bad_url and don't report in output
    if bad_url_count == 3:
        query = "UPDATE %s SET status='bad_url', %s=%s WHERE %s='%s';" % (table, bad_url_col, bad_url_count, uid_col, uid)
        cur.execute(query)
        return("")
    else:
        query = "UPDATE %s SET %s=%s WHERE %s='%s';" % (table, bad_url_col, bad_url_count, uid_col, uid)
        cur.execute(query)
        return('<li><a href="%s">%s</a>: ERROR in table %s - broken link (warning No.%s)</li>' % (url, url, table, bad_url_count))



def getUrlsFromTable(table, url_col, bad_url_col, uid_col, cur, verbose):
    msg = ""

    query = 'SELECT * from %s;' % (table)
    cur.execute(query)
    res = cur.fetchall()
    for line in res:
        msg += testUrlFromTable(line[url_col], bad_url_col, line.get(bad_url_col), uid_col, line.get(uid_col), table, cur, verbose)
    
    # commit any updates to bad_url_counts, or statuses
    cur.execute('COMMIT;')

    return(msg)


def testAllUrls(verbose=False):
    conn, cur = connect_to_db()
    output = ""

    # first test landing pages
    query = "SELECT id from dataset;"
    cur.execute(query)
    res = cur.fetchall()
    for line in res:
        uid = line.get('id')
        url = ROOT + 'view/dataset/%s' % uid
        output += testUrl(url, verbose)
        # url = ROOT + 'edit/dataset/%s' % uid
        # testUrl(url, verbose)

    query = "SELECT proj_uid from project;"
    cur.execute(query)
    res = cur.fetchall()
    for line in res:
        uid = line.get('proj_uid')
        url = ROOT + 'view/project/%s' % uid
        output += testUrl(url, verbose)
        # url = ROOT + 'edit/project/%s' % uid
        # testUrl(url, verbose)

    #now test URLs in database
    urls_to_test = [
        {'table': 'dataset', 'url_col': 'url'},
        {'table': 'license', 'url_col': 'url'},
        {'table': 'project_dataset', 'url_col': 'url', 'bad_url_col': 'bad_url_count', 'uid_col':'dataset_id'},
        {'table': 'project_deployment', 'url_col': 'url'},
        {'table': 'project_website', 'url_col': 'url'},
        {'table': 'repository', 'url_col': 'repository_URL'},
    ]

    for entry in urls_to_test:
        if verbose: print('\n******** TABLE: %s\n' %entry['table'])
        output += getUrlsFromTable(entry['table'], entry['url_col'], entry.get('bad_url_col'), entry.get('uid_col'), cur, verbose)

    # print(output)
    return(output)


if __name__ == '__main__':
    testAllUrls(True)
