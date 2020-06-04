#!/opt/rh/python27/root/usr/bin/python

import os
import sys
import datetime
import apache_log_parser
import json
import psycopg2
import psycopg2.extras

LOGS_DIR = "/var/log/httpd/"
DOMAIN = "www.usap-dc.org"

line_parser = apache_log_parser.make_parser("%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"")

config = json.loads(open('../config.json', 'r').read())

exclude = ["bot", "craw", "spider", "159.255.167", "geoinfo-", "5.188.210", "5.188.211"]


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

    if len(sys.argv) == 3:
        year = int(sys.argv[1])
        month = int(sys.argv[2])
    else:
        # get current date and use it to figure out access log file
        now = datetime.datetime.now()
        year = now.year
        month = now.month

    (conn, cur) = connect_to_db()
    if type(conn) is str:
        out_text = conn
    else:
        filename = "%s-access_log.%s-%02d" % (DOMAIN, year, month)
        log_file = os.path.join(LOGS_DIR, filename)
        print(log_file)
        #log_file = "test.log"
        f = open(log_file, 'r')
        entries = f.readlines()
        num = 0
        for entry in entries:
            log_line_data = line_parser(entry)
            request_url = log_line_data['request_url']
            # DOWNLOADS
            if "/dataset/usap-dc/" in request_url:
                if (not any(substring in log_line_data['remote_host'] for substring in exclude)):
                    sql = '''INSERT INTO access_logs_downloads (remote_host, time, resource_requested, resource_size, referer, user_agent) 
                             VALUES ('%s', '%s', '%s', '%s', '%s', '%s');''' % (log_line_data['remote_host'], log_line_data['time_received_utc_isoformat'],
                                log_line_data['request_url'], log_line_data['response_bytes_clf'], log_line_data['request_header_referer'],
                                log_line_data['request_header_user_agent'])
                    try:
                        cur.execute(sql)
                        num += 1
                    except:
                        #entry already exists, do nothing
                        pass
                    cur.execute("COMMIT;")
            
            # SEARCHES
            elif "search?" in request_url:
                dataset_search = "dataset_search?" in request_url
                if (not any(substring in log_line_data['remote_host'] for substring in exclude)):
                    sql = '''INSERT INTO access_logs_searches (remote_host, time, resource_requested, resource_size, referer, user_agent, dataset_search) 
                             VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %s);''' % (log_line_data['remote_host'], log_line_data['time_received_utc_isoformat'],
                                log_line_data['request_url'], log_line_data['response_bytes_clf'], log_line_data['request_header_referer'],
                                log_line_data['request_header_user_agent'], dataset_search)
                    try:
                        cur.execute(sql)
                        num += 1
                    except:
                        #entry already exists, do nothing
                        pass
                    cur.execute("COMMIT;")
        print("%s entries added to the database" % num)
