#!/usr/bin/python3

import os
import sys
import datetime
import apache_log_parser
import json
import psycopg2
import psycopg2.extras
import pickle
import socket
import whois
import geoip2.database
import re
from urllib.parse import unquote


LOGS_DIR = "/var/log/httpd/"
DOMAIN = "www.usap-dc.org"
country_db = '/web/usap-dc/htdocs/static/GeoLite2-Country_20181030/GeoLite2-Country.mmdb'
coutries_pickle = '/web/usap-dc/htdocs/inc/ip_countries.pickle'
counter_robots_list = '/web/usap-dc/htdocs/static/COUNTER_Robots_list.json' # https://github.com/atmire/COUNTER-Robots/blob/master/COUNTER_Robots_list.json

recaptcha_date = datetime.date(2021, 7, 1)  # date when we started using recaptcha for downloads - preventing automated python downloads

line_parser = apache_log_parser.make_parser("%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"")

config = json.loads(open('../config.json', 'r').read())

exclude = ["bot", "craw", "spider", "159.255.167", "geoinfo-", "5.188.210", "5.188.211", 'Bot', 'Spider', 'Craw', 'WebInject', '63.142.253.235', 
              'BUbiNG', 'AddThis.com', 'ia_archiver', 'facebookexternalhit', 'ltx71', 'panscient', 'ns343855.ip-94-23-45.eu', 'ns320079.ip-37-187-150.eu', 
              'hive.ldeo.columbia.edu', 'seafloor.mgds.ldeo.columbia.edu', 'ec01-vm3.ldeo.columbia.edu', 'ns533874.ip-192-99-7.net', '31.187.70.17',
              'The Knowledge AI', 'pool-72-89-254-157.nycmny.fios.verizon.net', 'ip-172-31-']

# add official list of user agents that are regarded as robots/spiders by Project COUNTER to my exclude list
counter_robots = json.loads(open(counter_robots_list, 'r').read())
for item in counter_robots:
    exclude.append(item['pattern'])

reader = geoip2.database.Reader(country_db)


def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER_CURATOR'],
                            password=info['PASSWORD_CURATOR'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


def valueInHoneyPot(request_url):
    # hidden fields that will only be populated by bots
    search = parseSearch(request_url)
    in_honey_pot = (search.get('email') and search['email'] != '') or (search.get('name') and search['name'] != '')
    # print(in_honey_pot)
    return in_honey_pot


def parseSearch(resource):
    if '?' not in resource:
        return {}
    resource = resource.split('?')[1]
    filters = resource.split('&')
    search = {}
    for f in filters:
        try:
            filter, value = f.split('=', 1)
            value = unquote(value).replace('%20', ' ').replace('+', ' ')
            search[filter] = value 
        except:
            continue
    return search


def getCountryFromIP(ip_line):
    # print('IP: %s' % ip_line)
    if ip_to_country.get(ip_line) is None:
        try:
            # resolve hostname to IP address
            ip = socket.gethostbyname(ip_line)
            # look up country by IP address
            country = reader.country(ip).country.name
        except:
            country = None  

        if country is None:
            try:
                # try getting country using whois
                w = whois.whois(ip_line)
                if w.get('country'):
                    country = w['country']
                    if isinstance(country, list):
                        country = country[0]
                    # print('COUNTRY FOUND USING WHOIS (country) %s: %s' % (ip_line, country))
                elif w.get('nserver'):
                    ip = w['nserver']
                    print(('new ip ' + ip))
                    country = getCountryFromIP(ip)
                    #print('COUNTRY FOUND USING WHOIS (nserver) %s: %s' % (ip_line, country))
                else:
                    country = 'Unknown'
            except:
                country = 'Unknown'
        #         print('***UNKNOWN***')
        # print('Country: %s' % country)
        if country in ['U.S.', 'USA', 'U.S.A.', 'United States']: 
            country = 'US'
        ip_to_country[ip_line] = country
    return ip_to_country[ip_line]


def excludeEntry(log_line_data):
    # exclude bots and crawlers
    if ((any(re.search(substring.lower(), log_line_data['remote_host'].lower()) for substring in exclude)) 
        or (any(re.search(substring.lower(), log_line_data['request_header_user_agent'].lower()) for substring in exclude))  
        or valueInHoneyPot(request_url)): 
        return True
    return False    


if __name__ == '__main__':

    if len(sys.argv) == 3:
        year = int(sys.argv[1])
        month = int(sys.argv[2])
    else:
        # get current date and use it to figure out access log file
        now = datetime.datetime.now()
        year = now.year
        month = now.month

    log_date = datetime.date(year, month, 1)

    # import previously saved ip_to_country table 
    if os.path.isfile(coutries_pickle):
        print(("IMPORTING %s" % coutries_pickle))
        with open(coutries_pickle, 'rb') as f:
            ip_to_country = pickle.load(f)
    else:
        ip_to_country = {}

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

            # REFERERS
            referer = log_line_data['request_header_referer'].replace("'", "''")
            status = log_line_data['status']
            if 'usap-dc.org' not in referer.lower() and referer != '-' and 'not_found' not in request_url and request_url != '/search_old' \
                and status == '200' and not request_url.startswith('/?') and referer != 'https://orcid.org/' \
                and not request_url.startswith('/static') and request_url != '' and referer != '' and request_url != '//' \
                and request_url != '/jsglue.js' and 'invalid' not in request_url and 'authorized' not in request_url \
                and 'supplement' not in request_url and 'accounts.' not in referer and referer.startswith('http') \
                and '34.195.51.19' not in referer and '34.195.50.19' not in referer:

                if excludeEntry(log_line_data): continue

                # print('%s: %s - %s' % (status, request_url, referer))
                country = getCountryFromIP(log_line_data['remote_host'])
                sql = '''INSERT INTO access_logs_referers (resource_requested, referer, remote_host, time, country)
                            VALUES ('%s', '%s', '%s', '%s', '%s');''' % (request_url, referer, log_line_data['remote_host'], 
                            log_line_data['time_received_utc_isoformat'], country)
                try:
                    cur.execute(sql)
                    num += 1
                except:
                    #entry already exists, do nothing
                    pass
                cur.execute("COMMIT;")      
            
            # DOWNLOADS
            if "/dataset/usap-dc/" in request_url:
                if excludeEntry(log_line_data):                    
                    if log_date < recaptcha_date and 'python' in log_line_data['request_header_user_agent'].lower() and 'ip-172-31-' not in log_line_data['remote_host']: 
                        pass # allow python downloads to be counted before we started using recaptcha check, unless they were fro our own IP address 
                    else:
                        continue

                sql = '''INSERT INTO access_logs_downloads (remote_host, time, resource_requested, resource_size, referer, user_agent) 
                            VALUES ('%s', '%s', '%s', '%s', '%s', '%s');''' % (log_line_data['remote_host'], log_line_data['time_received_utc_isoformat'],
                            log_line_data['request_url'], log_line_data['response_bytes_clf'], log_line_data['request_header_referer'],
                            log_line_data['request_header_user_agent'])
                try:
                    cur.execute(sql)
                    num += 1
                except Exception as e:
                    #entry already exists, do nothing
                    pass
                cur.execute("COMMIT;")
            
            # SEARCHES
            elif "search?" in request_url:
                if excludeEntry(log_line_data): continue

                dataset_search = "dataset_search?" in request_url
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
            
            # LANDING PAGE VIEWS
            if "/view/" in request_url:
                if excludeEntry(log_line_data): continue

                country = getCountryFromIP(log_line_data['remote_host'])
                sql = '''INSERT INTO access_logs_views (remote_host, time, resource_requested, resource_size, referer, user_agent, country) 
                            VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s');''' % (log_line_data['remote_host'], log_line_data['time_received_utc_isoformat'],
                            log_line_data['request_url'], log_line_data['response_bytes_clf'], log_line_data['request_header_referer'],
                            log_line_data['request_header_user_agent'], country)
                try:
                    cur.execute(sql)
                    num += 1
                except:
                    #entry already exists, do nothing
                    pass
                cur.execute("COMMIT;") 

            # EXTERNAL DATASETS
            if "/tracker?" in request_url:
                if excludeEntry(log_line_data): continue
                params = parseSearch(log_line_data['request_url'])
                if not params.get('url') or config['USAP_DOMAIN'] in params['url']: continue #external datasets only 

                country = getCountryFromIP(log_line_data['remote_host'])
                sql = '''INSERT INTO access_logs_external (remote_host, time, resource_requested, resource_size, referer, user_agent, country) 
                            VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s');''' % (log_line_data['remote_host'], log_line_data['time_received_utc_isoformat'],
                            log_line_data['request_url'], log_line_data['response_bytes_clf'], log_line_data['request_header_referer'],
                            log_line_data['request_header_user_agent'], country)
                try:
                    cur.execute(sql)
                    num += 1
                except:
                    #entry already exists, do nothing
                    pass
                cur.execute("COMMIT;")             

        print(("%s entries added to the database" % num))
