#!/opt/rh/python27/root/usr/bin/python

"""
Script to go through project_deployment table and check that R2R URLs are correct.
Will update the database and send an email with changes at the end.
Can be run as a cron.

Run from bin directory with >python deploymentR2R.py
"""

import psycopg2
import psycopg2.extras
import json
import requests
from gmail_functions import send_gmail_message


config = json.loads(open('/web/usap-dc/htdocs/config.json', 'r').read())
config.update(json.loads(open('/web/usap-dc/htdocs/inc/report_config.json', 'r').read()))

R2R_API = 'https://service.rvdata.us/api/cruise/cruise_id/'
R2R_LANDING_PAGE = 'https://www.rvdata.us/search/cruise/'

def connect_to_db():
    # info = config['PROD_DATABASE'] # when running on dev server, so we can access prouction DB
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER_CURATOR'],
                            password=info['PASSWORD_CURATOR'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


def sendEmail(message_text, subject):
    sender = config['USAP-DC_GMAIL_ACCT']
    recipients = config['RECIPIENTS']
    success, error = send_gmail_message(sender, recipients, subject, message_text, None)
    if error:
        print(error)
        sys.exit()


if __name__ == '__main__':
    (conn, cur) = connect_to_db()  
    msg = ''

    # search for all LMG and NBP deployments
    sql_cmd = """SELECT * FROM project_deployment 
                    WHERE deployment_type = 'ship expedition'
                    AND deployment_id ~* 'LMG|NBP'
                    ORDER BY proj_uid
                    """
    cur.execute(sql_cmd)

    data = cur.fetchall()

    for d in data:
        cruise = None
        # get cruise names from deployment_id
        dep_id = d['deployment_id'].replace('-','')
        ind = dep_id.upper().find('LMG')
        if ind == -1:
            ind = dep_id.upper().find('NBP')
        if ind > -1:
            cruise = dep_id.upper()[ind:8].strip()
        
        # check cruise is in R2R using R2R API
        if cruise:
            api_url = R2R_API + cruise
            r = requests.get(api_url)
            if r.json()['data']:
                landing_page = R2R_LANDING_PAGE + cruise
            else:
                landing_page = ''
                print('No cruise data found for %s') % cruise
            
            if d['url'] != landing_page:
                sql_cmd = """UPDATE project_deployment SET url = %s WHERE proj_uid = %s AND deployment_id = %s;""" 
                cur.execute(sql_cmd, (landing_page, d['proj_uid'], d['deployment_id']))

                # print('%s - %s:\nold:%s\nnew:%s\n\n') % (d['proj_uid'], d['deployment_id'], d['url'], landing_page)
                msg += """<b>%s - %s</b>
                               <br><b>Old URL:</b> %s<br>
                               <b>New URL:</b> %s<br><br>""" \
                               % (d['proj_uid'], d['deployment_id'], d['url'], landing_page)
    cur.execute('commit;')
    if msg != '':
        sendEmail(msg, "Updated Deployment R2R URLs")