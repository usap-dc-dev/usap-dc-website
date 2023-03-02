#!/usr/bin/python3
# Run as a cron task to generate weekly report
# run from main usap-dc directory with python bin/weeklyReport

import datetime
import psycopg2
import psycopg2.extras
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from gmail_functions import send_gmail_message
import crossref
import url_test

config = json.loads(open('/web/usap-dc/htdocs/config.json', 'r').read())
config.update(json.loads(open('/web/usap-dc/htdocs/inc/report_config.json', 'r').read()))


def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER'],
                            password=info['PASSWORD'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


def querySubmissionTable(cur, submission_type, status):
    query = "SELECT * FROM submission WHERE submission_type = '%s' AND status = '%s';" % (submission_type, status)
    cur.execute(query)
    res = cur.fetchall()
    msg = ""
    for d in res:
        url = config['CURATOR_PAGE'] % d['uid']
        msg += """<li><a href="%s">%s</a> %s</li>""" % (url, d['uid'], d['submitted_date'].strftime('%Y-%m-%d'))
    msg += """</ul>"""
    return msg


# this is the old sendmail function, which led to emails in the spma folder
def sendEmail_old(message, subject):
    sender = config['USAP-DC_GMAIL_ACCT']
    recipients = config['RECIPIENTS']

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)

    content = MIMEText(message, 'html', 'utf-8')
    msg.attach(content)
    
    success, error = send_gmail_message(sender, recipients, msg['Subject'], msg.as_string(), None, None)   

# send email using gmail link

def sendEmail(message_text, subject, file=None):
    print(subject)
    sender = config['USAP-DC_GMAIL_ACCT']
    recipients = config['RECIPIENTS']
    success, error = send_gmail_message(sender, recipients, subject, message_text, file)
    if error:
        print(error)
        sys.exit()

if __name__ == '__main__':
    # get current date
    today = datetime.date.today()
    week_ago = today - datetime.timedelta(days=7)
    (conn, cur) = connect_to_db()

    title = "USAP-DC WEEKLY REPORT: %s TO %s" % (week_ago, today)

    msg = """<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
             <title>%s</title>""" % title

    msg = """<html><head></head><body><h1>%s</h1>""" % title

    query = "SELECT * FROM dataset WHERE date_created > '%s';" % week_ago
    cur.execute(query)
    res = cur.fetchall()
    msg += """<h3>New Datasets Added to USAP-DC in Last Week:</h3><ul>"""
    for d in res:
        url = config['DATASET_LANDING_PAGE'] % d['id']
        msg += """<li><a href="%s">%s</a> %s %s</li>""" % (url, d['id'], d['date_created'], d['title'])
    msg += """</ul>"""

    # Get projects INGESTED in last week
    query = "SELECT * FROM project WHERE date_created > '%s';" % week_ago
    cur.execute(query)
    res = cur.fetchall()
    msg += """<h3>New Projects Registered at USAP-DC in Last Week:</h3><ul>"""
    for p in res:
        url = config['PROJECT_LANDING_PAGE'] % p['proj_uid']
        msg += """<li><a href="%s">%s</a> %s %s</li>""" % (url, p['proj_uid'], p['date_created'], p['title'])
    msg += """</ul>"""

    # Get datasets MODIFIED in last week
    (conn, cur) = connect_to_db()
    query = "SELECT * FROM dataset WHERE date_modified > '%s' AND date_modified != date_created;" % week_ago
    cur.execute(query)
    res = cur.fetchall()
    msg += """<h3>Datasets Modified in Last Week:</h3><ul>"""
    for d in res:
        url = config['DATASET_LANDING_PAGE'] % d['id']
        msg += """<li><a href="%s">%s</a> %s %s</li>""" % (url, d['id'], d['date_modified'], d['title'])
    msg += """</ul>"""

    # Get projects MODIFIED in last week
    query = "SELECT * FROM project WHERE date_modified > '%s' AND date_modified != date_created;" % week_ago
    cur.execute(query)
    res = cur.fetchall()
    msg += """<h3>Projects Modified in Last Week:</h3><ul>"""
    for p in res:
        url = config['PROJECT_LANDING_PAGE'] % p['proj_uid']
        msg += """<li><a href="%s">%s</a> %s %s</li>""" % (url, p['proj_uid'], p['date_modified'], p['title'])
    msg += """</ul>"""

    msg += """<h3>Datasets Not Yet Ingested:</h3><ul>"""
    msg += querySubmissionTable(cur, 'dataset submission', 'Pending')

    msg += """<h3>Datasets Not Yet Registered With Datacite:</h3><ul>"""
    msg += querySubmissionTable(cur, 'dataset submission', 'Not yet registered with DataCite')

    msg += """<h3>Datasets Missing ISO XML:</h3><ul>"""
    msg += querySubmissionTable(cur, 'dataset submission', 'ISO XML file missing')

    msg += """<h3>Projects Not Yet Ingested:</h3><ul>"""
    msg += querySubmissionTable(cur, 'project submission', 'Pending')

    msg += """<h3>Project Missing DIF XML:</h3><ul>"""
    msg += querySubmissionTable(cur, 'project submission', 'DIF XML file missing')

    msg += """<h3>Pending Dataset Edits:</h3><ul>"""
    msg += querySubmissionTable(cur, 'dataset edit', 'Pending')

    msg += """<h3>Pending Project Edits:</h3><ul>"""
    msg += querySubmissionTable(cur, 'project edit', 'Pending')

    msg += """<h3>New publications found using Crossref:</h3><ul>"""
    msg += crossref.get_crossref_pubs()
    msg += """</ul>"""

    msg += """<h3>Broken URLs:</h3><ul>"""
    msg += url_test.testAllUrls(False)
    msg += """</ul>"""


    # Check if data set has a valid dif
    query = """SELECT id, title, dif_id FROM dataset d 
               LEFT JOIN dataset_dif_map dfm ON dfm.dataset_id = d.id 
               LEFT JOIN dataset_weekly_report dwr ON dwr.dataset_id = d.id
               WHERE dif_id IS NULL 
               AND no_dif IS NOT TRUE
               ORDER BY id"""
    cur.execute(query)
    res = cur.fetchall()
    if res:
        msg += """<h3>Datasets Without a Valid DIF:</h3><ul>"""
        for r in res:
            url = config['DATASET_LANDING_PAGE'] % r['id']
            msg += """<li><a href="%s">%s</a> %s</li>""" % (url, r['id'], r['title'])
        msg += """</ul>"""

    # Check if datasets not linked to projects
    query = """SELECT id, title, proj_uid FROM dataset d 
               LEFT JOIN project_dataset_map pdm ON pdm.dataset_id = d.id 
               LEFT JOIN dataset_weekly_report dwr ON dwr.dataset_id = d.id
               WHERE proj_uid is NULL 
               AND no_project IS NOT TRUE
               ORDER BY id"""
    cur.execute(query)
    res = cur.fetchall()
    if res:
        msg += """<h3>Datasets not Linked to a Project:</h3><ul>"""
        for r in res:
            url = config['DATASET_LANDING_PAGE'] % r['id']
            msg += """<li><a href="%s">%s</a> %s</li>""" % (url, r['id'], r['title'])
        msg += """</ul>"""

    # datasets that are not yet archived in glacier
    query = """SELECT id, title, status FROM dataset d 
               LEFT JOIN dataset_archive da ON da.dataset_id = d.id 
               WHERE status is NULL 
               OR status != 'Archived' 
               ORDER BY id"""
    cur.execute(query)
    res = cur.fetchall()
    if res:
        msg += """<h3>Datasets Not Archived in Glacier:</h3><ul>"""
        for r in res:
            url = config['DATASET_LANDING_PAGE'] % r['id']
            msg += """<li><a href="%s">%s</a> %s</li>""" % (url, r['id'], r['title'])
        msg += """</ul>"""

    msg += """</body></html>"""

    # print(msg)

    sendEmail(msg, title)
