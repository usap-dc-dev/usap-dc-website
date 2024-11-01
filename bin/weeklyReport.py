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
from functools import reduce

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

def getFairSummary(cur):
    unevaluated = []
    needsUpdate = []
    incomplete = []
    unevaluatedQuery = 'select id from dataset where id not in (select dataset_id from dataset_fairness);'
    print(unevaluatedQuery)
    cur.execute(unevaluatedQuery)
    res = cur.fetchall()
    unevaluated = list(map(lambda x: x['id'], res))
    needsUpdateQuery = 'select id from (select dataset.id, dataset.date_modified, cast(dataset_fairness.reviewed_time as date) as date_reviewed from dataset join dataset_fairness on dataset.id=dataset_fairness.dataset_id) as d where date_modified>date_reviewed order by date_modified desc;'
    print(needsUpdateQuery)
    cur.execute(needsUpdateQuery)
    res = cur.fetchall()
    needsUpdate = list(map(lambda x: x['id'], res))
    incompleteQuery = 'select dataset_id as id from dataset_fairness where \'-2\' in (file_name_check, file_format_check, file_organization_check,\
													  table_header_check, data_content_check, data_process_check,\
													  data_acquisition_check, data_spatial_check, data_variable_check,\
													  data_issues_check, data_ref_check, abstract_check,\
													  data_temporal_check, title_check, keywords_check)'
    if len(needsUpdate) > 0:
        incompleteQuery += ' and dataset_id not in (' + reduce(lambda a,b: a + ','+ b, map(lambda a: '\''+a+'\'', needsUpdate)) + ')'
        print(incompleteQuery)
    cur.execute(incompleteQuery)
    res = cur.fetchall()
    incomplete = list(map(lambda x: x['id'], res))
    retMap = {'unevaluated':unevaluated, 'needsUpdate':needsUpdate, 'incomplete':incomplete}
    return retMap

def formatList(theList, ordered=False):
    ret = '<ol>' if ordered else '<ul>'
    ret += reduce(lambda a,b: a+b, map(lambda c: '<li>' + c + '</li>', theList))
    ret += '</ol>' if ordered else '</ul>'
    return ret


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
    
    smtp_details = config['SMTP']
    s = smtplib.SMTP(smtp_details["SERVER"], smtp_details['PORT'].encode('utf-8'))
    # identify ourselves to smtp client
    s.ehlo()
    # secure our email with tls encryption
    s.starttls()
    # re-identify ourselves as an encrypted connection
    s.ehlo()
    s.login(smtp_details["USER"], smtp_details["PASSWORD"])
    s.sendmail(sender, recipients, msg.as_string())
    s.quit()  

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

    fairLists = getFairSummary(cur)
    unevaluated = fairLists['unevaluated']
    needsUpdate = fairLists['needsUpdate']
    incomplete = fairLists['incomplete']
    getCuratorUrl = lambda uid: '<a href="' + (config['CURATOR_PAGE'] % uid) + '">' + uid + '</a>'
    msg += """<h3>Datasets missing FAIRness evaluation:</h3>"""
    msg += formatList(list(map(getCuratorUrl, unevaluated)))
    msg += """<h3>Datasets edited since their last FAIRness evaluation:</h3>"""
    msg += formatList(list(map(getCuratorUrl, needsUpdate)))
    msg += """<h3>Datasets whose FAIRness evaluation is incomplete:</h3>"""
    msg += formatList(list(map(getCuratorUrl, incomplete)))

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
