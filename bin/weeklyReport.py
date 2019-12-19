#!/opt/rh/python27/root/usr/bin/python
# Run as a cron task to generate weekly report
# run from main usap-dc directory with python bin/weeklyReport

import os
import datetime
import psycopg2
import psycopg2.extras
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


SUBMITTED_FOLDER = "../submitted"
ISOXML_FOLDER = "../watch/isoxml"
config = json.loads(open('../config.json', 'r').read())
config.update(json.loads(open('../inc/report_config.json', 'r').read()))


def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER'],
                            password=info['PASSWORD'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


def isDatabaseImported(uid):
    (conn, cur) = connect_to_db()
    query = "SELECT COUNT(*) from dataset WHERE id = '%s'" % uid
    cur.execute(query)
    res = cur.fetchone()
    return res['count'] > 0


def isProjectImported(uid):
    (conn, cur) = connect_to_db()
    query = "SELECT COUNT(*) from project WHERE proj_uid = '%s'" % uid
    cur.execute(query)
    res = cur.fetchone()
    return res['count'] > 0


def isEditComplete(uid):
    submission_file = os.path.join(SUBMITTED_FOLDER, uid + ".json")
    if not os.path.exists(submission_file):
        return False
    with open(submission_file) as infile:
        data = json.load(infile)
    return data.get('edit_complete')


def isRegisteredWithDataCite(uid):
    (conn, cur) = connect_to_db()
    query = "SELECT doi from dataset WHERE id = '%s'" % uid
    cur.execute(query)
    res = cur.fetchone()
    return (res['doi'] and len(res['doi']) > 0)


def ISOXMLExists(uid):
    return os.path.exists(getISOXMLFileName(uid))


def getISOXMLFileName(uid):
    return os.path.join(ISOXML_FOLDER, "%siso.xml" % uid)


def sendEmail(message, subject):
    sender = 'info@usap-dc.org'
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


if __name__ == '__main__':
    # get current date
    today = datetime.date.today()
    week_ago = today - datetime.timedelta(days=7)

    title = "USAP-DC WEEKLY REPORT: %s TO %s" % (week_ago, today)

    msg = """<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
             <title>%s</title>""" % title

    msg = """<html><head></head><body><h1>%s</h1>""" % title

    # List any new datasets or projects have been registered.

    # First get SUBMISSIONS from the last week
    current_dir = os.getcwd()
    os.chdir(SUBMITTED_FOLDER)
    files = filter(os.path.isfile, os.listdir(SUBMITTED_FOLDER))
    files = [os.path.join(SUBMITTED_FOLDER, f) for f in files]  # add path to each file
    files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    os.chdir(current_dir)

    pending_projects = []
    pending_datasets = []
    pending_project_edits = []
    pending_dataset_edits = []
    not_registered = []
    no_iso = []
    for f in files:
        if f.find(".json") > 0:
            date = datetime.date.fromtimestamp(os.path.getmtime(f))
            f = os.path.basename(f)
            uid = f.split(".json")[0]
            url = config['CURATOR_PAGE'] % uid
            if uid[0] == 'p':
                if not isProjectImported(uid):
                    pending_projects.append("""<li><a href="%s">%s</a> %s</li>""" % (url, uid, date))
            elif uid[0:2] == 'ep':
                if not isEditComplete(uid):
                    pending_project_edits.append("""<li><a href="%s">%s</a> %s</li>""" % (url, uid[1:], date))
            elif uid[0] == 'e':
                if not isEditComplete(uid):
                    pending_dataset_edits.append("""<li><a href="%s">%s</a> %s</li>""" % (url, uid[1:], date))           
            else:
                if not isDatabaseImported(uid):
                    pending_datasets.append("""<li><a href="%s">%s</a> %s</li>""" % (url, uid, date))
                else:
                    if not isRegisteredWithDataCite(uid):
                        not_registered.append("""<li><a href="%s">%s</a> %s</li>""" % (url, uid, date))
                    elif not ISOXMLExists(uid):
                        no_iso.append("""<li><a href="%s">%s</a> %s</li>""" % (url, uid, date))

    # Get datasets INGESTED in last week
    (conn, cur) = connect_to_db()
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
    for line in pending_datasets:
        msg += line
    msg += """</ul>"""

    msg += """<h3>Datasets Not Yet Registered With Datacite:</h3><ul>"""
    for line in not_registered:
        msg += line
    msg += """</ul>"""

    msg += """<h3>Datasets Missing ISO XML:</h3><ul>"""
    for line in no_iso:
        msg += line
    msg += """</ul>"""

    msg += """<h3>Projects Not Yet Ingested:</h3><ul>"""
    for line in pending_projects:
        msg += line
    msg += """</ul>"""

    msg += """<h3>Pending Dataset Edits:</h3><ul>"""
    for line in pending_dataset_edits:
        msg += line
    msg += """</ul>"""

    msg += """<h3>Pending Project Edits:</h3><ul>"""
    for line in pending_project_edits:
        msg += line
    msg += """</ul>"""  

    # datasets that are not yet archived in  glacier
    query = "SELECT id, title, archived_date FROM dataset d LEFT JOIN dataset_archive da ON da.dataset_id = d.id WHERE archived_date is NULL ORDER BY id"
    cur.execute(query)
    res = cur.fetchall()
    if res:
        msg += """<h3>Datasets Not Archived in Glacier:</h3><ul>"""
        for r in res:
            url = config['DATASET_LANDING_PAGE'] % r['id']
            msg += """<li><a href="%s">%s</a> %s</li>""" % (url, r['id'], r['title'])
        msg += """</ul>"""

    # Check if data set has a valid dif
    query = "SELECT id, title, dif_id FROM dataset d LEFT JOIN dataset_dif_map dfm ON dfm.dataset_id = d.id WHERE dif_id is NULL ORDER BY id"
    cur.execute(query)
    res = cur.fetchall()
    if res:
        msg += """<h3>Datasets Without a Valid DIF:</h3><ul>"""
        for r in res:
            url = config['DATASET_LANDING_PAGE'] % r['id']
            msg += """<li><a href="%s">%s</a> %s</li>""" % (url, r['id'], r['title'])
        msg += """</ul>"""

    # Check if datasets not linked to projects
    query = "SELECT id, title, proj_uid FROM dataset d LEFT JOIN project_dataset_map pdm ON pdm.dataset_id = d.id WHERE proj_uid is NULL ORDER BY id"
    cur.execute(query)
    res = cur.fetchall()
    if res:
        msg += """<h3>Datasets not Linked to a Project:</h3><ul>"""
        for r in res:
            url = config['DATASET_LANDING_PAGE'] % r['id']
            msg += """<li><a href="%s">%s</a> %s</li>""" % (url, r['id'], r['title'])
        msg += """</ul>"""

    msg += """</body></html>"""

    # print(msg)

    sendEmail(msg, title)
