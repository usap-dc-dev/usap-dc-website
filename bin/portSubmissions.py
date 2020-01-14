#!/opt/rh/python27/root/usr/bin/python

# script to submissions to submission table
# to run from main dir: >python bin/portSubmissions
import psycopg2
import psycopg2.extras
import json
import os
from datetime import datetime


config = json.loads(open('config.json', 'r').read())
submitted_dir = "/web/usap-dc/htdocs/submitted"
ISOXML_FOLDER = "/web/usap-dc/htdocs/watch/isoxml"
DIFXML_FOLDER = "/web/usap-dc/htdocs/watch/difxml"


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
    submitted_dir = "submitted"
    submission_file = os.path.join(submitted_dir, uid + ".json")
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


def DifXMLExists(uid):
    return os.path.exists(getDifXMLFileName(uid))


def getDifXMLFileName(uid):
    return os.path.join(DIFXML_FOLDER, "%s.xml" % getDifID(uid))


def getDifID(uid):
    conn, cur = connect_to_db()
    query = "SELECT award_id FROM project_award_map WHERE is_main_award = 'True' AND proj_uid = '%s';" % uid
    cur.execute(query)
    res = cur.fetchone()
    return "USAP-%s_1" % res['award_id'] 


def getDatasetCreatedDate(uid):
    return getDate('dataset', uid, 'date_created')


def getDatasetModifiedDate(uid):
    return getDate('dataset', uid[1:], 'date_modified')


def getProjectCreatedDate(uid):
    return getDate('project', uid, 'date_created')


def getProjectModifiedDate(uid):
    return getDate('project', uid[1:], 'date_modified')


def getDate(table, uid, date_type):
    if table == 'dataset':
        uid_col = 'id'
    else:
        uid_col = 'proj_uid'
    (conn, cur) = connect_to_db()
    query = "SELECT * from %s WHERE %s = '%s'" % (table, uid_col, uid)
    cur.execute(query)
    res = cur.fetchone()
    if res:
        return res.get(date_type)
    return None


# get list of json files in submission directory, ordered by date
current_dir = os.getcwd()
os.chdir(submitted_dir)
files = filter(os.path.isfile, os.listdir(submitted_dir))
files = [os.path.join(submitted_dir, f) for f in files]  # add path to each file
files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
os.chdir(current_dir)
(conn, cur) = connect_to_db()
today = datetime.now().strftime('%Y-%m-%d')

for f in files:
    if f.find(".json") > 0:
        date = datetime.utcfromtimestamp(os.path.getmtime(f)).strftime('%Y-%m-%d %H:%M:%S')
        last_update = date
        f = os.path.basename(f)
        uid = f.split(".json")[0]
        # set submission status
        if uid[0] == 'p':
            submission_type = 'project submission'
            if not isProjectImported(uid):
                status = "Pending"
            else:
                if DifXMLExists(uid):
                    status = "Completed"
                else:
                    status = "DIF XML file missing"
                last_update = getProjectCreatedDate(uid)
        elif uid[0:2] == 'ep':
            submission_type = 'project edit'
            if isEditComplete(uid):
                status = "Edit completed"
                last_update = getProjectModifiedDate(uid)
                if not last_update:
                    last_update = date
            else:
                status = "Pending"
        elif uid[0] == 'e':
            submission_type = 'dataset edit'
            if isEditComplete(uid):
                status = "Edit completed"
                last_update = getDatasetModifiedDate(uid)
                if not last_update:
                    last_update = date
            else:
                status = "Pending"       
        else:
            submission_type = 'dataset submission'
            if not isDatabaseImported(uid):
                status = "Pending"
            else:
                landing_page = '/view/dataset/%s' % uid
                if not isRegisteredWithDataCite(uid):
                    status = "Not yet registered with DataCite"
                elif not ISOXMLExists(uid):
                    status = "ISO XML file missing"
                else:
                    status = "Completed"
                last_update = getDatasetCreatedDate(uid)

        query = "INSERT INTO submission (uid, submission_type, status, submitted_date, last_update) VALUES ('%s', '%s', '%s', '%s', '%s')" \
                % (uid, submission_type, status, date, last_update)
        cur.execute(query)
cur.execute('COMMIT')
