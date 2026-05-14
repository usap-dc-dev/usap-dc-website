import json
import psycopg2
import psycopg2.extras
import os

config = json.loads(open('/web/usap-dc/htdocs/config.json', 'r').read())

def connect_to_db():
    # info = config['PROD_DATABASE'] # when running on dev server, so we can access prouction DB
    info = config['DATABASE']
    print(info)
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER_CURATOR'],
                            password=info['PASSWORD_CURATOR'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)

def updateEntry(cursor, submissionJson, fileName=None):
    submission = json.loads(submissionJson)
    if not ('dataset_id' in submission or 'project_id' in submission):
        print("\t is missing some the dataset_id or project_id JSON field")
    filenameWithoutExtension = ".".join(fileName.split(".")[0:-1]) if fileName else None
    uid = filenameWithoutExtension
    if uid:
        if 'submitter_name' in submission:
            template = "UPDATE submission SET submitter=%s WHERE submitter is null AND uid=%s RETURNING uid, submitter"
            submitter = submission['submitter_name']
            query = cursor.mogrify(template, (submitter, uid))
            cursor.execute(query)
            modified = cursor.fetchall()
            if len(modified)>0:
                print("Successfully added submitter (%s) to" % submitter, filenameWithoutExtension)
            else:
                print("Couldn't find the matching record for %s in the DB" % filenameWithoutExtension)
        else:
            print(uid, "has no submitter on record")
    else:
        print("Unknowable submission")

def updateEntries(connection, cursor, dirName):
    if not (connection and cursor):
        (connection, cursor) = connect_to_db()
    for jsonFile in list(filter(lambda x: x.endswith('.json'), os.listdir(dirName))):
        filePath = os.path.join(dirName, jsonFile)
        print(jsonFile)
        jsonData = open(filePath, 'r').read()
        updateEntry(cursor, jsonData, jsonFile)
    connection.commit()

(con, cur) = connect_to_db()

updateEntries(con, cur, 'submitted')
