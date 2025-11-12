from flask import json, abort, jsonify, send_file, send_from_directory, request, Response
import usap
from datetime import datetime
from collections import OrderedDict
from services.lib.flask_restplus import Resource, reqparse, fields, inputs, Namespace
import services.models as models
import os
from zipfile import ZipFile, ZIP_DEFLATED
from tempfile import TemporaryDirectory
from io import BytesIO
import psycopg2
import psycopg2.extras
import lib.curatorFunctions as cf


config = json.loads(open('config.json', 'r').read())

ns = Namespace('datafiles', description='Operations related to downloading data files', ordered=True)

datafiles_arguments = reqparse.RequestParser()
datafiles_arguments.add_argument('dataset_uid', help='USAP-DC dataset identification number', example='600030')
datafiles_arguments.add_argument('file_name', help='The name of the file to download', example='2009-03-10/Carson, Chris GPS, photo list, structure data.doc')

datafiles_model = ns.model("Datafile", OrderedDict([
    ("dataset_uid", fields.String(attribute="id")),
    ("file_name", fields.String(attribute="filename"))
    ]))

def connect_to_db(curator=False):
    info = config['DATABASE']
    if curator and cf.isCurator():
        user = info['USER_CURATOR']
        password = info['PASSWORD_CURATOR']
    else:
        user = info['USER']
        password = info['PASSWORD']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=user,
                            password=password)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)

conn, cur = connect_to_db()

#placeholder for now
def encrypt(kw):
    return kw

def isValidApiKey(key):
    queryTxt = "select * from api_key where encrypted_key=%s"
    query = cur.mogrify(queryTxt, (key,))
    cur.execute(query)
    results = cur.fetchall()
    valid = False
    for result in results:
        if result['status'] == "valid":
            valid = True
    return valid

def getFilesWithName(rootDir, file_name):
    fileList = []
    for dir, subDirs, filenames in os.walk(rootDir):
        for fn in filenames:
            if fn == file_name:
                fileList.append(os.path.join(dir, fn))
    return fileList

def getDirsWithMatchingFile(rootDir, fileName):
    dirList = []
    for dir, subdirs, filenames in os.walk(rootDir):
        for fn in filenames:
            if fn == fileName:
                dirList.append(dir)
    return dirList

def findDeepestCommonSubdir(paths):
    if not paths:
        return ""
    if len(paths) == 1:
        return paths[0]
    splitPaths = map(lambda path: path.split("/"), paths)
    longestCommon = paths[0]
    for sp in splitPaths[1:]:
        for i in range(min(len(sp), len(longestCommon))):
            if sp[i] != longestCommon[i]:
                longestCommon = sp[0:i]
                break
    return os.path.join(longestCommon)


def makeZip(dirs, filename):
    my_zip = BytesIO()
    pathPrefix = findDeepestCommonSubdir(dirs)
    with ZipFile(my_zip, mode="w", compression=ZIP_DEFLATED) as zip:
        for dir in dirs:
            file = open(os.path.join(dir, filename))
            contents = file.read()
            path_within_zip = dir[pathPrefix.length+1:] + filename
            zip.writestr(path_within_zip, contents)
    return my_zip.getvalue(), {"Content-Type: application/zip"}

@ns.route('/<dataset_uid>/<file_name>')
class DataFileItem(Resource):
    @ns.response(200, 'Success')
    @ns.response(401, 'Missing or invalid API key')
    @ns.response(404, 'File not found')
    def get(self, dataset_uid, file_name):
        """Downloads the designated file from the designated data set, if it exists and has no proprietary hold.\nPass in your API key with the X-Auth-Token header. To request your free API key, email us at info@usap-dc.org."""
        # test API key
        apiKey = request.headers["X-Auth-Token"]
        if cf.isValidApiKey(apiKey):
            # test for proprietary hold
            ds = usap.get_datasets([dataset_uid])[0]
            holdTime = ""
            # check for proprietary hold
            if len(ds['release_date']) == 4:
                hold = datetime.strptime(ds['release_date'], '%Y') > datetime.utcnow()
                holdTime = datetime.strptime(ds['release_date'], '%Y')
            elif len(ds['release_date']) == 10:  
                hold = datetime.strptime(ds['release_date'], '%Y-%m-%d') > datetime.utcnow()
                holdTime = datetime.strptime(ds['release_date'], '%Y-%m-%d')
            else:
                hold = False
            if hold:
                return "There is a hold on this data. Try again on or after " + holdTime + "."
            directory = os.path.join("dataset", "usap-dc", dataset_uid)
            print("Getting the file", file_name, "from", directory)
            parentDirs = getDirsWithMatchingFile(directory, file_name)
            if len(parentDirs) == 0:
                return usap.not_found()
            cf.recordApiDownload(apiKey, dataset_uid, file_name)
            if len(parentDirs) == 1:
                return send_from_directory(parentDirs[0], file_name, as_attachment=True)
            return makeZip(parentDirs, file_name)
        return Response("Missing or invalid API key", 401, {'WWW-Authenticate':'Basic realm="USAP-DC Download API"'})