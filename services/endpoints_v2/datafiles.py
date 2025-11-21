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
from functools import reduce


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
    fullPath = fileName.split(os.sep)
    parentDir = fullPath[:-1]
    parentPath = os.sep.join(parentDir)
    print("Parent directory:", parentDir)
    realFileName = fullPath[-1]
    print("File name:", realFileName)
    dirList = []
    for dir, subdirs, filenames in os.walk(rootDir):
        if len(parentDir) == 0 or dir.split(os.sep)[-len(parentDir):] == parentDir:
            for fn in filenames:
                if fn == realFileName:
                    dirList.append(dir)
    print("Directory list:", dirList)
    return dirList if 0 == len(parentPath) else list(map(lambda dir: dir[:-len(parentPath)], dirList))

def getPathToFile(dataset_uid, filename):
    (conn, cur) = usap.connect_to_db()
    queryTemplate = "SELECT dir_name, file_name FROM dataset_file WHERE dataset_id=%s AND file_name=%s"
    query = cur.mogrify(queryTemplate, (dataset_uid, filename))
    cur.execute(query)
    results = list(cur.fetchall())
    if 0 == len(results):
        queryTemplate = "SELECT dir_name, file_name FROM dataset_file WHERE dir_name=%s OR dir_name like %s"
        query = cur.mogrify(queryTemplate, (filename, "%s/"+filename))
        cur.execute(query)
        results = list(cur.fetchall())
        return list(map(lambda rslt: rslt[:-len(filename)], results))
    return list(map(lambda rslt: (rslt['dir_name']+rslt['file_name'])[:-len(filename)], results))


def findSubdir(rootDir, subDirName):
    fullPath = subDirName.split(os.sep)
    dirList = []
    for dir, subdirs, filenames in os.walk(rootDir):
        if dir.split(os.sep)[-len(fullPath)] == fullPath:
            dirList.append(dir)
    return dirList

def findDeepestCommonSubdir(paths):
    if not paths or len(paths)==0:
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
    return my_zip.getvalue(), {"Content-Type": "application/zip"}

def zipDir(dir):
    my_zip = BytesIO()
    with ZipFile(my_zip, mode="w", compression=ZIP_DEFLATED) as zip:
        for dir, subdirs, files in os.walk(dir):
            pathPrefix = dir
            for filename in files:
                file = open(os.path.join(dir, filename))
                contents = file.read()
                pathWithinZip = os.path.join(dir, filename)
                zip.writestr(pathWithinZip, contents)
    return my_zip.getvalue(), {"Content-Type": "application/zip"}

base_url = "{0}{1}/".format(config['API_BASE'], ns.path)
examples = """Base URL: {0}\nExample:\n
        wget --header="X-Auth-Token:YourApiKeyHere" {0}600030/2009-03-10/Carson%20map%20explanation%20May%2026%20%2704.doc""".format(base_url)

@ns.route('/<dataset_uid>/<path:file_name>', doc={'description':"Pass in your API key with the X-Auth-Token header. To request your free API key, email us at info@usap-dc.org.\n"+examples})
class DataFileItem(Resource):
    @ns.response(200, 'Success')
    @ns.response(401, 'Missing or invalid API key')
    @ns.response(404, 'File not found')
    def get(self, dataset_uid, file_name):
        """Downloads the designated file from the designated data set, if it exists and has no proprietary hold."""
        # test API key
        apiKey = request.headers["X-Auth-Token"]
        if cf.isValidApiKey(apiKey):
            datasets = usap.get_datasets([dataset_uid])
            if 0 == len(datasets):
                return Response("Dataset " + str(dataset_uid) + " not found", 404, {})
            # test for proprietary hold
            ds = datasets[0]
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
                parentDirs = list(map(lambda path: "dataset"+path, getPathToFile(dataset_uid, file_name)))
            if len(parentDirs) == 0:
                watchFile = os.path.join("watch", "dcxml", dataset_uid)
                if not os.path.exists(watchFile):
                    return Response("File not found", 404, {})
                difId = None
                for line in open(watchFile, encoding="UTF-8"):
                    if "dif_id" in line:
                        startOfDifId = line.find(">", line.find("dif_id"))+1
                        endOfDifId = line.find("<", startOfDifId)
                        difId = line[startOfDifId:endOfDifId]
                if difId:
                    possibleLocations = findSubdir("dataset", difId)
                    parentDirs = reduce(lambda acc, cur: [*acc, *cur], list(map(lambda loc: getDirsWithMatchingFile(loc, file_name), possibleLocations)), [])
            if len(parentDirs) == 0:
                    return Response("File not found", 404, {})
            cf.recordApiDownload(apiKey, dataset_uid, file_name)
            hasFile = reduce(lambda acc,cur: acc or os.path.exists(cur+os.sep+file_name), parentDirs, False)
            if not hasFile:
                return Response("File not found", 404, {})
            if len(parentDirs) == 1:
                if os.path.isdir(parentDirs[0] + os.sep + file_name):
                    return zipDir(parentDirs[0] + os.sep + file_name)
                return send_from_directory(parentDirs[0], file_name, as_attachment=True)
            return makeZip(parentDirs, file_name)
        return Response("Missing or invalid API key", 401, {'WWW-Authenticate':'Basic realm="USAP-DC Download API"'})