from flask import json, send_file, send_from_directory
import usap
import datetime
from collections import OrderedDict
from services.lib.flask_restplus import Resource, reqparse, fields, inputs, Namespace
import services.models as models
import os
from zipfile import ZipFile, ZIP_DEFLATED
from tempfile import TemporaryDirectory
from io import BytesIO


config = json.loads(open('config.json', 'r').read())

ns = Namespace('datafiles', description='Operations related to downloading data files', ordered=True)

datafiles_arguments = reqparse.RequestParser()
datafiles_arguments.add_argument('dataset_uid', help='USAP-DC dataset identification number', example='600030')
datafiles_arguments.add_argument('file_name', help='The name of the file to download', example='2009-03-10/Carson, Chris GPS, photo list, structure data.doc')

datafiles_model = ns.model("Datafile", OrderedDict([
    ("dataset_uid", fields.String(attribute="id")),
    ("file_name", fields.String(attribute="filename"))
    ]))

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


# just a placeholder for now - will be replaced by API key
def canDownload(user):
    return not not user

@ns.route('/<dataset_uid>/<file_name>')
class DataFileItem(Resource):
    @ns.response(200, 'Success')
    @ns.response(404, 'File not found')
    def get(self, dataset_uid, file_name):
        """# test for proprietary hold
        ds = usap.get_datasets([dataset_id])[0]
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
        """
        directory = os.path.join("dataset", "usap-dc", dataset_uid)
        print("Getting the file", file_name, "from", directory)
        if canDownload("hello"):
            parentDirs = getDirsWithMatchingFile(directory, file_name)
            if len(parentDirs) == 1:
                return send_from_directory(parentDirs[0], file_name, as_attachment=True)
            return makeZip(parentDirs, file_name)
        return "<html><body>Can't authenticate your API key. If this persists, contact the USAP-DC team at info@usap-dc.org for assistance.</body></html>"