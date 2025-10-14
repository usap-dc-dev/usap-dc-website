from flask import json, send_file, send_from_directory
import usap
import datetime
from collections import OrderedDict
from services.lib.flask_restplus import Resource, reqparse, fields, inputs, Namespace
import services.models as models
import os


config = json.loads(open('config.json', 'r').read())

ns = Namespace('datafiles', description='Operations related to downloading data files', ordered=True)

datafiles_arguments = reqparse.RequestParser()
datafiles_arguments.add_argument('dataset_uid', help='USAP-DC dataset identification number', example='600030')
datafiles_arguments.add_argument('file_name', help='The name of the file to download', example='2009-03-10/Carson, Chris GPS, photo list, structure data.doc')

datafiles_model = ns.model("Datafile", OrderedDict([
    ("dataset_uid", fields.String(attribute="id")),
    ("file_name", fields.String(attribute="filename"))
    ]))

# just a placeholder for now - will be replaced by API key
def canDownload(user):
    return not not user

@ns.route('/<dataset_id>/<path:filename>')
# TODO make this return a CLASS, not a FUNCTION, like in datasets.py
class DataFile(Resource):
    @ns.expect(datafiles_arguments)
    @ns.marshal_with(datafiles_model)
    @ns.response(200, 'Success')
    @ns.response(400, 'Validation Error')
    @ns.response(404, 'File not found')
    def get(self):
        query_parameters = datafiles_arguments.parse_args()
        dataset_id = query_parameters.get("dataset_uid")
        filename = query_parameters.get("filename")
        # test for proprietary hold
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
        directory = os.path.join("dataset", "usap-dc", dataset_id)
        print("Getting the file", filename, "from", directory)
        if canDownload("hello"):
            return send_from_directory(directory, filename, as_attachment=True)
        else:
            return "Can't authenticate your API key. If this persists, contact the USAP-DC team at info@usap-dc.org for assistance."