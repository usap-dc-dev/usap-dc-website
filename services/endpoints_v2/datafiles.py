from flask import json, send_file, send_from_directory
import usap
import datetime
from collections import OrderedDict
from services.lib.flask_restplus import Resource, reqparse, fields, inputs, Namespace
import services.models as models


config = json.loads(open('config.json', 'r').read())

ns = Namespace('datafiles', description='Operations related to downloading data files', ordered=True)

# just a placeholder for now - will be replaced by API key
def canDownload(user):
    return not not user

@ns.route('/<dataset_id>/<path:filename>')
def getFile(dataset_id, filename):
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
    directory = os.path.join(usap.current_app.root_path, usap.app.config['DATASET_FOLDER'])
    if canDownload("hello"):
        return send_from_directory(directory, filename, as_attachment=True)
    else:
        return "Can't authenticate your API key. If this persists, contact the USAP-DC team at info@usap-dc.org for assistance."