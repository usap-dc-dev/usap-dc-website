import math
import flask
from flask import Flask, session, render_template, redirect, url_for, request, send_from_directory, send_file, current_app, make_response
from flask_jsglue import JSGlue
from random import randint
import os
from authlib.integrations.flask_client import OAuth
import json
from urllib.request import urlopen
from urllib.parse import urlparse, unquote, urlencode
from werkzeug.utils import secure_filename
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import psycopg2
import psycopg2.extras
import requests
import re
import copy
from datetime import datetime, timedelta, date as dt_date
import csv
from collections import namedtuple
import humanize
import lib.json2sql as json2sql
import shutil
import lib.curatorFunctions as cf
from functools import partial
from services.api_v1 import blueprint as api_v1
# from services.api_v2 import blueprint as api_v2
import services.settings as rp_settings
import traceback
import pandas as pd
import pickle
from apiclient.discovery import build
from google.auth.transport.requests import Request as gRequest
import base64
import email
from email.header import decode_header
from dateutil.parser import parse
from lib.gmail_functions import send_gmail_message
import lib.difHarvest as dh
from pathlib import Path
import xml.etree.ElementTree as ET

app = Flask(__name__)
jsglue = JSGlue(app)

############
# Load configuration
############
app.config.update(
    SESSION_TYPE="filesystem",
    SESSION_FILE_DIR="flask_session",
    PERMANENT_SESSION_LIFETIME=86400,
    UPLOAD_FOLDER="upload",
    DATASET_FOLDER="dataset",
    SUBMITTED_FOLDER="submitted",
    SAVE_FOLDER="saved",
    DOCS_FOLDER="doc",
    AWARDS_FOLDER="awards",
    METADATA_FOLDER="watch",
    CROSSREF_FILE="inc/crossref_sql.txt",
    OLD_CROSSREF_FILE="inc/old_crossref_sql.txt",
    DOI_REF_FILE="inc/doi_ref",
    PROJECT_REF_FILE="inc/project_ref",
    GMAIL_PICKLE="inc/token.pickle",
    AWARD_WELCOME_EMAIL="static/letters/USAP_DCwelcomeletter.html",
    AWARD_FINAL_EMAIL="static/letters/USAP_DCcloseoutletter.html",
    AWARD_EMAIL_BANNER="/static/letters/images/image1.png",
    DEBUG=True
)

app.config.update(json.loads(open('config.json', 'r').read()))

app.debug = app.config['DEBUG']
app.secret_key = app.config['SECRET_KEY']


app.register_blueprint(api_v1)
# set up api v2 for future use
# app.register_blueprint(api_v2)

app.config['SWAGGER_UI_DOC_EXPANSION'] = rp_settings.RESTPLUS_SWAGGER_UI_DOC_EXPANSION
app.config['RESTPLUS_VALIDATE'] = rp_settings.RESTPLUS_VALIDATE
app.config['RESTPLUS_MASK_SWAGGER'] = rp_settings.RESTPLUS_MASK_SWAGGER
app.config['ERROR_404_HELP'] = rp_settings.RESTPLUS_ERROR_404_HELP
app.config['BUNDLE_ERRORS'] = rp_settings.RESTPLUS_BUNDLE_ERRORS


@app.route('/api')
def api():
    return render_template('api_swagger.html', api_url=url_for('api.doc'))


oauth = OAuth(app)

google = oauth.register('google',
                        client_id=app.config['GOOGLE_CLIENT_ID'],
                        client_secret=app.config['GOOGLE_CLIENT_SECRET'],
                        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
                        client_kwargs={'scope': 'openid profile email'})

orcid = oauth.register('orcid',
                       client_id=app.config['ORCID_CLIENT_ID'],
                       client_secret=app.config['ORCID_CLIENT_SECRET'],
                       server_metadata_url='https://orcid.org/.well-known/openid-configuration',
                       client_kwargs={'scope': 'openid '})

config = json.loads(open('config.json', 'r').read())

def connect_to_prod_db(curator=False):
    info = config['PROD_DATABASE']
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


def get_email_template(editing, submissionType, uid, data, hasId, doi=None):
    text = "Dear %s,\n\n" % data.get('submitter_name')
    if editing:
        url = url_for("project_landing_page", project_id=uid, _external=True) if submissionType == "project" else url_for("landing_page", dataset_id=uid, _external=True)
        text += "This is to confirm that your %s, %s, has been successfully updated.\n" % (submissionType, data.get("title"))
        text += "Please check the landing page %s and contact us if there are any issues." % url
    else:
        if submissionType == "project":
            text += "This is to confirm that your project, %s, has been successfully registered at USAP-DC.\n" % data.get("title")
            text += "Please check the landing page %s and contact us (info@usap-dc.org) if there are any issues." % url_for("project_landing_page", project_id=uid, _external=True)
            if hasId:
                text += "\n\nWe have also prepared and submitted a catalog entry (DIF) at the Antarctic Metadata Directory (AMD)."
                text += "\nThe DIF ID will be %s." % cf.getDifID(uid)
                text += "\nThe direct link to the AMD record will be %s." % cf.getDifUrl(uid)
                text += "\n\nIt usually takes AMD staff a few business days to review the submission before it goes live."
            else:
                text += "\n\nIf everything looks fine, I will also prepare and submit an entry (DIF record) to the Antarctic Metadata Directory (AMD)."
                text += "\n\nYou can update the project page in the future using the 'edit' function in the top right, e.g. when new datasets or publications become available. In the case that you archive your dataset(s) at the USAP-DC repository, we will automatically link the dataset to the project."
                text += "\n\nAny edits will be reviewed by a USAP-DC curator before they become live."
        else:
            text += "We have processed your dataset %s, and added it to the USAP-DC repository.\nThe dataset ID is %s." % (data.get("title"), uid)
            #print(data)
            if len(data['awards']) > 0:
                query = "SELECT proj_uid FROM project_dataset_map WHERE dataset_id = %s"
                (conn, cur) = connect_to_db(curator=True)
                query_str = cur.mogrify(query, (uid,))
                cur.execute(query_str)
                proj_uids_tuples = cur.fetchall()
                #print(proj_uids_tuples)
                proj_uids = list(map(lambda x : x['proj_uid'], proj_uids_tuples))
                #print(proj_uids)
                if len(proj_uids) == 1:
                    text += "\n\nBased on the award number, we have linked the dataset to the project %s." % url_for("project_landing_page", project_id=proj_uids[0], _external=True)
                else:
                    text += "\n\nBased on the award numbers, we have linked the dataset to the following %s projects:" % len(proj_uids)
                    for proj_uid in proj_uids:
                        text += "\n%s" % url_for("project_landing_page", project_id=proj_uid, _external=True)
            text += "\n\nPlease check the landing page %s and let us know if everything looks good or if there are any issues.\n\n" % url_for("landing_page", dataset_id=uid, _external=True)
            if hasId:
                text += "The DOI for the dataset is %s." % doi
            else:
                text += "If everything is fine, we will create a DOI for the dataset."
    text += "\n\nBest regards,\n"
    return text

def get_nsf_grants(columns, award=None, only_inhabited=True):
    (conn, cur) = connect_to_db()
    query_string = """SELECT %s FROM award a WHERE a.award != 'XXXXXXX' and a.award != 'None' and a.award ~ '^[0-9]' 
                      and a.award::integer<8000000 and a.award::integer>0400000""" % ','.join(columns) 
    
    if only_inhabited:
        query_string += ' AND EXISTS (SELECT award_id FROM dataset_award_map dam WHERE dam.award_id=a.award)'
    query_string += ' ORDER BY name,award'

    cur.execute(query_string)
    return cur.fetchall()


def get_datasets(dataset_ids):
    if len(dataset_ids) == 0:
        return []
    else:
        (conn, cur) = connect_to_db()
        query_string = \
                   cur.mogrify(
                       '''SELECT d.*,
                             CASE WHEN a.awards IS NULL THEN '[]'::json ELSE a.awards END,
                             CASE WHEN k.keywords IS NULL THEN '[]'::json ELSE k.keywords END,
                             CASE WHEN par.parameters IS NULL THEN '[]'::json ELSE par.parameters END,
                             CASE WHEN l.locations IS NULL THEN '[]'::json ELSE l.locations END,
                             CASE WHEN per.persons IS NULL THEN '[]'::json ELSE per.persons END,
                             CASE WHEN pl.platforms IS NULL THEN '[]'::json ELSE pl.platforms END,
                             CASE WHEN sen.sensors IS NULL THEN '[]'::json ELSE sen.sensors END,
                             CASE WHEN ref.references IS NULL THEN '[]'::json ELSE ref.references END,
                             CASE WHEN sp.spatial_extents IS NULL THEN '[]'::json ELSE sp.spatial_extents END,
                             CASE WHEN tem.temporal_extents IS NULL THEN '[]'::json ELSE tem.temporal_extents END,
                             CASE WHEN prog.programs IS NULL THEN '[]'::json ELSE prog.programs END,
                             CASE WHEN proj.projects IS NULL THEN '[]'::json ELSE proj.projects END,
                             CASE WHEN dif.dif_records IS NULL THEN '[]'::json ELSE dif.dif_records END,
                             CASE WHEN rel_proj.rel_projects IS NULL THEN '[]'::json ELSE rel_proj.rel_projects END,
                             license.url AS license_url, license.label AS license_label
                       FROM
                        dataset d
                        LEFT JOIN (
                            SELECT dam.dataset_id, json_agg(a) awards
                            FROM dataset_award_map dam JOIN award a ON (a.award=dam.award_id)
                            WHERE a.award != 'XXXXXXX'
                            GROUP BY dam.dataset_id
                        ) a ON (d.id = a.dataset_id)
                        LEFT JOIN (
                            SELECT kw.dataset_id, json_agg(kw) keywords FROM (
                                SELECT dkm.dataset_id, ku.keyword_label AS keyword_label, ku.keyword_description AS keyword_description
                                FROM dataset_keyword_map dkm JOIN keyword_usap ku ON (ku.keyword_id=dkm.keyword_id)
                                UNION
                                SELECT dkm.dataset_id, ki.keyword_label AS keyword_label, ki.keyword_description AS keyword_description
                                FROM dataset_keyword_map dkm JOIN keyword_ieda ki ON (ki.keyword_id=dkm.keyword_id)   
                            ) kw
                            GROUP BY kw.dataset_id
                        ) k ON (d.id = k.dataset_id)
                        LEFT JOIN (
                            SELECT dparm.dataset_id, json_agg(par) parameters
                            FROM dataset_parameter_map dparm JOIN parameter par ON (par.id=dparm.parameter_id)
                            GROUP BY dparm.dataset_id
                        ) par ON (d.id = par.dataset_id)
                        LEFT JOIN (
                            SELECT dataset_id, json_agg(keyword_label) locations
                            FROM vw_dataset_location vdl
                            GROUP BY dataset_id
                        ) l ON (d.id = l.dataset_id)
                        LEFT JOIN (
                            SELECT dperm.dataset_id, json_agg(per) persons
                            FROM dataset_person_map dperm JOIN person per ON (per.id=dperm.person_id)
                            GROUP BY dperm.dataset_id
                        ) per ON (d.id = per.dataset_id)
                        LEFT JOIN (
                            SELECT dplm.dataset_id, json_agg(pl) platforms
                            FROM dataset_platform_map dplm JOIN platform pl ON (pl.id=dplm.platform_id)
                            GROUP BY dplm.dataset_id
                        ) pl ON (d.id = pl.dataset_id)
                        LEFT JOIN (
                            SELECT dsenm.dataset_id, json_agg(sen) sensors
                            FROM dataset_sensor_map dsenm JOIN sensor sen ON (sen.id=dsenm.sensor_id)
                            GROUP BY dsenm.dataset_id
                        ) sen ON (d.id = sen.dataset_id)
                        LEFT JOIN (
                            SELECT drm.dataset_id, json_agg(ref) AS references
                            FROM dataset_reference_map drm JOIN reference ref ON ref.ref_uid=drm.ref_uid
                            GROUP BY drm.dataset_id
                        ) ref ON (d.id = ref.dataset_id)
                        LEFT JOIN (
                            SELECT sp.dataset_id, json_agg(sp) spatial_extents
                            FROM dataset_spatial_map sp
                            GROUP BY sp.dataset_id
                        ) sp ON (d.id = sp.dataset_id)
                        LEFT JOIN (
                            SELECT tem.dataset_id, json_agg(tem) temporal_extents
                            FROM dataset_temporal_map tem
                            GROUP BY tem.dataset_id
                        ) tem ON (d.id = tem.dataset_id)
                        LEFT JOIN (
                            SELECT dam.dataset_id, json_agg(prog) programs
                            FROM dataset_award_map dam, award_program_map apm, program prog
                            WHERE dam.award_id = apm.award_id AND apm.program_id = prog.id
                            GROUP BY dam.dataset_id
                        ) prog ON (d.id = prog.dataset_id)
                        LEFT JOIN (
                            SELECT dprojm.dataset_id, json_agg(proj) projects
                            FROM dataset_initiative_map dprojm JOIN initiative proj ON (proj.id=dprojm.initiative_id)
                            GROUP BY dprojm.dataset_id
                        ) proj ON (d.id = proj.dataset_id)
                        LEFT JOIN (
                            SELECT ddm.dataset_id, json_agg(dif) dif_records
                            FROM dataset_dif_map ddm JOIN dif ON (dif.dif_id=ddm.dif_id)
                            GROUP BY ddm.dataset_id
                        ) dif ON (d.id = dif.dataset_id)
                        LEFT JOIN (
                            SELECT pdm.dataset_id, json_agg(proj) rel_projects
                            FROM project_dataset_map pdm JOIN project proj ON (proj.proj_uid=pdm.proj_uid)
                            GROUP BY pdm.dataset_id
                        ) rel_proj ON (d.id = rel_proj.dataset_id)
                        LEFT JOIN license ON (d.license = license.id)
                        WHERE d.id IN %s ORDER BY d.title''',
                       (tuple(dataset_ids),))
        cur.execute(query_string)
        return cur.fetchall()


def get_parameters(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT DISTINCT id FROM gcmd_science_key'
    query += ' ORDER BY id'
    cur.execute(query)
    return cur.fetchall()


def get_titles(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT DISTINCT title FROM dataset ORDER BY title'
    cur.execute(query)
    return cur.fetchall()


def get_locations(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT DISTINCT id FROM gcmd_location'
    query += ' ORDER BY id'
    cur.execute(query)
    return cur.fetchall()


def get_usap_locations(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = "SELECT * FROM vw_location"
    query += ' ORDER BY keyword_label'
    cur.execute(query)
    return cur.fetchall()


def get_keywords(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM keyword'
    if dataset_id:
        query += cur.mogrify(' WHERE id in (SELECT keyword_id FROM dataset_keyword_map WHERE dataset_id=%s)', (dataset_id,)).decode()
    query += ' ORDER BY id'
    cur.execute(query)
    return cur.fetchall()


def get_platforms(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM platform'
    if dataset_id:
        query += cur.mogrify(' WHERE id in (SELECT platform_id FROM dataset_platform_map WHERE dataset_id=%s)', (dataset_id,)).decode()
    query += ' ORDER BY id'
    cur.execute(query)
    return cur.fetchall()


def get_persons(conn=None, cur=None, dataset_id=None, order=True):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM person'
    if dataset_id:
        query += cur.mogrify(' WHERE id in (SELECT person_id FROM dataset_person_map WHERE dataset_id=%s)', (dataset_id,)).decode()
    if order:
        query += ' ORDER BY id'
    cur.execute(query)
    return cur.fetchall()


def get_project_persons(conn=None, cur=None, project_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM person'
    if project_id:
        query += cur.mogrify(' WHERE id in (SELECT person_id FROM project_person_map WHERE proj_uid=%s)', (project_id,)).decode()
    query += ' ORDER BY id'
    cur.execute(query)
    return cur.fetchall()


def get_person(person_id):
    (conn, cur) = connect_to_db()
    query = 'SELECT * FROM person'
    if person_id:
        query += cur.mogrify(' WHERE id = %s', (person_id,)).decode()
    cur.execute(query)
    return cur.fetchone()


def get_sensors(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM sensor'
    if dataset_id:
        query += cur.mogrify(' WHERE id in (SELECT sensor_id FROM dataset_sensor_map WHERE dataset_id=%s)', (dataset_id,)).decode()
    query += ' ORDER BY id'
    cur.execute(query)
    return cur.fetchall()


def get_references(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM reference'
    if dataset_id:
        query += cur.mogrify(' WHERE ref_uid in (SELECT ref_uid FROM dataset_reference_map WHERE dataset_id=%s)', (dataset_id,)).decode()
    cur.execute(query)
    return cur.fetchall()


def get_spatial_extents(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM dataset_spatial_map'
    if dataset_id:
        query += cur.mogrify(' WHERE dataset_id=%s', (dataset_id,)).decode()
    cur.execute(query)
    return cur.fetchall()


def get_temporal_extents(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM dataset_temporal_map'
    if dataset_id:
        query += cur.mogrify(' WHERE dataset_id=%s', (dataset_id,)).decode()
    cur.execute(query)
    return cur.fetchall()


def get_programs(conn=None, cur=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM program'
    cur.execute(query)
    return cur.fetchall()


def get_projects(conn=None, cur=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM initiative ORDER BY ID'
    cur.execute(query)
    # need to convert from RealDictRow to dict
    return [dict(row) for row in cur.fetchall()]


def get_licenses(conn=None, cur=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM license WHERE valid_option = true ORDER BY ID'
    cur.execute(query)
    return cur.fetchall()


def get_orgs(conn=None, cur=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM organizations ORDER BY name'
    cur.execute(query)
    return cur.fetchall()


def get_roles(conn=None, cur=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT id FROM role'
    cur.execute(query)
    return cur.fetchall()


def get_deployment_types(conn=None, cur=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM deployment_type ORDER BY deployment_type'
    cur.execute(query)
    return cur.fetchall()


def get_files(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM dataset_file '
    if dataset_id:
        query += cur.mogrify(' WHERE dataset_id=%s', (dataset_id,)).decode()
    query += ' ORDER BY file_name;'
    cur.execute(query)
    return cur.fetchall()


def get_gcmd_platforms(conn=None, cur=None):
    query = "SELECT id FROM gcmd_platform WHERE id != 'Not provided' ORDER BY id"
    return gcmd_id_to_json(conn, cur, query, 'GCMD Platforms', 'Not Provided')


def get_gcmd_instruments(conn=None, cur=None):
    query = "SELECT id FROM gcmd_instrument WHERE id !~* 'NOT APPLICABLE' ORDER BY id"
    return gcmd_id_to_json(conn, cur, query, 'GCMD Instruments', 'NOT APPLICABLE')


def get_gcmd_paleo_time(conn=None, cur=None):
    query = "SELECT id FROM gcmd_paleo_time WHERE id !~* 'NOT APPLICABLE' ORDER BY id"
    return gcmd_id_to_json(conn, cur, query, 'GCMD Paleo Time', 'NOT APPLICABLE')


def get_gcmd_progress():
    (conn, cur) = connect_to_db()
    query = 'SELECT * FROM gcmd_collection_progress'
    cur.execute(query)
    return cur.fetchall()


def get_product_levels():
    (conn, cur) = connect_to_db()
    query = "SELECT * FROM product_level WHERE id != 'Not provided'"
    cur.execute(query)
    return cur.fetchall()


def get_gcmd_data_types():
    (conn, cur) = connect_to_db()
    query = 'SELECT * FROM gcmd_collection_data_type'
    cur.execute(query)
    return cur.fetchall()


def get_gcmd_data_formats():
    (conn, cur) = connect_to_db()
    query = "SELECT * FROM gcmd_data_format WHERE short_name != 'Not Provided'"
    cur.execute(query)
    return cur.fetchall()  


# function to convert gcmd ids from DB tables into json that can be used to populate bootstrap-treeviews
def gcmd_id_to_json(conn=None, cur=None, query=None, base_node_text=None, none_option=None):

    if not (query and base_node_text):
        return[]

    if not (conn and cur):
        (conn, cur) = connect_to_db() 
    cur.execute(query)
    res = cur.fetchall()

    json = [{'text': base_node_text, 'nodes': []}]
    if none_option:
        json[0]['nodes'].append({'text': none_option, 'id': none_option})

    for r in res:
        this_id = r['id']
        parts = this_id.split(' > ')
        parent_node = [p for p in json if p['text'] == base_node_text][0]
        for idx, part in enumerate(parts):
            if idx > 0:
                parent_node = [p for p in parent_node['nodes'] if p['text'] == parts[idx-1]][0]
            if idx == len(parts) - 1:
                node = {'text': part, 'id': this_id}
                if parent_node.get('nodes'):
                    parent_node['nodes'].append(node)
                else:
                    parent_node['nodes'] = [node]

    return json


def check_user_permission(user_info, uid, project=False):
    # if user is a curator, always return true
    if cf.isCurator():
        return True

    if project:
        persons = get_project_persons(project_id=uid)
    else:
        # get users associated with this dataset
        persons = get_persons(dataset_id=uid)
    # check if orcid or email address from user_info matches any of these users
    for p in persons:
        if (p.get('email') and user_info.get('email') and p['email'].lower() == user_info['email'].lower()) \
           or (p.get('id_orcid') and p['id_orcid'] == user_info.get('orcid')):
            return True

    return False


#sort list numerically instead of alphabetically
def sortNumerically(val, replace_str, replace_str2=''):
    return int(val.replace(replace_str, '0').replace(replace_str2, ''))


@app.route('/edit/dataset/<dataset_id>', methods=['GET', 'POST'])
@app.route('/submit/dataset', methods=['GET', 'POST'])
def dataset(dataset_id=None):
    error = ''
    success = ''
    session['error'] = False
    # make some space in the session cookie by clearing any project_metadata
    if session.get('project_metadata'):
        del session['project_metadata']
    if session.get('_flashes'):
        del session['_flashes']

    edit = False 
    template = False
    template_id = None

    if not dataset_id:
        dataset_id = request.form.get('dataset_id')
        template_id = request.args.get('template_id')

    if dataset_id and dataset_id != '':
        edit = True

    if template_id and template_id != '':
        template = True

    user_info = session.get('user_info')
    if user_info is None:
        session['next'] = request.path
        return redirect(url_for('login'))

    # if editing - check user has editing permissions on this dataset
    if edit and not check_user_permission(user_info, dataset_id):
        return redirect(url_for('invalid_user', dataset_id=dataset_id))

    if request.method == 'POST':
        if request.form.get('action') == "Previous Page":
            # coming from page 2
            page1 = {}
            page2 = request.form.to_dict()
            if 'page1' in page2 and page2['page1'] != "":
                page1 = eval(page2.pop('page1'))
        else:
            page1 = request.form.to_dict()
            page2 = {}
            if 'page2' in page1 and page1['page2'] != "":
                page2 = eval(page1.pop('page2'))

        page1 = groupPage1Fields(page1)

        if request.form.get('action') == "Previous Page":
            # arriving back from Page 2
            return render_template('dataset.html', name=user_info['name'], email="", error=error, success=success, 
                                   dataset_metadata=page1, page2=page2, nsf_grants=get_nsf_grants(['award', 'name', 'title'], only_inhabited=False), 
                                   projects=get_projects(), persons=get_persons(), locations=get_usap_locations(), edit=edit)

        elif request.form.get('action') == "save":
            # save to file
            if user_info.get('orcid'):
                save_file = os.path.join(app.config['SAVE_FOLDER'], user_info['orcid'] + ".json")
            elif user_info.get('sub'):
                save_file = os.path.join(app.config['SAVE_FOLDER'], user_info['sub'] + ".json")
            else:
                error = "Unable to save dataset."
            if save_file:
                try:
                    save_metadata = {'page1': page1, 'page2': page2}
                    with open(save_file, 'w') as file:
                        file.write(json.dumps(save_metadata, indent=4, sort_keys=True))
                    success = "Saved dataset form"
                except Exception as e:
                    error = "Unable to save dataset."
            return render_template('dataset.html', name=user_info['name'], email="", error=error, success=success, 
                                   dataset_metadata=page1, page2=page2, nsf_grants=get_nsf_grants(['award', 'name', 'title'], only_inhabited=False), 
                                   projects=get_projects(), persons=get_persons(), locations=get_usap_locations(), edit=edit)

        elif request.form.get('action') == "restore":
            # restore from file
            if user_info.get('orcid'):
                saved_file = os.path.join(app.config['SAVE_FOLDER'], user_info['orcid'] + ".json")
            elif user_info.get('sub'):
                saved_file = os.path.join(app.config['SAVE_FOLDER'], user_info['sub'] + ".json")
            else:
                error = "Unable to restore dataset"

            if saved_file:
                try:
                    with open(saved_file, 'r') as file:
                        data = json.load(file)
                        page1 = data.get('page1',{})
                        page2 = data.get('page2',{})
                        if page1.get('dataset_id'):
                             del page1['dataset_id']
                    success = "Restored dataset form"
                except Exception as e:
                    error = "Unable to restore dataset."
            else:
                error = "Unable to restore dataset."
            return render_template('dataset.html', name=user_info['name'], email="", error=error, success=success, 
                                   dataset_metadata=page1, page2=page2, nsf_grants=get_nsf_grants(['award', 'name', 'title'], 
                                   only_inhabited=False), projects=get_projects(), persons=get_persons(), locations=get_usap_locations(), edit=edit)

        if edit:
            return redirect('/edit/dataset2/' + dataset_id, code=307)

        return redirect(url_for('dataset2'), code=307)

    else:
        page1 = {}
        page2 = {}
        # EDIT dataset
        # get the dataset ID from the URL
        if edit:
            page1, page2 = dataset_db2form(dataset_id)
            email = page1.get('email')
            name = ""

        # Create new dataset using existing dataset as template
        elif template:
            page1, page2 = dataset_db2form(template_id)

            email = ""
            if user_info.get('email'):
                email = user_info.get('email')
            name = ""
            if user_info.get('name'):
                name = user_info.get('name')

            # remove dataset_id when creating new submission
            if page1.get('dataset_id'):
                del(page1['dataset_id'])
            if page2.get('dataset_id'):
                del(page2['dataset_id'])
        else:
            page2['license'] = 'CC_BY_4.0' #default value
            email = ""
            if user_info.get('email'):
                email = user_info.get('email')
            name = ""
            if user_info.get('name'):
                name = user_info.get('name')
                names = name.split(' ')
                page1['authors'] = [{'first_name': names[0], 'last_name': names[-1]}]
                page2['release_date'] = datetime.now().strftime('%Y-%m-%d')

        return render_template('dataset.html', name=name, email=email, error=error, success=success, 
                               dataset_metadata=page1, page2=page2,
                               nsf_grants=get_nsf_grants(['award', 'name', 'title'], only_inhabited=False), projects=get_projects(), 
                               persons=get_persons(), locations=get_usap_locations(), edit=edit, template=template)


def groupPage1Fields(page1):
    # collect publications, awards, authors, etc, and save as lists

    publications_keys = [s for s in list(page1.keys()) if "publication" in s and s != "publications"]
    if len(publications_keys) > 0:
        page1['publications'] = []
        publications_keys.sort(key=partial(sortNumerically, replace_str='publication'))
        for key in publications_keys:
            if page1[key] != "":
                pub_text = page1.get(key)
                pub_doi = page1.get(key.replace('publication', 'pub_doi'))
                publication = {'text': pub_text, 'doi': pub_doi}
                page1['publications'].append(publication)
            del page1[key]
            del page1[key.replace('publication', 'pub_doi')]

    awards_keys = [s for s in list(page1.keys()) if "award" in s and "user" not in s and s != "awards"]
    awards = []
    if len(awards_keys) > 0:
        awards_keys.sort(key=partial(sortNumerically, replace_str='award'))
        for key in awards_keys:
            if page1[key] != "" and page1[key] != "None":
                if page1[key] == 'Not In This List':
                    user_award_fld = 'user_' + key
                    award_name = "Not_In_This_List:" + page1.get(user_award_fld)
                    del page1[user_award_fld]
                else:
                    award_name = page1.get(key)
                awards.append(award_name)
            del page1[key]
        page1['awards'] = awards

    locations_keys = [s for s in list(page1.keys()) if "location" in s and "user" not in s and s != "locations"]
    locations = []
    if len(locations_keys) > 0:
        locations_keys.sort(key=partial(sortNumerically, replace_str='location'))
        for key in locations_keys:
            if page1[key] != "" and page1[key] != "None":
                if page1[key] == 'Not In This List':
                    user_loc_fld = 'user_' + key
                    location_name = "Not_In_This_List:" + page1.get(user_loc_fld)
                    del page1[user_loc_fld]
                else:
                    location_name = page1.get(key)
                locations.append(location_name)
            del page1[key]
        page1['locations'] = locations


    author_keys = [s for s in list(page1.keys()) if "author_name_last" in s and s != "authors"]
    if len(author_keys) > 0:
        page1['authors'] = []
        author_keys.sort(key=partial(sortNumerically, replace_str='author_name_last'))
        for key in author_keys:
            if page1[key] != "":
                last_name = page1.get(key)
                first_name = page1.get(key.replace('last', 'first'))
                author = {'first_name': first_name, 'last_name': last_name}
                page1['authors'].append(author) 
            del page1[key]
            del page1[key.replace('last', 'first')]
    
    return page1


# get dataset data from DB and convert to json that can be displayed in the Deposit/Edit Dataset page
def dataset_db2form(uid):
    db_data = get_datasets([uid])[0]
    if not db_data: 
        return {}
    page1 = {
        'dataset_id': uid,
        'abstract': db_data.get('abstract'),
        'name': db_data.get('submitter_id'),
        'title': db_data.get('title'),
        'submitter_name': db_data.get('submitter_id'),
        'locations': db_data.get('locations')
    }
    page2 = {
        'dataset_id': uid,
        'filenames': [],
        'release_date': db_data.get('release_date')
    }

    page1['authors'] = []
    main_author = None
    if db_data.get('creator'):
        for author in db_data.get('creator').split('; '):
            try:
                last_name, first_name = author.split(', ', 1)
            except:
                last_name, first_name = author.split(',', 1)

            if len(first_name) == 0: 
                first_name = ' '
            if not main_author:
                main_author = author
            page1['authors'].append({'first_name': first_name, 'last_name': last_name})

    page1['awards'] = []
    for award in db_data.get('awards'):
        page1['awards'].append(award.get('award') + ' ' + award.get('name'))

    if db_data.get('spatial_extents'):
        se = db_data.get('spatial_extents')[0]
        page1['cross_dateline'] = se.get('cross_dateline')
        page1['geo_e'] = str(se.get('east'))
        page1['geo_n'] = str(se.get('north'))
        page1['geo_s'] = str(se.get('south'))
        page1['geo_w'] = str(se.get('west'))

    if main_author:
        creator = get_person(main_author)
        if creator:
            page1['email'] = creator.get('email')

    page1['publications'] = []
    for ref in db_data.get('references'):
        page1['publications'].append({'doi': ref.get('doi'), 'text': ref.get('ref_text')})

    page1['project'] = None
    if db_data.get('projects') and len(db_data['projects']) > 0:
        page1['project'] = db_data['projects'][0].get('id')
    
    if db_data.get('temporal_extents') and len(db_data['temporal_extents']) > 0:
        page1['start'] = db_data['temporal_extents'][0].get('start_date')
        page1['stop'] = db_data['temporal_extents'][0].get('stop_date')

    keywords = cf.getDatasetKeywords(uid)
    page1['user_keywords'] = ''
    for kw in keywords:
        if kw.get('keyword_id')[0:2] == 'uk' and kw.get('keyword_label') not in page1['locations']:
            if page1['user_keywords'] != '':
                page1['user_keywords'] += ', '
            page1['user_keywords'] += kw.get('keyword_label')

    # read in more fields from the readme file and add to the page2 form_data
    page2.update(dataset_readme2form(uid))

    # read in remaining fields from previous submisison form and add to the page1 form_data
    page1.update(dataset_oldform2form(uid))

    # get uploaded files
    url = db_data.get('url')
    page2['uploaded_files'] = []
    if url:
        usap_domain = app.config['USAP_DOMAIN']
        if url.startswith(usap_domain):
            directory = os.path.join(current_app.root_path, url[len(usap_domain):])
            file_paths = [os.path.join(dp, f) for dp, dn, fn in os.walk(directory) for f in fn]
            omit = set(['readme.txt', '00README.txt', 'index.php', 'index.html', 'data.html'])
            file_paths = [f for f in file_paths if os.path.basename(f) not in omit]
            files = []
            for f_path in file_paths:
                f_size = os.stat(f_path).st_size
                f_name = os.path.basename(f_path)
                f_subpath = f_path[len(directory):]
                files.append({'url': os.path.join(url, f_subpath), 'name': f_name, 'size': humanize.naturalsize(f_size)})
                page2['filenames'].append(f_name)
            page2['uploaded_files'] = files

        else:
            page2['uploaded_files'] = [{'url': url, 'name': os.path.basename(os.path.normpath(url))}]

    page2['license'] = db_data['license']
    return page1, page2


def dataset_readme2form(uid):
    r = requests.get(url_for('readme', dataset_id=uid, _external=True))
    form_data = {}
    # check readme file is found and is plain text (not a pdf)
    if r.url != url_for('not_found', _external=True) and r.headers.get('Content-Type') and r.headers['Content-Type'].find('text/plain') == 0:
        text = r.text

        if 'Content and processing steps' in text:
            # old readme file format
            start = text.find('Instruments and devices:') + len('Instruments and devices:')
            end = text.find('Acquisition procedures:')
            form_data['devices'] = text[start:end].replace('\n', '')

            start = text.find('Acquisition procedures:') + len('Acquisition procedures:')
            end = text.find('Content and processing steps:')
            form_data['procedures'] = text[start:end].replace('\n', '')

            start = text.find('Content and processing steps:') + len('Content and processing steps:')
            end = text.find('Limitations and issues:')
            c_p = text[start:end].replace('\r', '').split('\n\n')
            form_data['content'] = c_p[0].replace('\n', '')
            if len(c_p) > 1:
                form_data['data_processing'] = c_p[1].replace('\n', '')
            else:
                form_data['data_processing'] = ''

            start = text.find('Limitations and issues:') + len('Limitations and issues:')
            end = text.find('Checkboxes:')
            form_data['issues'] = text[start:end].replace('\n', '')
        else:
            # new readme file format
            start = text.find('Instruments and devices:') + len('Instruments and devices:')
            end = text.find('Acquisition procedures:')
            form_data['devices'] = text[start:end].replace('\n', '').strip()

            start = text.find('Acquisition procedures:') + len('Acquisition procedures:')
            end = text.find('Description of data processing:')
            form_data['procedures'] = text[start:end].replace('\n', '').strip()

            start = text.find('Description of data processing:') + len('Description of data processing:')
            end = text.find('Description of data content:')
            form_data['data_processing'] = text[start:end].replace('\n', '').strip()

            start = text.find('Description of data content:') + len('Description of data content:')
            end = text.find('Limitations and issues:')
            form_data['content'] = text[start:end].replace('\n', '').strip()

            start = text.find('Limitations and issues:') + len('Limitations and issues:')
            end = text.find('Checkboxes:')
            form_data['issues'] = text[start:end].replace('\n', '').strip()

    return form_data


def dataset_oldform2form(uid):
    #get Related Field Event IDs and Region Feature Name from previous submission
    submitted_dir = os.path.normpath(os.path.join(current_app.root_path, app.config['SUBMITTED_FOLDER']))
    try:
        if not submitted_dir.startswith(current_app.root_path):
            raise Exception()
        # if there is an editted file, use that one, other wise use original
        if os.path.isfile(os.path.join(submitted_dir, "e" + uid + ".json")):
            submitted_file = os.path.normpath(os.path.join(submitted_dir, "e" + uid + ".json"))
        else:
            submitted_file = os.path.normpath(os.path.join(submitted_dir, uid + ".json"))
  
        if not submitted_file.startswith(current_app.root_path):
            raise Exception()
        with open(submitted_file) as infile:
            submitted_data = json.load(infile)
    except:
            submitted_data = {}

    form_data = {'related_fields': submitted_data.get('related_fields')}
    return form_data


@app.route('/submit/help', methods=['GET', 'POST'])
def submit_help():
    return render_template('submission_help.html')


@app.route('/submit/dataset/help', methods=['GET', 'POST'])
def submit_dataset_help():
    return render_template('submission_dataset_help.html')


@app.route('/submit/project/help', methods=['GET', 'POST'])
def submit_project_help():
    return render_template('submission_project_help.html')


@app.route('/amd')
def amd():
    return render_template('amd_help.html')


class ExceptionWithRedirect(Exception):
    def __init__(self, message, redirect):
        self.redirect = redirect
        super(ExceptionWithRedirect, self).__init__(message)
  

class BadSubmission(ExceptionWithRedirect):
    pass

        
class CaptchaException(ExceptionWithRedirect):
    pass


class InvalidDatasetException(ExceptionWithRedirect):
    def __init__(self, msg='Invalid Dataset#', redirect='/'):
        super(InvalidDatasetException, self).__init__(msg, redirect)


@app.errorhandler(CaptchaException)
def failed_captcha(e):
    return render_template('error.html', error_message=str(e))


@app.errorhandler(BadSubmission)
def invalid_dataset(e):
    session['error'] = True
    name = ""
    if session.get('user_info') and session['user_info'].get('name'):
        name = session['user_info']['name']
    return render_template('error.html', error_message=str(e), back_url=e.redirect, name=name)


@app.errorhandler(psycopg2.OperationalError)
def db_error(e):
    print(traceback.format_exc())
    msg = "Error connecting to database. Please <a href='mailto:%s'>contact us</a>." % app.config['USAP-DC_GMAIL_ACCT']
    # msg += traceback.format_exc()
    return render_template('error.html', error_message=msg)


#@app.errorhandler(OAuthException)
def oauth_error(e):
    return render_template('error.html', error_message=str(e))


@app.errorhandler(Exception)
def general_error(e):
    print(traceback.format_exc())
    msg = "Oops, there is an error on this page.  Please <a href='mailto:%s'>contact us</a>.<br>" % app.config['USAP-DC_GMAIL_ACCT']
    if cf.isCurator():
        msg += traceback.format_exc()
    return render_template('error.html', error_message=msg)


#@app.errorhandler(InvalidDatasetException)
def view_error(e):
    return render_template('error.html', error_message=str(e))


@app.route('/thank_you/<submission_type>')
def thank_you(submission_type):
    return render_template('thank_you.html', submission_type=submission_type)


@app.route('/invalid_user/<dataset_id>')
def invalid_user(dataset_id):
    return render_template('invalid_user.html', dataset_id=dataset_id)


@app.route('/invalid_project_user/<project_id>')
def invalid_project_user(project_id):
    return render_template('invalid_project_user.html', project_id=project_id)


@app.route('/data_on_hold/<release_date>')
def data_on_hold(release_date):
    return render_template('data_on_hold.html', release_date=release_date)


Validator = namedtuple('Validator', ['func', 'msg'])


def check_spatial_bounds(data):
    if not(data['geo_e'] or data['geo_w'] or data['geo_s'] or data['geo_n']):
        return True
    else:
        try:
            return \
                abs(float(data['geo_w'])) <= 180 and \
                abs(float(data['geo_e'])) <= 180 and \
                abs(float(data['geo_n'])) <= 90 and abs(float(data['geo_s'])) <= 90
        except:
            return False


def check_dataset_submission(msg_data):

    def default_func(field):
        return lambda data: field in data and bool(data[field]) and data[field] != "None"

    def check_valid_email(data):
        return re.match("[^@]+@[^@]+\.[^@]+", data['email'])

    def check_valid_author(data):
        if data.get('authors') is None:
            return False
        for author in data['authors']:
            if author.get('last_name') is None or author.get('last_name') == "":
                return False
            if author.get('first_name') is None or author.get('first_name') == "": 
                return False
        return True

    def check_valid_award(data):
        return data.get('awards') is not None and data['awards'] != [""]

    validators = [
        Validator(func=default_func('title'), msg='You must include a dataset title for the submission.'),
        Validator(func=check_valid_author, msg='You must include at least one dataset author (both first and last name) for the submission.' +
                  '  All authors must have a first and a last name.'),
        Validator(func=default_func('email'), msg='You must include a contact email address for the submission.'),
        Validator(func=check_valid_email, msg='You must a valid contact email address for the submission.'),
        Validator(func=check_valid_award, msg='You must select at least one NSF grant for the submission.'),
        Validator(func=check_spatial_bounds, msg="Spatial bounds are invalid."),
        Validator(func=default_func('agree'), msg='You must agree to have your files posted with a DOI.')
    ]

    if not msg_data.get('edit') or msg_data.get('edit') == 'False':
        validators.append(Validator(func=default_func('filenames'), msg='You must include files in your submission.'))
    
    msg = ""
    for v in validators:
        if not v.func(msg_data):
            msg += "<p>" + v.msg + "</p>"
    if len(msg) > 0:
        return msg
    return None


@app.route('/repo_list')
def repo_list():
    return render_template('repo_list.html')


@app.route('/not_found')
def not_found():
    return render_template('not_found.html')


@app.route('/maintenance')
def maintenance():
    return render_template('roadworks.html')


#test route for static site maintenance page that should be copied to /var/www/html
@app.route('/site_maintenance')
def site_maintenance():
    return redirect(url_for('static', filename='maintenance.html'))


def check_project_registration(msg_data):

    def default_func(field):
        return lambda data: field in data and bool(data[field])

    def check_valid_email(data):
        return re.match("[^@]+@[^@]+\.[^@]+", data['email'])

    validators = [
        # Validator(func=default_func('award'), msg="You must select an award #"),
        Validator(func=default_func("title"), msg="You must provide a title for the project"),
        Validator(func=default_func("pi_name_first"), msg="You must provide the PI's first name for the project"),
        Validator(func=default_func("pi_name_last"), msg="You must provide the PI's last name for the project"),
        # Validator(func=default_func('email'), msg='You must include a contact email address for the submission.'),
        Validator(func=check_valid_email, msg='You must a valid contact email address for the submission.'),
        Validator(func=default_func('start'), msg='You must include a start date for the project'),
        Validator(func=default_func('progress'), msg='You must include the progress of the project'),
        Validator(func=default_func('end'), msg='You must include an end date for the project'),
        Validator(func=default_func('sum'), msg='You must include an abstract for the project'),
        # Validator(func=lambda data: 'repos' in data and (len(data['repos']) > 0 or data['repos'] == 'nodata'), msg="You must provide info about the repository where you submitted the dataset"),
        Validator(func=lambda data: 'locations' in data and len(data['locations']) > 0, msg="You must provide at least one location term"),
        Validator(func=lambda data: 'parameters' in data and len(data['parameters']) > 0, msg="You must provide at least one keyword term")
    ]
    msg = ""
    for v in validators:
        if not v.func(msg_data):
            msg += "<p>" + v.msg
    if len(msg) > 0:
        raise BadSubmission(msg, '/submit/project')
    

def format_time():
    t = datetime.utcnow()
    s = t.strftime('%Y-%m-%dT%H:%M:%S.%f')
    return s[:-5] + 'Z'
   

@app.route('/edit/dataset2/<dataset_id>', methods=['GET', 'POST'])
@app.route('/submit/dataset2', methods=['GET', 'POST'])
def dataset2(dataset_id=None):
    error = ""
    success = ""
    edit = False 
    session['error'] = False
    if not dataset_id:
        dataset_id = request.form.get('dataset_id') 
    if dataset_id and dataset_id != '':
        edit = True

    user_info = session.get('user_info')
    if user_info is None:
        session['next'] = request.path
        return redirect(url_for('login'))

    # if editing - check user has editing permissions on this dataset
    if edit and not check_user_permission(user_info, dataset_id):
            return redirect(url_for('invalid_user', dataset_id=dataset_id))

    if request.method == 'POST':
        if request.form.get('action') == "next":
            # coming from page 1
            page2 = {}
            page1 = request.form.to_dict()
            if 'page2' in page1 and page1['page2'] != "":
                page2 = eval(page1.pop('page2'))
        else:
            page2 = request.form.to_dict()
            page1 = {}
            if 'page1' in page2 and page2['page1'] != "":
                page1 = eval(page2.pop('page1'))

        page1 = groupPage1Fields(page1)

        if request.form.get('action') == 'Submit':
            msg_data = copy.copy(page1)
            msg_data.update(page2)

            msg_data['submitter_name'] = session['user_info'].get('name')
            del msg_data['action']

            cross_dateline = False
            if page1.get('cross_dateline') == 'on':
                cross_dateline = True
            msg_data['cross_dateline'] = cross_dateline

            msg_data['agree'] = 'agree' in page2 
            msg_data['properGeoreferences'] = 'properGeoreferences' in page2
            msg_data['propertiesExplained'] = 'propertiesExplained' in page2
            msg_data['comprehensiveLegends'] = 'comprehensiveLegends' in page2
            msg_data['dataUnits'] = 'dataUnits' in page2

            if 'orcid' in session['user_info']:
                msg_data['submitter_orcid'] = session['user_info']['orcid']
            if 'email' in session['user_info']:
                msg_data['submitter_email'] = session['user_info']['email']

            msg_data['filenames'] = []

            if edit:
                # get all the names of any files previously uploaded
                file_keys = [s for s in list(request.form.keys()) if "uploaded_file_" in s]
                for f in file_keys:
                    msg_data['filenames'].append(f.replace('uploaded_file_', ''))
                    del(msg_data[f])

            if msg_data.get('uploaded_files'):
                del(msg_data['uploaded_files'])   

            # get filenames of any files uploaded with this submission
            files = request.files.getlist('file[]')
            fnames = dict()
            for f in files:
                fname = secure_filename(f.filename)
                if len(fname) > 0:
                    fnames[fname] = f

            msg_data['filenames'] += list(fnames.keys())

            # if files have been added or deleted during an edit, we will create a new dataset
            if edit and (len(fnames) > 0 or msg_data.get('file_deleted') == 'true'):
                msg_data['related_dataset'] = dataset_id
                msg_data['edit'] = False
                edit = False

            timestamp = format_time()
            msg_data['timestamp'] = timestamp
            error = check_dataset_submission(msg_data)
            if error:
                return render_template('dataset2.html', error=error, success=success, 
                                    dataset_metadata=page2, page1=page1, licenses=get_licenses(), edit=edit)

            # nsfid = 'NSF' + msg_data['awards'][0].split(' ')[0]
            upload_dir = os.path.join(current_app.root_path, app.config['UPLOAD_FOLDER'], timestamp)
            msg_data['upload_directory'] = upload_dir
            if not os.path.exists(upload_dir):
                os.makedirs(upload_dir)

            for fname, fobj in list(fnames.items()):
                fobj.save(os.path.join(upload_dir, fname))
          
            # save json file in submitted dir
            try:
                submitted_dir = os.path.join(current_app.root_path, app.config['SUBMITTED_FOLDER'])
                if edit:
                    submitted_file = os.path.normpath(os.path.join(submitted_dir, "e" + dataset_id + ".json"))
                else:
                    # get next_id
                    next_id = getNextDOIRef()
                    updateNextDOIRef()
                    submitted_file = os.path.normpath(os.path.join(submitted_dir, next_id + ".json"))
                if not submitted_file.startswith(current_app.root_path):
                    raise Exception()
                with open(submitted_file, 'w') as file:
                    file.write(json.dumps(msg_data, indent=4, sort_keys=True))
                os.chmod(submitted_file, 0o664)

            except Exception as e:
                error = "Unable to submit dataset. Please contact info@usap-dc.org"
                return render_template('dataset2.html', error=error, success=success, 
                                dataset_metadata=page2, page1=page1, licenses=get_licenses(), edit=edit)
          
            # email curators
            if msg_data.get('submitter_email'):
                submitter = msg_data.get('submitter_email')
            else:
                submitter = msg_data.get('email')
            if msg_data.get('submitter_name'):
                submitter = "%s <%s>" % (msg_data['submitter_name'], submitter)

            if edit:
                message = "Dataset Edit.<br><br>Dataset JSON: <a href='%scurator?uid=e%s'>%scurator?uid=e%s</a><br>" \
                    % (request.url_root, dataset_id, request.url_root, dataset_id)
            else:
                message = "New dataset submission.<br><br>Dataset JSON: <a href='%scurator?uid=%s'>%scurator?uid=%s</a><br>" \
                    % (request.url_root, next_id, request.url_root, next_id)
            message += "<br>Submitter: <a href='mailto:%s'>%s</a><br>" % (msg_data['submitter_name'], submitter)
            msg = MIMEMultipart('alternative')

            recipients = parse_email_list([app.config['USAP-DC_GMAIL_ACCT']])
            sender = app.config['USAP-DC_GMAIL_ACCT']

            if edit:
                msg['Subject'] = 'USAP-DC Dataset Edit [uid:%s]' % dataset_id
            else: 
                msg['Subject'] = 'USAP-DC Dataset Submission [uid:%s]' % next_id
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
            
            
            # Send autoreply to user
            send_autoreply(submitter, msg['Subject'])

            # add to submission table
            conn, cur = connect_to_db()
            if edit:
                submission_type = 'dataset edit'
                uid = 'e' + dataset_id
                query = "SELECT * FROM submission WHERE uid = '%s'" % uid
                cur.execute(query)
                res = cur.fetchone()
                if res:
                    query = "DELETE FROM submission WHERE uid = '%s'" % uid
                    cur.execute(query)
            else:
                submission_type = 'dataset submission'
                uid = next_id
            query = "INSERT INTO submission (uid, submission_type, status, submitted_date, last_update) VALUES ('%s', '%s', 'Pending', '%s', '%s')" \
                    % (uid, submission_type, timestamp, timestamp[0:10])

            cur.execute(query)
            cur.execute('COMMIT')    

            return redirect('/thank_you/dataset')
        elif request.form['action'] == 'Previous Page':
            # post the form back to dataset
            if edit:
                return redirect('/edit/dataset/' + dataset_id, code=307)
            return redirect('/submit/dataset', code=307)
        elif request.form.get('action') == "save":
            # save to file
            if user_info.get('orcid'):
                save_file = os.path.join(app.config['SAVE_FOLDER'], user_info['orcid'] + ".json")
            elif user_info.get('sub'):
                save_file = os.path.join(app.config['SAVE_FOLDER'], user_info['sub'] + ".json")
            else:
                error = "Unable to save dataset."
            if save_file:
                try:
                    save_metadata = {'page1': page1, 'page2': page2}
                    with open(save_file, 'w') as file:
                        file.write(json.dumps(save_metadata, indent=4, sort_keys=True))
                    success = "Saved dataset form"
                except Exception as e:
                    error = "Unable to save dataset."
            return render_template('dataset2.html', error=error, success=success, 
                                    dataset_metadata=page2, page1=page1, licenses=get_licenses(), edit=edit)

        elif request.form.get('action') == "restore":
            # restore from file
            if user_info.get('orcid'):
                saved_file = os.path.join(app.config['SAVE_FOLDER'], user_info['orcid'] + ".json")
            elif user_info.get('sub'):
                saved_file = os.path.join(app.config['SAVE_FOLDER'], user_info['sub'] + ".json")
            else:
                error = "Unable to restore dataset"
            if saved_file:
                try:
                    with open(saved_file, 'r') as file:
                        data = json.load(file)
                        page1 = data.get('page1',{})
                        page2 = data.get('page2',{})
                        if page1.get('dataset_id'):
                             del page1['dataset_id']

                    success = "Restored dataset form"
                except Exception as e:
                    error = "Unable to restore dataset."
            else:
                error = "Unable to restore dataset."
            return render_template('dataset2.html', error=error, success=success, 
                                    dataset_metadata=page2, page1=page1, licenses=get_licenses(), edit=edit)

        else:
            return render_template('dataset2.html', dataset_metadata=page2, page1=page1,
                                licenses=get_licenses(), edit=edit)       
    else:
        # if accessing directly through GET - return empty form
        return render_template('dataset2.html', dataset_metadata={}, page1={}, 
                               licenses=get_licenses(), edit=edit)


# Read the next doi reference number from the file
def getNextDOIRef():
    return open(app.config['DOI_REF_FILE'], 'r').readline().strip()


# increment the next email reference number in the file
def updateNextDOIRef():
    newRef = int(getNextDOIRef()) + 1
    with open(app.config['DOI_REF_FILE'], 'w') as refFile:
        refFile.write(str(newRef))


# Read the next project reference number from the file
def getNextProjectRef():
    ref = open(app.config['PROJECT_REF_FILE'], 'r').readline().strip()
    return 'p%0*d' % (7, int(ref))


# increment the next project reference number in the file
def updateNextProjectRef():
    newRef = int(getNextProjectRef().replace('p', '')) + 1
    with open(app.config['PROJECT_REF_FILE'], 'w') as refFile:
        refFile.write(str(newRef))


@app.route('/edit/project/<project_id>', methods=['GET', 'POST'])
@app.route('/submit/project', methods=['GET', 'POST'])
def project(project_id=None):
    edit = False
    template = False 
    template_id = None
    success = ""
    error = ""
    if not project_id:
        project_id = request.form.get('project_id')
        template_id = request.args.get('template_id')

    if project_id and project_id != '':
        edit = True

    if template_id and template_id != '':
        template = True

    user_info = session.get('user_info')
    if user_info is None:
        session['next'] = request.path
        return redirect(url_for('login'))

    # if editing - check user has editing permissions on this dataset
    if edit and not check_user_permission(user_info, project_id, project=True):
            return redirect(url_for('invalid_project_user', project_id=project_id))

    if request.method == 'POST':
        if request.form.get('action') == 'Submit':
            msg_data = process_form_data(request.form.to_dict())

            session['project_metadata'] = msg_data
            # save first
            success, error, full_name = save_project(msg_data)

            timestamp = format_time()
            msg_data['timestamp'] = timestamp

            msg = MIMEText(json.dumps(msg_data, indent=4, sort_keys=True))
            check_project_registration(msg_data)

            # get the data management plan and save it in the upload folder
            dmp = request.files.get('dmp')
            if dmp:
                dmp_fname = secure_filename(dmp.filename)
                msg_data['dmp_file'] = dmp_fname

                upload_dir = os.path.join(current_app.root_path, app.config['UPLOAD_FOLDER'], timestamp)
                msg_data['upload_directory'] = upload_dir
                if not os.path.exists(upload_dir):
                    os.makedirs(upload_dir)

                dmp.save(os.path.join(upload_dir, dmp_fname))
            elif msg_data.get('current_dmp'):
                msg_data['dmp_file'] = msg_data['current_dmp']
                del msg_data['current_dmp']

            # save json file in submitted dir
            try:
                session['project_metadata'] = {}
                submitted_dir = os.path.join(current_app.root_path, app.config['SUBMITTED_FOLDER'])
                if edit:
                    submitted_file = os.path.normpath(os.path.join(submitted_dir, "e" + project_id + ".json"))
                else:
                    # get next_id for project
                    next_id = getNextProjectRef()
                    updateNextProjectRef()
                    submitted_file = os.path.normpath(os.path.join(submitted_dir, next_id + ".json"))
                if not submitted_file.startswith(current_app.root_path):
                    raise Exception()
                with open(submitted_file, 'w') as file:
                    file.write(json.dumps(msg_data, indent=4, sort_keys=True))
                os.chmod(submitted_file, 0o664)
            except Exception as e:
                print(e)
                raise BadSubmission('Unable to submit Project. Please contact info@usap-dc.org', '/submit/project')

            # email curators
            # use submitter's email if available, otherwise the email given in the form
            submitter = msg_data.get('submitter_email')
            if not submitter: 
                submitter = msg_data.get('email')
            if msg_data.get('submitter_name'):
                submitter = "%s <%s>" % (msg_data['submitter_name'], submitter)

            if edit:
                message = "Project Edit.<br><br>Project JSON: <a href='%scurator?uid=e%s'>%scurator?uid=e%s</a><br>" \
                    % (request.url_root, project_id, request.url_root, project_id)
            else:
                message = "New project submission.<br><br>Project JSON: <a href='%scurator?uid=%s'>%scurator?uid=%s</a><br>" \
                    % (request.url_root, next_id, request.url_root, next_id)
            message += "<br>Submitter: <a href='mailto:%s'>%s</a><br>" % (msg_data['submitter_name'], submitter)
            msg = MIMEMultipart('alternative')

            recipients = parse_email_list([app.config['USAP-DC_GMAIL_ACCT']])
            sender = app.config['USAP-DC_GMAIL_ACCT']

            if edit:
                msg['Subject'] = 'USAP-DC Project Edit [uid:%s]' % project_id
            else: 
                msg['Subject'] = 'USAP-DC Project Submission [uid:%s]' % next_id
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

            # Send autoreply to user
            send_autoreply(submitter, msg['Subject'])


            # add to submission table
            conn, cur = connect_to_db()
            if edit:
                submission_type = 'project edit'
                uid = 'e' + project_id
                query = "SELECT * FROM submission WHERE uid = '%s'" % uid
                cur.execute(query)
                res = cur.fetchone()
                if res:
                    query = "DELETE FROM submission WHERE uid = '%s'" % uid
                    cur.execute(query)
            else:
                submission_type = 'project submission'
                uid = next_id
            query = "INSERT INTO submission (uid, submission_type, status, submitted_date, last_update) VALUES ('%s', '%s', 'Pending', '%s', '%s')" \
                    % (uid, submission_type, timestamp, timestamp[0:10])
            
            cur.execute(query)
            cur.execute('COMMIT')   
            
            return redirect('/thank_you/project')

        elif request.form.get('action') == "save":

            project_metadata = process_form_data(request.form.to_dict())
            success, error, full_name = save_project(project_metadata)
            
            return render_template('project.html', name=user_info['name'], full_name=full_name, email=project_metadata['email'], programs=get_projects(), persons=get_persons(),
                                   nsf_grants=get_nsf_grants(['award', 'name', 'title'], only_inhabited=False), deployment_types=get_deployment_types(),
                                   locations=get_usap_locations(), parameters=get_parameters(), orgs=get_orgs(), roles=get_roles(), platforms=get_gcmd_platforms(),
                                   instruments=get_gcmd_instruments(), paleo_time=get_gcmd_paleo_time(), progresses=get_gcmd_progress(), product_levels=get_product_levels(),
                                   data_types=get_gcmd_data_types(), formats=get_gcmd_data_formats(), project_metadata=project_metadata, edit=edit, error=error, success=success)

        elif request.form.get('action') == "restore":
            # restore from file
            if user_info.get('orcid'):
                saved_file = os.path.join(app.config['SAVE_FOLDER'], user_info['orcid'] + "_p.json")
            elif user_info.get('sub'):
                saved_file = os.path.join(app.config['SAVE_FOLDER'], user_info['sub'] + "_p.json")
            else:
                error = "Unable to restore dataset"
            if saved_file:
                try:
                    with open(saved_file, 'r') as file:
                        project_metadata = json.load(file)

                    full_name = {'first_name': '', 'last_name': ''}
                    if project_metadata.get('pi_name_first') and project_metadata.get('pi_name_last'):
                        full_name = {'first_name': project_metadata['pi_name_first'], 'last_name': project_metadata['pi_name_last']}
                    success = "Restored project form"
                except Exception as e:
                    error = "Unable to restore dataset."
            else:
                error = "Unable to restore dataset."
            return render_template('project.html', name=user_info['name'], full_name=full_name, email=project_metadata['email'], programs=get_projects(), persons=get_persons(),
                                   nsf_grants=get_nsf_grants(['award', 'name', 'title'], only_inhabited=False), deployment_types=get_deployment_types(),
                                   locations=get_usap_locations(), parameters=get_parameters(), orgs=get_orgs(), roles=get_roles(), platforms=get_gcmd_platforms(),
                                   instruments=get_gcmd_instruments(), paleo_time=get_gcmd_paleo_time(), progresses=get_gcmd_progress(), product_levels=get_product_levels(),
                                   data_types=get_gcmd_data_types(), formats=get_gcmd_data_formats(), project_metadata=project_metadata, edit=edit, error=error, success=success)

    else:  
        # if returning after an unsuccessful submission, repopulate form with the existing metadata
        if session.get('error') and session.get('project_metadata'):
            form_data = session['project_metadata']
            session['error'] = False
        else:
            session['project_metadata'] = {}
            form_data = {}
        
            if edit:
                # Load up existing project data for EDIT
                form_data = project_db2form(project_id)
                session['project_metadata'] = form_data
            if template:
                # Load up existing project as template for new submission
                form_data = project_db2form(template_id)
                # remove dmp_link if creating new submission
                if form_data.get('dmp_file'):
                    del(form_data['dmp_file'])
                # remove project_id when creating new submission
                if form_data.get('project_id'):
                    del(form_data['project_id'])
                session['project_metadata'] = form_data

        email = ""
        if user_info and user_info.get('email'):
            email = user_info.get('email')

        name = ""
        full_name = {'first_name': '', 'last_name': ''}
        if user_info and user_info.get('name'):
            name = user_info.get('name')
            names = name.split(' ')
            full_name = {'first_name': names[0], 'last_name': names[-1]}

        if form_data.get('pi_name_first') and form_data.get('pi_name_last'):
            full_name = {'first_name': form_data['pi_name_first'], 'last_name': form_data['pi_name_last']}

        if form_data.get('email'):
            email = form_data['email']

        return render_template('project.html', name=user_info['name'], full_name=full_name, email=email, programs=get_projects(), persons=get_persons(),
                               nsf_grants=get_nsf_grants(['award', 'name', 'title'], only_inhabited=False), deployment_types=get_deployment_types(),
                               locations=get_usap_locations(), parameters=get_parameters(), orgs=get_orgs(), roles=get_roles(), platforms=get_gcmd_platforms(),
                               instruments=get_gcmd_instruments(), paleo_time=get_gcmd_paleo_time(), progresses=get_gcmd_progress(), product_levels=get_product_levels(),
                               data_types=get_gcmd_data_types(), formats=get_gcmd_data_formats(), project_metadata=session.get('project_metadata'), edit=edit)


def send_autoreply(recipient, subject):
    sender = app.config['USAP-DC_GMAIL_ACCT']
    subject = "AutoReply: " + subject
    message_text = """This is an automated confirmation that we have received your submission, edit, or message.  We will respond shortly.  Thank you."""

    msg_raw = create_gmail_message(sender, [recipient], subject, message_text)

    service, error = connect_to_gmail()
    if error:
        print('ERROR sending autoreply' + error)
    else:
        send_gmail(service, 'me', msg_raw)


def save_project(project_metadata):
    success = ""
    error = ""
    user_info = session.get('user_info')

    full_name = {'first_name': '', 'last_name': ''}
    if project_metadata.get('pi_name_first') and project_metadata.get('pi_name_last'):
        full_name = {'first_name': project_metadata['pi_name_first'], 'last_name': project_metadata['pi_name_last']}

    # save to file
    if user_info.get('orcid'):
        save_file = os.path.join(app.config['SAVE_FOLDER'], user_info['orcid'] + "_p.json")
    elif user_info.get('sub'):
        save_file = os.path.join(app.config['SAVE_FOLDER'], user_info['sub'] + "_p.json")
    else:
        error = "Unable to save project."
    if save_file:
        try:
            with open(save_file, 'w') as file:
                file.write(json.dumps(project_metadata, indent=4, sort_keys=True))
            success = "Saved project form"
        except Exception as e:
            error = "Unable to save project."
    return success, error, full_name


def process_form_data(form):
    msg_data = dict()
    msg_data['submitter_name'] = session['user_info']['name']
    if 'orcid' in session['user_info']:
        msg_data['submitter_orcid'] = session['user_info']['orcid']
    if 'email' in session['user_info']:
        msg_data['submitter_email'] = session['user_info']['email']
    msg_data.update(form)
    if msg_data.get('entry'):
        del msg_data['entry']
    if msg_data.get('entire_region') is not None:
        del msg_data['entire_region']

    # handle user added awards
    if msg_data['award'] == 'Not In This List':
        msg_data['award'] = "Not_In_This_List:" + msg_data.get('user_award')
        del msg_data['user_award']

    # handle multiple parameters, awards, co-authors, etc
    parameters = []
    idx = 1
    key = 'parameter1'
    while key in msg_data:
        if msg_data[key] != "":
            parameters.append(msg_data[key])
        del msg_data[key]
        idx += 1
        key = 'parameter' + str(idx)
    msg_data['parameters'] = parameters

    other_awards = []
    idx = 1
    key = 'award1'
    while key in msg_data:
        user_award_key = 'user_award' + str(idx)
        is_previous_award = msg_data.get('previous_award' + str(idx)) == 'on'
        if msg_data[key] != "":
            if msg_data[key] == 'Not In This List':
                msg_data[key] = "Not_In_This_List:" + msg_data[user_award_key]
            other_awards.append({'id': msg_data[key], 'is_previous_award': is_previous_award})
        del msg_data[key]
        del msg_data[user_award_key]
        idx += 1
        key = 'award' + str(idx)
   
    msg_data['other_awards'] = other_awards

    copis = []
    idx = 0
    key = 'copi_name_last'
    while key in msg_data:
        if msg_data[key] != "":
            copi = {'name_last': msg_data[key],
                    'name_first': msg_data.get(key.replace('last', 'first')),
                    'role': msg_data.get(key.replace('name_last', 'role')),
                    'org': msg_data.get(key.replace('name_last', 'org'))
                    }
            copis.append(copi)
        del msg_data[key]
        if msg_data.get(key.replace('last', 'first')): 
            del msg_data[key.replace('last', 'first')]
        if msg_data.get(key.replace('name_last', 'role')): 
            del msg_data[key.replace('name_last', 'role')] 
        if msg_data.get(key.replace('name_last', 'org')): 
            del msg_data[key.replace('name_last', 'org')]
        idx += 1
        key = 'copi_name_last' + str(idx)
    msg_data['copis'] = copis

    if msg_data.get('program'):
        msg_data['program'] = json.loads(msg_data['program'].replace("\'", "\""))

    websites = []
    idx = 0
    key = 'website_url'
    while key in msg_data:
        if msg_data[key] != "":
            website = {'url': msg_data[key], 'title': msg_data.get(key.replace('url', 'title'))}
            websites.append(website)
        del msg_data[key]
        if msg_data.get(key.replace('url', 'title')): 
            del msg_data[key.replace('url', 'title')]
        idx += 1
        key = 'website_url' + str(idx)
    msg_data['websites'] = websites

    deployments = []
    idx = 0
    key = 'deployment_name'
    while key in msg_data:
        if msg_data[key] != "":
            depl = {'name': msg_data[key], 'type': msg_data.get(key.replace('name', 'type')), 'url': msg_data.get(key.replace('name', 'url'))}
            deployments.append(depl)
        del msg_data[key]
        if msg_data.get(key.replace('name', 'type')):
            del msg_data[key.replace('name', 'type')]
        if msg_data.get(key.replace('name', 'url')):
            del msg_data[key.replace('name', 'url')] 
        idx += 1
        key = 'deployment_name' + str(idx)
    msg_data['deployments'] = deployments

    publications = []
    idx = 0
    key = 'publication'
    while key in msg_data:
        if msg_data[key] != "":
            pub = {'name': msg_data[key], 'doi': msg_data.get(key.replace('publication', 'pub_doi'))}
            publications.append(pub)
        del msg_data[key]
        if msg_data.get(key.replace('publication', 'pub_doi')): 
            del msg_data[key.replace('publication', 'pub_doi')]
        idx += 1
        key = 'publication' + str(idx)
    msg_data['publications'] = publications

    locations_keys = [s for s in list(request.form.keys()) if "location" in s and "user" not in s]
    locations = []
    if len(locations_keys) > 0:
        locations_keys.sort(key=partial(sortNumerically, replace_str='location'))
        for key in locations_keys:
            if msg_data[key] != "" and msg_data[key] != "None":
                if msg_data[key] == 'Not In This List':
                    user_loc_fld = 'user_' + key
                    location_name = "Not_In_This_List:" + msg_data.get(user_loc_fld)
                    del msg_data[user_loc_fld]
                else:
                    location_name = msg_data.get(key)
                locations.append(location_name)
            del msg_data[key]
    msg_data['locations'] = locations

    platforms = []
    del_keys = []
    for key in msg_data.keys():
        if 'plat' in key:
            plat_id = key.split('_')[-1]
            platform = {'id': msg_data[key], 'instruments': []}
            for key2 in msg_data.keys():
                if 'instr_'+plat_id in key2:
                    platform['instruments'].append(msg_data[key2])
                    del_keys.append(key2)
            platforms.append(platform)
            del_keys.append(key)

    msg_data['platforms'] = platforms
    for key in del_keys:
        del msg_data[key]

    paleo_times = []
    del_keys = []
    for key in msg_data.keys():
        if 'paleo_time_' in key:
            paleo_id = key.split('_')[-1]
            paleo_time = {'id': msg_data[key]}
            for key2 in msg_data.keys():
                if 'paleo_start_date_'+paleo_id in key2:
                    paleo_time['start_date'] = msg_data[key2]
                    del_keys.append(key2)
                if 'paleo_stop_date_'+paleo_id in key2:
                    paleo_time['stop_date'] = msg_data[key2]
                    del_keys.append(key2)
            paleo_times.append(paleo_time)
            del_keys.append(key)

    msg_data['paleo_times'] = paleo_times
    for key in del_keys:
        del msg_data[key]

    datasets = []
    idx = 0
    key = 'ds_repo'
    while key in msg_data:
        if msg_data[key] != "":
            del_keys = []
            repo = {'repository': msg_data[key],
                    'title': msg_data.get(key.replace('repo', 'title')),
                    'url': msg_data.get(key.replace('repo', 'url')),
                    'doi': msg_data.get(key.replace('repo', 'doi')), 
                    'formats': []}
            format_key = key.replace('repo', 'format')
            
            for key2 in msg_data.keys():
                if format_key == key2.split('-')[0]:
                    repo['formats'].append(msg_data[key2])
                    del_keys.append(key2)



            datasets.append(repo)
        del msg_data[key] 
        if msg_data.get(key.replace('repo', 'title')):
            del msg_data[key.replace('repo', 'title')]
        if msg_data.get(key.replace('repo', 'url')):
            del msg_data[key.replace('repo', 'url')]
        if msg_data.get(key.replace('repo', 'doi')):
            del msg_data[key.replace('repo', 'doi')]
        for key3 in del_keys:
            if key3 in msg_data:
                del msg_data[key3]
        idx += 1
        key = 'ds_repo' + str(idx)
    msg_data['datasets'] = datasets

    cross_dateline = False
    if msg_data.get('cross_dateline') == 'on':
        cross_dateline = True
    msg_data['cross_dateline'] = cross_dateline

    return msg_data


# get project data from DB and convert to json that can be displayed in the Register/Edit Project page
def project_db2form(uid):
    db_data = get_project(uid)

    if not db_data: 
        return {}
    form_data = {
        'project_id': uid,
        'sum': db_data.get('description'),
        'title': db_data.get('title'),
        'short_title': db_data.get('short_name'),
        'start': str(db_data.get('start_date')),
        'end': str(db_data.get('end_date')),
        'progress': db_data.get('project_progress'),
        'product_level': db_data.get('product_level_id'),
        'data_type': db_data.get('collection_data_type'),
        'websites': [],
        'copis': [],
        'deployments': [],
        'locations': [],
        'parameters': [],
        'publications': [],
        'datasets': [],
        'platforms': [],
        'paleo_times': [],
        'program': None,
        'dmp_file': None,
        'user_keywords': ''
    }

    main_contact = None
    for person in db_data.get('persons'):
        if person.get('role') == 'Investigator and contact' and not main_contact:
            name_last, name_first = person.get('id').split(', ', 1)
            form_data['pi_name_first'] = name_first
            form_data['pi_name_last'] = name_last
            form_data['org'] = person.get('org') or ''
            form_data['email'] = person.get('email') or ''
            main_contact = person
        else:
            name_last, name_first = person.get('id').split(', ', 1)
            form_data['copis'].append({'name_first': name_first,
                                       'name_last': name_last,
                                       'org': person.get('org') or '',
                                       'role': person['role']})
    # if nobody is listed as 'Investigator and contact', take the first 'Investigator'
    if not main_contact:
        for person in db_data.get('persons'):
            if person.get('role') == 'Investigator':
                name_last, name_first = person.get('id').split(', ', 1)
                form_data['pi_name_first'] = name_first
                form_data['pi_name_last'] = name_last
                form_data['org'] = person.get('org') or ''
                form_data['email'] = person.get('email') or ''
                main_contact = person
                # remove from copis list
                form_data['copis'] = [copi for copi in form_data['copis'] if not (copi['name_first'] == name_first and copi['name_last'] == name_last)]
                break

    awards = []
    for award in db_data.get('funding'):
        if award.get('is_main_award'):
            form_data['award'] = award.get('award') + ' ' + award.get('pi_name')
            if award.get('dmp_link'):
                form_data['dmp_file'] = award.get('dmp_link').split('/')[-1]
        else:
            other_award = {'id': award.get('award') + ' ' + award.get('pi_name'), 'is_previous_award': award.get('is_previous_award')}
            if other_award not in awards:
                awards.append(other_award)
    form_data['other_awards'] = awards

    if db_data.get('initiatives'):
        i = db_data['initiatives'][0]
        form_data['program'] = {'id': i.get('id')}
    
    if db_data.get('website'):
        for w in db_data['website']:
                form_data['websites'].append({'title': w.get('title'), 'url': w.get('url')})

    if db_data.get('deployment'): 
        for d in db_data.get('deployment'):
            form_data['deployments'].append({'name': d.get('deployment_id'), 'type': d.get('deployment_type'), 'url': d.get('url')})

    if db_data.get('locations'):
        for l in db_data.get('locations'):
            form_data['locations'].append(l)

    if db_data.get('parameters'):
        for p in db_data.get('parameters'):
            form_data['parameters'].append(p.get('id'))

    if db_data.get('spatial_bounds'):
        se = db_data.get('spatial_bounds')[0]
        form_data['cross_dateline'] = se.get('cross_dateline')
        form_data['geo_e'] = str(se.get('east'))
        form_data['geo_n'] = str(se.get('north'))
        form_data['geo_s'] = str(se.get('south'))
        form_data['geo_w'] = str(se.get('west'))
   
    if db_data.get('reference_list'):
        for ref in db_data['reference_list']:
            form_data['publications'].append({'doi': ref.get('doi'), 'name': ref.get('ref_text')})

    if db_data.get('datasets'):
        for d in db_data['datasets']:
            formats = None
            if d.get('data_format'):
                formats = d['data_format'].split('; ')
            form_data['datasets'].append({'repository': d.get('repository'),
                                          'title': d.get('title'),
                                          'url': d.get('url'),
                                          'doi': d.get('doi'),
                                          'formats': formats})

    if db_data.get('gcmd_platforms'):
        for p in db_data['gcmd_platforms']:
            instruments = []
            if p.get('gcmd_instruments'):
                for i in p['gcmd_instruments']:
                    instruments.append(i['id'])
            form_data['platforms'].append({'id': p['id'],
                                           'instruments': instruments})

    if db_data.get('gcmd_paleo_time'):
        for pt in db_data['gcmd_paleo_time']:
            form_data['paleo_times'].append({'id': pt['paleo_time']['id'],
                                           'start_date': pt['paleo_start_date'],
                                           'stop_date': pt['paleo_stop_date']})

    if db_data.get('aux_keywords'):
        for kw in db_data['aux_keywords'].split('; '):
            if kw not in form_data['locations']:
                print(kw)
                if form_data['user_keywords'] != '':
                    form_data['user_keywords'] += ', '
                form_data['user_keywords'] += kw

    return form_data


@app.route('/submit/projectinfo', methods=['GET'])
def projectinfo():
    award_id = request.args.get('award')
    if award_id and award_id != 'Not_In_This_List':       
        (conn, cur) = connect_to_db()
        query_string = """SELECT a.*, pam.proj_uid FROM award a 
            LEFT JOIN project_award_map pam ON pam.award_id = a.award        
            WHERE a.award = '%s'""" % award_id
        cur.execute(query_string)
        return flask.jsonify(cur.fetchall()[0])
    return flask.jsonify({})


@app.route('/login')
def login():
    if request.referrer == url_for('stats', _external=True):
        session['next'] = '/stats'
    if session.get('next') is None:
        session['next'] = '/home'
    return render_template('login.html')


@app.route('/login_google')
def login_google():
    redirect_uri = url_for('authorized', _external=True)
    return google.authorize_redirect(redirect_uri)


@app.route('/login_orcid')
def login_orcid():
    redirect_uri = url_for('authorized_orcid', _external=True)
    return orcid.authorize_redirect(redirect_uri)


@app.route('/authorized')
def authorized():
    token = google.authorize_access_token()
    session['google_access_token'] = token, ''
    user = token.get('userinfo')  
    session['user_info'] = user
    if user.get('name') is None:
        session['user_info']['name'] = ""

    session['user_info']['is_curator'] = cf.isCurator() 
    # return flask.jsonify(session['user_info'])  
    return redirect(session['next'])


@app.route('/authorized_orcid')
def authorized_orcid():
    token = orcid.authorize_access_token()
    session['orcid_access_token'] = token
    user = token.get('userinfo')  
    session['user_info'] = {
        'name': '',
        'orcid': user.get('sub')
    }
    if user.get('given_name') and user.get('family_name'):
        session['user_info']['name'] =  '%s %s' % (user.get('given_name'), user.get('family_name'))

    res = requests.get('https://pub.orcid.org/v2.1/' + user['sub'] + '/email',
                       headers={'accept': 'application/json'}).json()
    try:
        email = res['email'][0]['email']
        session['user_info']['email'] = email
    except:
        email = ''

    session['user_info']['is_curator'] = cf.isCurator() 
    # return flask.jsonify(session['user_info'])  
    return redirect(session['next'])


@app.route('/logout', methods=['GET'])
def logout():
    if 'user_info' in session:
        del session['user_info']
    if 'google_access_token' in session:
        del session['google_access_token']
    if 'orcid_access_token' in session:
        del session['orcid_access_token']
    if 'project_metadata' in session:
        del session['project_metadata']
    if request.args.get('type') == 'curator':
        return redirect(url_for('curator'))
    if request.args.get('type') == 'award_letters':
        return redirect(url_for('award_letters'))
    return redirect(url_for('home'))


@app.route("/index", methods=['GET'])
@app.route("/", methods=['GET'])
@app.route("/home", methods=['GET'])
def home():
    template_dict = {}

    if request.method == 'GET':
        if request.args.get('google') == 'false':
            session['googleSignedIn'] = False

    # read in news
    news_dict = []
    with open("inc/recent_news.txt", 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter="\t")
        for row in reader:
            if row[0] == "#" or len(row) < 2:
                continue
            news_dict.append({"date": row[0], "news": row[1]})
        template_dict['news_dict'] = news_dict
    # read in recent data
    data_dict = []
    with open("inc/recent_data.txt", 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter="\t")
        for row in reader:
            if row[0] == "#" or len(row) < 4: 
                continue
            data_dict.append({"date": row[0], "link": row[1], "authors": row[2], "title": row[3]})
        template_dict['data_dict'] = data_dict

    # get all spatial extents
    (conn, cur) = connect_to_db()
    template_dict['spatial_extents'] = get_spatial_extents(conn=conn, cur=cur)

    template_dict['mapserver'] = app.config['MAPSERVER_URL']
    return render_template('home.html', **template_dict)


# @app.route("/home2")
# def home2():
#     template_dict = {}
#     # read in news
#     news_dict = []
#     with open("inc/recent_news.txt") as csvfile:
#         reader = csv.reader(csvfile, delimiter="\t")
#         for row in reader:
#             if row[0] == "#" or len(row) != 2: continue
#             news_dict.append({"date": row[0], "news": row[1]})
#         template_dict['news_dict'] = news_dict
#     # read in recent data
#     data_dict = []
#     with open("inc/recent_data.txt") as csvfile:
#         reader = csv.reader(csvfile, delimiter="\t")
#         for row in reader:
#             if row[0] == "#" or len(row) != 4: continue
#             data_dict.append({"date": row[0], "link": row[1], "authors": row[2], "title": row[3]})
#         template_dict['data_dict'] = data_dict
#     return render_template('home2.html', **template_dict)


@app.route('/overview')
def overview():
    return render_template('overview.html')


@app.route('/faq')
def faq():
    return render_template('faq.html')


@app.route('/webinars')
def webinars():
    return render_template('webinars.html')


@app.route('/services')
def services():
    return render_template('services.html')


@app.route('/links')
def links():
    return render_template('links.html')


@app.route('/sdls')
def sdls():
    return render_template('sdls.html')


@app.route('/legal')
def legal():
    return render_template('legal.html')


@app.route('/privacy')
def privacy():
    return render_template('privacy.html')


@app.route('/terms_of_use')
def terms_of_use():
    return render_template('terms_of_use.html')


@app.route('/title_examples')
def title_examples():
    return render_template('title_examples.html')


@app.route('/abstract_examples')
def abstract_examples():
    return render_template('abstract_examples.html')


@app.route('/contact', methods=['GET'])
def contact():
    return render_template('contact.html')


def checkHoneypot(form, msg, redirect):
    # check honeypot trap - these fields are only visible to bots, not humans
    if form.get('email') or form.get('name'):
        raise BadSubmission(msg, redirect)



@app.route('/submit', methods=['GET'])
def submit():
    if request.method == 'GET':
        if request.args.get('google') == 'false':
            session['googleSignedIn'] = False
    return render_template('submit.html')


@app.route('/data_repo')
def data_repo():
    return render_template('data_repo.html')


@app.route('/news')
def news():
    template_dict = {}
    # read in news
    news_dict = []
    with open("inc/recent_news.txt", 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter="\t")
        for row in reader:
            if row[0] == "#" or len(row) < 2:
                continue
            news_dict.append({"date": row[0], "news": row[1]})
        template_dict['news_dict'] = news_dict

    return render_template('news.html', **template_dict)


@app.route('/data')
def data():
    template_dict = {}
    # read in recent data
    data_dict = []
    with open("inc/recent_data.txt", 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter="\t")
        for row in reader:
            if row[0] == "#" or len(row) < 4:
                continue
            data_dict.append({"date": row[0], "link": row[1], "authors": row[2], "title": row[3]})
        template_dict['data_dict'] = data_dict
    return render_template('data.html', **template_dict)


@app.route('/devices')
def devices():
    user_info = session.get('user_info')
    if user_info is None:
        session['next'] = '/devices'
        return redirect(url_for('login'))
    else:
        return render_template('device_examples.html', name=user_info['name'])


@app.route('/procedures')
def procedures():
    user_info = session.get('user_info')
    if user_info is None:
        session['next'] = '/procedures'
        return redirect(url_for('login'))
    else:
        return render_template('procedure_examples.html', name=user_info['name'])


@app.route('/content')
def content():
    user_info = session.get('user_info')
    if user_info is None:
        session['next'] = '/content'
        return redirect(url_for('login'))
    else:
        return render_template('content_examples.html', name=user_info['name'])


@app.route('/files_to_upload')
def files_to_upload():
    user_info = session.get('user_info')
    if user_info is None:
        session['next'] = '/files_to_upload'
        return redirect(url_for('login'))
    else:
        return render_template('files_to_upload_help.html', name=user_info['name'])


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    return str(obj)


@app.route('/view/dataset/<dataset_id>')
def landing_page(dataset_id):
    datasets = get_datasets([dataset_id])
    if len(datasets) == 0:
        return redirect(url_for('not_found'))
    metadata = datasets[0]
   
    url = metadata['url']
    if not url:
        return redirect(url_for('not_found'))
    usap_domain = app.config['USAP_DOMAIN']
    
    # check for proprietary hold
    if len(metadata['release_date']) == 4:
        metadata['hold'] = datetime.strptime(metadata['release_date'], '%Y') > datetime.utcnow()
    elif len(metadata['release_date']) == 10:  
        metadata['hold'] = datetime.strptime(metadata['release_date'], '%Y-%m-%d') > datetime.utcnow()
    else:
        metadata['hold'] = False

    if url.startswith(usap_domain): 
        (conn, cur) = connect_to_db()
        files_db = get_files(conn, cur, dataset_id)
        omit = set(['readme.txt', '00README.txt', 'index.php', 'index.html', 'data.html'])
        files = [f for f in files_db if f['file_name'] not in omit]
        for f in files:
            f['size'] = humanize.naturalsize(f['file_size'])
            if f['dir_name'].startswith('/archive'):
                f['url'] = 'archive'
            else:
                f['url'] = usap_domain + app.config['DATASET_FOLDER'] + os.path.join(f['dir_name'], f['file_name'])
            f['document_types'] = f['document_types']
        metadata['files'] = files
    else:
        metadata['files'] = [{'url': url, 'file_name': os.path.basename(os.path.normpath(url))}]

    if metadata.get('url_extra'):
        metadata['url_extra'] = os.path.basename(metadata['url_extra'])

    metadata['creator_orcids'] = []
    for c in metadata['creator'].split('; '):
        p = get_person(c)
        if p:
            metadata['creator_orcids'].append({'id': p.get('id'), 'orcid': p.get('id_orcid')}) 
        else:
            metadata['creator_orcids'].append({'id': c, 'orcid': None})

    if not metadata['citation'] or metadata['citation'] == '':
        metadata['citation'] = makeCitation(metadata, dataset_id)
    metadata['json_ld'] = makeJsonLD(metadata, dataset_id)

    # get count of how many times this dataset has been downloaded
    metadata['downloads'] = getDownloadsCount(dataset_id)

    # get CMR/GCMD URLs for dif records
    getCMRUrls(metadata['dif_records'])

    return render_template('landing_page.html', data=metadata, contact_email=app.config['USAP-DC_GMAIL_ACCT'], secret=app.config['RECAPTCHA_DATA_SITE_KEY'])


def getCMRUrls(dif_records):
    # get the CMR pages for each dif record
    if not dif_records:
        return
    for rec in dif_records:
        # use the CMR API to get the concept-id
        try: 
            api_url = app.config['CMR_API'] + rec['dif_id']
            r = requests.get(api_url).json()
            concept_id = r['feed']['entry'][0]['id']
            # generate the GCMD page URL
            rec['cmr_url'] = app.config['CMR_URL'] + concept_id + '.html'
        except:
            pass   



def getDownloadsCount(uid):
    try:
        (conn, cur) = connect_to_db()
        query = '''SELECT SUM(count) FROM (
                    SELECT count(DISTINCT (remote_host, to_char(time,'YY-MM-DD'))) FROM access_logs_downloads 
                    WHERE resource_requested ~ '%s'
                    AND resource_requested !~ 'Please_contact_us_to_arrange_for_download.txt' 
                    AND resource_requested !~ 'image_file_list'
                    UNION
                    SELECT count(*)  FROM access_ftp_downloads WHERE dataset_id = '%s'
                    ) as downloads;''' % (uid, uid)
        cur.execute(query)

        res = cur.fetchone() 
        return res['sum']
    except:
        return None


def getProjectViews(uid):
    try:
        (conn, cur) = connect_to_db()
        # count number of unique date-IP combinations, and exclude any where the remote_host
        # has made more than 100 views in 1 day, as probably a bot
        query = '''SELECT COUNT(*) FROM
                    (SELECT DISTINCT(remote_host, TO_CHAR(time, 'YY-MM-DD'))::text AS host_date 
                        FROM access_logs_views WHERE resource_requested ~ '%s'
                    ) a1 
                    JOIN 
                    access_views_ip_date_matview a2
                    ON a1.host_date = a2.host_date
                    WHERE a2.count <= 100
                    ''' % (uid)
        cur.execute(query)
        res = cur.fetchone()
        return res['count']
    except Exception as e:
        print(e)
        return None


def makeJsonLD(data, uid):
    keywords = cf.getDatasetKeywords(uid)
    creators = []
    for p in data.get('persons'):
        creator = {
            "@type": "Person",
            "additionalType": "geolink:Person",
            "name": p.get('id'),
            "email": p.get('email'),
            "affiliation": {
                "@type": "Organization",
                "name": p.get('address')
            }
        }
        creators.append(creator)

    awards = []
    for a in data.get('awards'):
        (conn, cur) = connect_to_db()
        cur.execute("SELECT program_id FROM award_program_map apm  WHERE award_id='%s'" % a['award'])
        program = cur.fetchone()
        if (not program):
            program = {'program_id': 'NONE'}
        
        award = {
            "@type": "Role",
            "roleName": "credit",
            "description": "funderName:NSF:%s:%s:%s awardNumber:%s awardTitle:%s" % (a.get('dir'), a.get('div'), program['program_id'], a.get('award'), a.get('title'))
        }
        awards.append(award)

    spatial_coverage = []
    for s in data.get('spatial_extents'):
        if s['west'] == s['east'] and s['south'] == s['north']:
            ex = {
                "@type": "Place",
                "geo": {
                    "@type": "GeoCoordinates",
                    "longitude": s.get('east'),
                    "latitude": s.get('north')
                }
            }
        else:   
            ex = {
                "@type": "Place",
                "geo": {
                    "@type": "GeoShape",
                    "box": "%s, %s, %s, %s" % (s.get('west'), s.get('south'), s.get('east'), s.get('north'))
                }
            }
        spatial_coverage.append(ex)

    if data.get('doi'):
        doi = data['doi']
    else:
        doi = 'TBD'

    description = data.get('abstract')
    if not description or description == '':
        description = data.get('title')

    json_ld = {
        "@context": "https://schema.org/",
        "@type": "Dataset",
        "@id": "doi:" + doi,
        "additionalType": ["geolink:Dataset", "vivo:Dataset"],
        "name": data.get('title'),
        "description": description,
        "citation": data.get('citation'),
        "datePublished": data.get('release_date'),
        "keywords": [kw['keyword_label'] for kw in keywords],
        "creator": creators,
        "distribution": [
            {
                "@type": "DataDownload",
                "additionalType": "http://www.w3.org/ns/dcat#DataCatalog",
                "encodingFormat": "text/xml",
                "name": "ISO Metadata Document",
                "url": "http://get.iedadata.org/metadata/iso/usap/%siso.xml" % uid,
                "contentUrl": url_for('file_download', filename='filename')
            },
            {
                "@type": "DataDownload",
                "@id": "http://dx.doi.org/%s" % doi,
                "additionalType": "dcat:distribution",
                "url": "http://dx.doi.org/%s" % doi,
                "name": "landing page",
                "description": "Link to a web page related to the resource.. Service Protocol: Link to a web page related to the resource.. Link Function: information",
                "contentUrl": url_for('file_download', filename='filename'),
                "encodingFormat": "text/html"
            },
            {
                "@type": "DataDownload",
                "@id": url_for('landing_page', dataset_id=uid),
                "additionalType": "dcat:distribution",
                "url": url_for('landing_page', dataset_id=uid),
                "name": "landing page",
                "description": "Link to a web page related to the resource.. Service Protocol: Link to a web page related to the resource.. Link Function: information",
                "contentUrl": url_for('file_download', filename='filename'),
                "encodingFormat": "text/html"
            }

        ],
        "identifier": {
            "@type": "PropertyValue",
            "propertyID": "dataset identifier",
            "value": "doi:" + doi
        },
        "contributor": awards,
        "license": [
            {
                "@type": "CreativeWork", 
                "URL": "https://creativecommons.org/licenses/by-nc-sa/3.0/us/",
                "name": "MD_Constraints",
                "description": "useLimitation: Creative Commons Attribution-NonCommercial-Share Alike 3.0 United States [CC BY-NC-SA 3.0].   "
            },
            {
                "@type": "CreativeWork", 
                "name": "MD_LegalConstraints",
                "description": "accessConstraints: license.    otherConstraints: Creative Commons Attribution-NonCommercial-Share Alike 3.0 United States [CC BY-NC-SA 3.0].   "
            },
            {
                "@type": "CreativeWork",
                "name": "MD_SecurityConstraints",
                "description": "classification: "
            }
        ],
        "publisher": {
            "@type": "Organization",
            "name": "U.S. Antarctic Program (USAP) Data Center"
        },
        "spatialCoverage": spatial_coverage
        
        
    }

    return json_ld


def getCitation(dataset_id):
    datasets = get_datasets([dataset_id])
    if len(datasets) == 0:
        return 'Could not get citation for dataset %s' % dataset_id
    metadata = datasets[0]
    if not metadata['citation'] or metadata['citation'] == '':
        metadata['citation'] = makeCitation(metadata, dataset_id)
    return metadata['citation']


def makeCitation(metadata, dataset_id):
    try:
        (conn, cur) = connect_to_db()
        cur.execute("SELECT person_id, first_name, middle_name, last_name FROM dataset_person_map dpm JOIN person ON person.id=dpm.person_id WHERE dataset_id='%s' ORDER BY person_id" % dataset_id)
        creators = cur.fetchall()

        co_pis = []

        if len(creators) == 0: 
            middle_init = ''
            creators = metadata['creator'].split(';')
            (last_name, first_name) = creators[0].split(',', 1)
            first_names = first_name.strip().split(' ')
            if len(first_names) > 1:
                middle_init = ' ' + first_names[1][0] + '.'
            pi = {'last_name': last_name, 'first_name': first_name, 'middle_init':middle_init}
        else:
    
            # try and find the person who matches the first in the dataset.creators list
            creator1 = metadata['creator'].split(' ')[0]
            for idx, creator in enumerate(creators):
                middle_init = ''

                if creator.get('first_name') and creator.get('last_name'):
                    first_name = creator['first_name']
                    last_name = creator['last_name']
                    
                    if creator.get('middle_name'):
                        middle_init = ' ' + creator['middle_name'][0] + '.'
                else:
                    (last_name, first_name) = creator['person_id'].split(',', 1)

                creator_details = {'last_name': last_name, 'first_name': first_name, 'middle_init':middle_init}

                # make sure we have a PI
                if idx == 0:
                    pi = creator_details
                if creator1.lower() in creator['person_id'].lower():
                    pi = creator_details
                else:
                    co_pis.append(creator_details)
 
        # try and follow AGU citation style guide https://www.agu.org/Publish-with-AGU/Publish/Author-Resources/Grammar-Style-Guide
        creator_text = '%s, %s.%s' % (pi['last_name'], pi['first_name'].strip()[0], pi['middle_init'])

        if len(creators) < 8:
            for idx, co_pi in enumerate(co_pis):
                if idx != len(co_pis)-1:
                    creator_text += ', %s, %s.%s' % (co_pi['last_name'], co_pi['first_name'].strip()[0], co_pi['middle_init'])
                else:
                    creator_text += ', & %s, %s.%s' % (co_pi['last_name'], co_pi['first_name'].strip()[0], co_pi['middle_init'])
        else:
            for idx, co_pi in enumerate(co_pis):
                if idx < 5:
                    creator_text += ', %s, %s.%s' % (co_pi['last_name'], co_pi['first_name'].strip()[0], co_pi['middle_init'])

            creator_text += ', et al. ' 
        year = metadata['release_date'].split('-')[0]

        citation = '%s (%s) "%s" U.S. Antarctic Program (USAP) Data Center. doi: https://doi.org/%s.' % (creator_text, year, metadata['title'], metadata['doi'])
        return citation
    except:
        return None


@app.route('/dataset/<path:filename>', methods=['GET','POST'])
def file_download(filename):
    form = request.form.to_dict()
    g_recaptcha_response = form.get('g-recaptcha-response')
    remoteip = request.remote_addr

    resp = requests.post('https://www.google.com/recaptcha/api/siteverify', data={'response':g_recaptcha_response,'remoteip':remoteip,'secret': app.config['RECAPTCHA_SECRET_KEY']}).json()
    if resp.get('success'):
        # dataset_id = request.args.get('dataset_id')
        dataset_id = form.get('dataset_id')
        if not dataset_id: 
            return redirect(url_for('not_found'))

        # test for proprietary hold
        ds = get_datasets([dataset_id])[0]
        # check for proprietary hold
        if len(ds['release_date']) == 4:
            hold = datetime.strptime(ds['release_date'], '%Y') > datetime.utcnow()
        elif len(ds['release_date']) == 10:  
            hold = datetime.strptime(ds['release_date'], '%Y-%m-%d') > datetime.utcnow()
        else:
            hold = False

        if hold:
            return redirect(url_for('data_on_hold', release_date=ds['release_date'])) 

        directory = os.path.join(current_app.root_path, app.config['DATASET_FOLDER'])
        return send_from_directory(directory, filename, as_attachment=True)
    else:
        msg = "<br/>You failed to pass the reCAPTCHA test<br/>"
        raise CaptchaException(msg, url_for('home'))


@app.route('/readme/<dataset_id>')
def readme(dataset_id):
    (conn, cur) = connect_to_db()
    cur.execute('''SELECT url_extra FROM dataset WHERE id=%s''', (dataset_id,))
    res = cur.fetchall()

    if not res or not res[0]['url_extra']:
        return redirect(url_for('not_found'))
    url_extra = (res[0]['url_extra'])[1:]

    if url_extra.startswith('/'):
        url_extra = url_extra[1:]
    try:
        return send_from_directory(current_app.root_path, url_extra, as_attachment=False)
    except:
        return redirect(url_for('not_found'))


@app.route('/readme-download/<dataset_id>')
def readme_download(dataset_id):
    (conn, cur) = connect_to_db()
    cur.execute('''SELECT url_extra FROM dataset WHERE id=%s''', (dataset_id,))
    url_extra = cur.fetchall()[0]['url_extra'][1:]
    if url_extra.startswith('/'):
        url_extra = url_extra[1:]
    try:
        return send_from_directory(current_app.root_path, url_extra, as_attachment=True)
    except:
        return redirect(url_for('not_found'))


@app.route('/metadata/<path>')
def metadata(path):
    metapath = os.path.join(app.config['METADATA_FOLDER'], path)

    files = Path(metapath).iterdir()
    files_dict = []
    for file in files:
        name = file.name
        mtime = datetime.fromtimestamp(file.stat().st_mtime)
        size = file.stat().st_size
        url = '/metadata/'+path + '/' + name
        files_dict.append({'name': name, 'mtime': mtime, 'size': size, 'url': url})
    return render_template('metadata.html', files=files_dict, path=metapath)


@app.route('/metadata/<path>/<filename>')
def metadata_xml(path, filename):
    url = os.path.join(app.config['METADATA_FOLDER'], path, filename)
    try:
        return send_from_directory(current_app.root_path, url, as_attachment=False)
    except:
        return redirect(url_for('not_found'))


@app.route('/mapserver-template.html')
def mapserver_template():
    return render_template('mapserver-template.html')


@app.route('/map')
def _map():
    return render_template('data_map.html')


def get_crossref_sql():
    try:
        with open(app.config['CROSSREF_FILE'], encoding='utf-8') as infile:
            sql = infile.read()
    except:
        sql = "No Crossref SQL file to be ingested"
    return sql


@app.route('/curator', methods=['GET', 'POST'])
def curator():
    template_dict = {}
    template_dict['message'] = []
    template_dict['no_action_status'] = ['Completed', 'Edit completed', 'Rejected', 'No Action Required']
    (conn, cur) = connect_to_db(curator=True)

    # login
    if (not cf.isCurator()):
        session['next'] = request.url
        template_dict['need_login'] = True
    else:
        template_dict['need_login'] = False
        submitted_dir = os.path.join(current_app.root_path, app.config['SUBMITTED_FOLDER'])


        # if Add User To Dataset / Project, or Ingest Crossref button pressed
        if request.args.get('fnc') is not None:
            return render_template('curator.html', type=request.args['fnc'], projects=filter_datasets_projects(dp_type='Project'),
                                   datasets=filter_datasets_projects(dp_type='Dataset'), persons=get_persons(), orgs=get_orgs(), 
                                   roles=get_roles(), crossref_sql=get_crossref_sql())

        # if Add User submit button pressed
        if request.method == 'POST' and request.form.get('submit') == 'add_user':
            message = []
            error = None 
            try:
                msg = cf.addUserToDatasetOrProject(request.form)
                if msg:
                    error = "Error adding user: " + msg
                else:
                    message.append("Successfully added user")
            except Exception as err:
                error = "Error adding user: " + str(err)
            finally:
                return render_template('curator.html', type='addUser', projects=filter_datasets_projects(dp_type='Project'),
                                   datasets=filter_datasets_projects(dp_type='Dataset'), persons=get_persons(), orgs=get_orgs(), 
                                   roles=get_roles(), error=error, message=message)

        # if Add Import Crossref button pressed
        if request.method == 'POST' and request.form.get('submit') == 'crossref_to_db':
            message = []
            error = None 
            try:
                # run sql to import data into the database
                sql_str = request.form.get('crossref_sql')
                cur.execute(sql_str)
                # rename crossref file once it has been ingested
                if os.path.exists(app.config['CROSSREF_FILE']):
                    os.rename(app.config['CROSSREF_FILE'], app.config['OLD_CROSSREF_FILE'])

                message.append("Successfully imported to database")                        
            except Exception as err:
                error = "Error importing Crossref Publications: " + str(err)
            finally:
                return render_template('curator.html', type='addCrossref', crossref_sql=sql_str, message=message, error=error)
            
        if not request.args.get('uid'):
            # get list of json files in submission directory, ordered by date
            query = "SELECT * FROM submission ORDER BY submitted_date DESC"
            cur.execute(query)
            res = cur.fetchall()
            if res:
                submissions = []
                for sub in res:
                    uid = sub['uid']
                    landing_page = cf.getLandingPage(uid, cur)
                    submissions.append({'id': uid, 'date': sub['submitted_date'].strftime('%Y-%m-%d'), 'status': sub['status'], 
                                        'landing_page': landing_page, 'comments': sub['comments'], 'last_update': sub['last_update']})

            template_dict['submissions'] = submissions
        template_dict['coords'] = {'geo_n': '', 'geo_e': '', 'geo_w': '', 'geo_s': '', 'cross_dateline': False}

        if request.args.get('uid') is not None:
            filename = request.args.get('uid')
            uid = filename.replace('e', '')
            template_dict['uid'] = uid
            edit = filename[0] == 'e'

            query = "SELECT * FROM submission WHERE uid = '%s'" % filename
            cur.execute(query)
            res = cur.fetchone()
            if res:
                template_dict['status'] = res['status']
                if res['comments']:
                    template_dict['comments'] = res['comments']
            
            # get list of creator emails from db (if already in db)
            template_dict['email_recipients'] = cf.getCreatorEmails(uid)
            
            # check is project or dataset
            if uid[0] == 'p':
                template_dict['type'] = 'project'
                template_dict['tab'] = "project_json"
                template_dict['email_subject'] = "Message From USAP-DC regarding project %s" % uid
                # check whether project as already been imported to database
                template_dict['db_imported'] = cf.isProjectImported(uid)
                # if the project is already imported, retrieve any coordinates already in the dataset_spatial_map table
                if template_dict['db_imported']:
                    coords = cf.getProjectCoordsFromDatabase(uid)
                    if coords is not None:
                        template_dict['coords'] = coords
                    # also retrieve awards linked to project
                    template_dict['proj_awards'] = cf.getProjectAwardsFromDatabase(uid) 

                if edit:
                    template_dict['status_options'] = ['Pending', 'Edit completed', 'Rejected', 'No Action Required']
                else:
                    template_dict['status_options'] = ['Pending', 'DIF XML file missing', 
                                                       'Completed', 'Rejected', 'No Action Required']  

            else:
                template_dict['type'] = 'dataset'
                template_dict['tab'] = "json"
                template_dict['email_subject'] = "Message From USAP-DC regarding dataset %s" % uid
                # check whether dataset as already been imported to database
                template_dict['db_imported'] = cf.isDatabaseImported(uid)
                # if the dataset is already imported, retrieve any coordinates already in the dataset_spatial_map table
                if template_dict['db_imported']:
                    coords = cf.getCoordsFromDatabase(uid)
                    if coords is not None:
                        template_dict['coords'] = coords 
                    # also retrieve citation
                    template_dict['citation'] = getCitation(uid)
                    template_dict['dataset_keywords'] = cf.getDatasetKeywords(uid)
                # if this dataset replaces another, will need to update XML for old dataset too
                if not request.form.get('dc_uid'):
                    template_dict['dc_uid'] = uid
                if not edit:
                    template_dict['replaced_dataset'] = cf.getReplacedDataset(uid)
                    template_dict['status_options'] = ['Pending', 'Not yet registered with DataCite', 'ISO XML file missing', 
                                                       'Completed', 'Rejected', 'No Action Required'] 
                else:
                    template_dict['status_options'] = ['Pending', 'Edit Completed', 'Rejected', 'No Action Required']
                template_dict['weekly_report_options'] = cf.getWeeklyReportOptions(uid)
        
            submission_file = os.path.join(submitted_dir, filename + ".json")
            template_dict['filename'] = submission_file
            template_dict['sql'] = "Will be generated after you click on Create SQL and Readme in JSON tab."
            template_dict['readme'] = "Will be generated after you click on Create SQL and Readme in JSON tab."
            template_dict['dcxml'] = cf.getDataCiteXMLFromFile(uid)
            # for each keyword_type, get all keywords from database 
            template_dict['keywords'] = cf.getKeywordsFromDatabase()
            template_dict['archive_status'] = cf.getArchiveStatus(uid)

            if request.method == 'POST':
                template_dict.update(request.form.to_dict())

                # read in json and convert to sql
                if request.form.get('submit') == 'make_sql':
                    json_str = request.form.get('json')
                    json_data = json.loads(json_str)
                    template_dict['json'] = json_str

                    sql, readme_file = json2sql.json2sql(json_data, uid)

                    template_dict['sql'] = sql
                    template_dict['readme_file'] = readme_file
                    template_dict['tab'] = "sql"

                    if 'Error writing README file' in readme_file:
                        template_dict['error'] = readme_file
                        if 'Read-only file system' in readme_file:
                            template_dict['error'] += '\n---This is expected if you are running in DEV mode'
                    else:
                        readme_text = ''
                        try:
                            with open(readme_file, encoding='utf-8') as infile:
                                readme_text = infile.read()
                        except:
                            template_dict['error'] = "Can't read Read Me file"
                        template_dict['readme'] = readme_text

                # read in sql and submit to the database only
                elif request.form.get('submit') == 'import_to_db':
                    sql_str = request.form.get('sql').encode('utf-8')
                    template_dict['tab'] = "sql"
                    problem = False
                    print("IMPORTING TO DB")
                    try:
                        # run sql to import data into the database
                        cur.execute(sql_str)
                        cf.updateEditFile(uid)
                        cf.updateMatViews()

                        template_dict['message'].append("Successfully imported to database")
                        if edit:
                            template_dict['message'].append("Remember to update DataCite and ISOXML records")
                        data = json.loads(request.form.get('json'))
                        
                        template_dict['email_recipients'] = cf.getCreatorEmails(uid)
                        # add contact and submitter to list of recipients
                        template_dict['email_recipients'] = getEmailsFromJson(data, template_dict['email_recipients'])

                        template_dict['email_text'] = get_email_template(edit, "dataset", uid, data, False) 

                        coords = cf.getCoordsFromDatabase(uid)
                        if coords is not None:
                            template_dict['coords'] = coords
                        template_dict['citation'] = getCitation(uid)
                        template_dict['landing_page'] = '/view/dataset/%s' % uid
                        template_dict['db_imported'] = True
                        template_dict['dataset_keywords'] = cf.getDatasetKeywords(uid)

                    except Exception as err:
                        template_dict['error'] = "Error Importing to database: " + str(err)
                        problem = True

                    if not problem:
                        # copy uploaded files to their permanent home
                        print('Copying uploaded files')
                        json_str = request.form.get('json').encode('utf-8')
                        json_data = json.loads(json_str)
                        timestamp = json_data.get('timestamp')
                        if timestamp:
                            upload_dir = os.path.normpath(os.path.join(current_app.root_path, app.config['UPLOAD_FOLDER'], timestamp))
                            uid_dir = os.path.normpath(os.path.join(current_app.root_path, app.config['DATASET_FOLDER'], 'usap-dc', uid))
                            dest_dir = os.path.join(uid_dir, timestamp)
                            try:
                                if not uid_dir.startswith(current_app.root_path) or not upload_dir.startswith(current_app.root_path):
                                    raise Exception()

                                if os.path.exists(dest_dir):
                                    shutil.rmtree(dest_dir)
                                shutil.copytree(upload_dir, dest_dir)

                                # if this dataset is replacing an existing one
                                # any files from data['filenames'] that were not in
                                # the submission dir should be in the dataset dir
                                # or the replaced dataset
                                if json_data.get('related_dataset') and json_data.get('filenames'):
                                    old_ds = get_datasets([json_data['related_dataset']])[0]
                                    old_dir = os.path.normpath(old_ds.get('url').replace(app.config['USAP_DOMAIN'], current_app.root_path + '/'))
                                    if not old_dir.startswith(current_app.root_path):
                                        raise Exception()

                                    for f in json_data['filenames']:
                                        if not os.path.exists(os.path.join(dest_dir, f)) and os.path.exists(os.path.join(old_dir, f)):
                                            shutil.copy(os.path.join(old_dir, f), dest_dir)

                                # change permissions
                                os.chmod(uid_dir, 0o775)
                                for root, dirs, files in os.walk(uid_dir):
                                    for d in dirs:
                                        os.chmod(os.path.join(root, d), 0o775)
                                    for f in files:
                                        os.chmod(os.path.join(root, f), 0o664)

                            except Exception as err:
                                template_dict['error'] = "Error copying uploaded files: " + str(err)
                                problem = True

                        # Update submission table
                        if edit:
                            update_status('e' + uid, 'Edit completed')
                        else:
                            update_status(uid, 'Not yet registered with DataCite')
                
                # save updates to the readme file
                elif request.form.get('submit') == "save_readme":
                    template_dict.update(request.form.to_dict())
                    template_dict['tab'] = "readme"
                    readme_str = request.form.get('readme').encode('utf-8')
                    filename = os.path.normpath(request.form.get('readme_file'))
                    try:
                        if not filename.startswith(app.config['DOCS_FOLDER']):
                            raise Exception()
                        with open(filename, 'w') as out_file:
                            out_file.write(readme_str.decode())
                        os.chmod(filename, 0o664)
                        template_dict['message'].append("Successfully updated Read Me file")
                    except:
                        template_dict['error'] = "Error updating Read Me file"

                # assign keywords in database
                elif request.form.get('submit') == "assign_keywords":
                    template_dict['tab'] = "keywords"
                    assigned_keywords = request.form.getlist('assigned_keyword')
                    (msg, status) = cf.addKeywordsToDatabase(uid, assigned_keywords)
                    if status == 0:
                        template_dict['error'] = msg
                    else:
                        template_dict['message'].append(msg)
                        template_dict['dataset_keywords'] = cf.getDatasetKeywords(uid)

                # update spatial bounds in database
                elif request.form.get('submit') == "spatial_bounds":
                    template_dict['tab'] = "spatial"
                    coords = {'geo_n': request.form.get('geo_n'), 
                              'geo_e': request.form.get('geo_e'), 
                              'geo_w': request.form.get('geo_w'),
                              'geo_s': request.form.get('geo_s'), 
                              'cross_dateline': request.form.get('cross_dateline') is not None}
                    template_dict['coords'] = coords
                    if check_spatial_bounds(coords):
                        (msg, status) = cf.updateSpatialMap(uid, coords)
                        if status == 0:
                            template_dict['error'] = msg
                        else:
                            template_dict['message'].append(msg)
                    else:
                        template_dict['error'] = 'Invalid bounds'

                # update award in database
                elif request.form.get('submit') == "award":
                    template_dict['tab'] = "spatial"
                    award = request.form.get('award')
                    (msg, status) = cf.addAwardToDataset(uid, award)
                    if status == 0:
                        template_dict['error'] = msg
                    else:
                        template_dict['message'].append(msg)

                # update citation in database 
                elif request.form.get('submit') == "citation":
                    template_dict['tab'] = "spatial"
                    citation = request.form.get('citation')
                    (msg, status) = cf.updateCitation(uid, citation)
                    if status == 0:
                        template_dict['error'] = msg
                    else:
                        template_dict['message'].append(msg)

                # DataCite DOI submission
                elif request.form.get('submit') == "submit_to_datacite":
                    template_dict.update(request.form.to_dict())
                    template_dict['tab'] = "dcxml"
                    xml_str = request.form.get('dcxml').encode('utf-8')
                    # could be new dataset, or could be editing old replaced dataset
                    dc_uid = template_dict['dc_uid']
                    if dc_uid != uid: 
                        edit = True
                    datacite_file = cf.getDCXMLFileName(dc_uid)
                    try:
                        with open(datacite_file, 'w', encoding='utf-8') as out_file:
                            out_file.write(xml_str.decode())
                        os.chmod(datacite_file, 0o664)

                        msg = cf.submitToDataCite(dc_uid, edit)
                        if msg.find("Error") >= 0:
                            template_dict['error'] = msg
                            problem = True
                        else:
                            template_dict['message'].append(msg)

                            message, doi = msg.split('DOI: ', 1)

                            data = json.loads(request.form.get('json'))
                            
                            template_dict['email_recipients'] = cf.getCreatorEmails(uid)
                            # add contact and submitter to list of recipients
                            template_dict['email_recipients'] = getEmailsFromJson(data, template_dict['email_recipients'])

                            template_dict['email_text'] = get_email_template(edit, "dataset", uid, data, True, doi)
                            # Update submission table
                            update_status(uid, 'ISO XML file missing')
                            template_dict['citation'] = getCitation(uid)

                    except Exception as err:
                        template_dict['error'] = "Error updating DataCite XML file: " + str(err) + " " + datacite_file

                # DataCite XML generation
                elif request.form.get('submit') == "generate_dcxml":
                    template_dict.update(request.form.to_dict())
                    template_dict['tab'] = "dcxml"
                    datacite_file, status = cf.getDataCiteXML(uid)
                    if status == 0:
                        template_dict['error'] = "Error: Unable to get DataCiteXML from database."
                    else:
                        template_dict['dcxml'] = cf.getDataCiteXMLFromFile(uid)
                        template_dict['dc_uid'] = uid

                # update DataCite XML for a replaced dataset
                elif request.form.get('submit') == "update_replaced_dcxml":
                    template_dict.update(request.form.to_dict())
                    template_dict['tab'] = "dcxml"
                    old_uid = template_dict['replaced_dataset']
                    datacite_file, status = cf.getDataCiteXML(old_uid)
                    if status == 0:
                        template_dict['error'] = "Error: Unable to get DataCiteXML from database."
                    else:
                        template_dict['dcxml'] = cf.getDataCiteXMLFromFile(old_uid)
                        template_dict['dc_uid'] = old_uid

                # Standalone save ISO XML to watch dir
                elif request.form.get('submit') == "save_isoxml":
                    template_dict.update(request.form.to_dict())
                    template_dict['tab'] = "isoxml"
                    dc_uid = template_dict['dc_uid']
                    xml_str = request.form.get('isoxml').encode('utf-8')
                    isoxml_file = cf.getISOXMLFileName(dc_uid)
                    try:
                        with open(isoxml_file, 'w', encoding='utf-8') as out_file:
                            out_file.write(xml_str.decode())
                            template_dict['message'].append("ISO XML file saved to watch directory.")
                        os.chmod(isoxml_file, 0o664)
                        if not edit:
                            # update Recent Data list
                            error = cf.updateRecentData(dc_uid)
                            if error:
                                template_dict['error'] = error
                            else:
                                # Update submission table
                                update_status(uid, 'Completed')

                    except Exception as err:
                        template_dict['error'] = "Error saving ISO XML file to watch directory: " + str(err)

                # ISO XML generation
                elif request.form.get('submit') == "generate_isoxml":
                    template_dict.update(request.form.to_dict())
                    template_dict['tab'] = "isoxml"
                    isoxml = cf.getISOXMLFromFile(uid, update=True)
                    if isoxml.find("Error") >= 0:
                        template_dict['error'] = "Error: Unable to generate ISO XML."
                    template_dict['isoxml'] = isoxml
                    template_dict['dc_uid'] = uid

                # update ISO XML for a replaced dataset
                elif request.form.get('submit') == "update_replaced_isoxml":
                    template_dict.update(request.form.to_dict())
                    template_dict['tab'] = "isoxml"
                    old_uid = template_dict['replaced_dataset']
                    isoxml = cf.getISOXMLFromFile(old_uid, update=True)
                    if isoxml.find("Error") >= 0:
                        template_dict['error'] = "Error: Unable to generate ISO XML."
                    template_dict['isoxml'] = isoxml
                    template_dict['dc_uid'] = old_uid

                # archive dataset
                elif request.form.get('submit') == "archive":
                    template_dict['tab'] = "archive"
                    success, error = cf.markReadyToArchive(uid)
                    if error:
                        template_dict['error'] = error
                    else:
                        template_dict['message'].append(success)
                        template_dict['archive_status'] = cf.getArchiveStatus(uid)

                # Send email to creator and editor - for both datasets and projects
                elif request.form.get('submit') == "send_email":
                    template_dict['tab'] = 'email'
                    try:
                        sender = app.config['USAP-DC_GMAIL_ACCT']
                        recipients_text = request.form.get('email_recipients')
                        recipients = recipients_text.splitlines()
                        recipients.append(app.config['USAP-DC_GMAIL_ACCT'])
                        msg_raw = create_gmail_message(sender, recipients, request.form.get('email_subject'), request.form.get('email_text'))
                        # msg_raw['threadId'] = get_threadid(uid) - this slow down the process, and I don't know if it is necessary

                        service, error = connect_to_gmail()
                        if error:
                            template_dict['error'] = error
                        else:
                            success, error = send_gmail(service, 'me', msg_raw)
                            template_dict['message'].append(success)
                            template_dict['error'] = error
 
                    except Exception as err:
                        template_dict['error'] = "Error sending email: " + str(err)

                # Update status or comments - for both datasets and projects
                elif request.form.get('submit') == "change_status":
                    template_dict['tab'] = 'status'
                    try:
                        status = request.form.get('status')
                        comments = request.form.get('comment_text')
                        template_dict['comments'] = comments
                        query = "UPDATE submission SET (status, comments, last_update) = ('%s', '%s', '%s') WHERE uid = '%s'; COMMIT;" \
                                % (status, comments.replace("'", "''"), datetime.now().strftime('%Y-%m-%d'), filename)
                        cur.execute(query)
                        template_dict['message'].append("Submission status and/or comments updated.")
                    except Exception as err:
                        template_dict['error'] = "Error updating status and/or comments: " + str(err)
                    

                    if template_dict['type'] == 'dataset':
                        try:
                            ds_dif = request.form.get('ds_dif_cbx') != None
                            ds_proj = request.form.get('ds_proj_cbx') != None
                            template_dict["weekly_report_options"] = {'dataset_id': uid, 'no_dif': ds_dif, 'no_project': ds_proj}
                            query = """INSERT INTO dataset_weekly_report(dataset_id, no_dif, no_project) VALUES (%s, %s, %s)
                                       ON CONFLICT (dataset_id) DO
                                       UPDATE SET no_dif=%s, no_project=%s;
                                    """ % (uid, ds_dif, ds_proj, ds_dif, ds_proj)
                            cur.execute(query)

                        except Exception as err:
                            template_dict['error'] = "Error updating Weekly Report options: " + str(err)


                # PROJECT CURATION

                # read in json and convert to sql
                elif request.form.get('submit') == 'make_project_sql':
                    json_str = request.form.get('proj_json')
                    json_data = json.loads(json_str)
                    template_dict['json'] = json_str
                    sql = cf.projectJson2sql(json_data, uid)
                    template_dict['sql'] = sql
                    template_dict['tab'] = "project_sql"
   
                # read in sql and submit to the database only
                elif request.form.get('submit') == 'import_project_to_db':
                    sql_str = request.form.get('proj_sql')
                    template_dict['sql'] = sql_str
                    template_dict['tab'] = "project_sql"
                    problem = False
                    print("IMPORTING TO DB")
                    try:
                        # run sql to import data into the database
                        cur.execute(sql_str)
                        cf.updateEditFile(uid)
                        cf.updateMatViews()
                        
                        template_dict['message'].append("Successfully imported to database")
                        data = json.loads(request.form.get('proj_json'))
                        
                        template_dict['email_recipients'] = cf.getCreatorEmails(uid)
                        # add contact and submitter to list of recipients
                        template_dict['email_recipients'] = getEmailsFromJson(data, template_dict['email_recipients'])

                        template_dict['email_text'] = get_email_template(edit, "project", uid, data, False)
                        
                        template_dict['landing_page'] = url_for('project_landing_page', project_id=uid)
                        template_dict['db_imported'] = True
                        template_dict['proj_awards'] = cf.getProjectAwardsFromDatabase(uid)
                    except Exception as err:
                        template_dict['error'] = "Error Importing to database: " + str(err)
                        problem = True

                    if not problem:
                        # copy uploaded files to their permanent home
                        print('Copying uploaded files')
                        json_str = request.form.get('json').encode('utf-8')
                        json_data = json.loads(json_str)
                        if json_data.get('dmp_file') is not None and json_data['dmp_file'] != '' and \
                           json_data.get('upload_directory') is not None and json_data.get('award') is not None:
                            src = os.path.normpath(os.path.join(json_data['upload_directory'], json_data['dmp_file']))
                            dst_dir = os.path.normpath(os.path.join(app.config['AWARDS_FOLDER'], json_data['award'].split(' ')[0]))
                            try:
                                # secutity check
                                if not dst_dir.startswith(app.config['AWARDS_FOLDER']):
                                    raise Exception()

                                if not os.path.exists(dst_dir):
                                    os.mkdir(dst_dir)
                                
                                dst = os.path.normpath(os.path.join(dst_dir, json_data['dmp_file']))
                                # secutity check
                                upload_path = os.path.join(current_app.root_path, app.config['UPLOAD_FOLDER'])
                                if not dst.startswith(app.config['AWARDS_FOLDER']) or not src.startswith(upload_path):
                                    raise Exception()

                                shutil.copyfile(src, dst)
                            except Exception as e:
                                template_dict['error'] = "ERROR: unable to copy data management plan to award directory. \n" + str(e)
                                problem = True

                        # Update submission table
                        if edit:
                            update_status('e' + uid, 'Edit completed')
                        else:
                            update_status(uid, 'DIF XML file missing')

                # update spatial bounds in database
                elif request.form.get('submit') == "project_spatial_bounds":
                    template_dict['tab'] = "spatial"
                    coords = {'geo_n': request.form.get('proj_geo_n'), 
                              'geo_e': request.form.get('proj_geo_e'), 
                              'geo_w': request.form.get('proj_geo_w'),
                              'geo_s': request.form.get('proj_geo_s'), 
                              'cross_dateline': request.form.get('proj_cross_dateline') is not None}
                    template_dict['coords'] = coords
                    if check_spatial_bounds(coords):
                        (msg, status) = cf.updateProjectSpatialMap(uid, coords)
                        if status == 0:
                            template_dict['error'] = msg
                        else:
                            template_dict['message'].append(msg)
                    else:
                        template_dict['error'] = 'Invalid bounds'
                
                # update award in database
                elif request.form.get('submit') == "project_award":
                    template_dict['tab'] = "spatial"
                    data = dict(request.form)

                    # any award updates
                    update_awards = []
                    for key in list(request.form.keys()):
                        if 'proj_award_' in key:
                            award_id = key.split('_')[-1]
                            update_awards.append({'award_id': award_id, 
                                                 'is_main_award': data.get('proj_main_award') == award_id, 
                                                 'is_previous_award': data.get('proj_previous_award_'+award_id) == 'on',
                                                 'remove': data.get('remove_award_'+award_id) == 'on'
                                                 })

                    # any new awards
                    new_award = None
                    award = data.get('proj_award')
                    if award and award.strip() != '':
                        new_award = {'award_id': award,
                                     'is_main_award': data.get('proj_main_award') == 'new_award', 
                                     'is_previous_award': data.get('proj_previous_award') == 'on'}

                    (msg, status) = cf.updateProjectAwards(uid, update_awards, new_award)
                    if status == 0:
                        template_dict['error'] = msg
                    else:
                        template_dict['message'].append(msg)
                    template_dict['proj_awards'] = cf.getProjectAwardsFromDatabase(uid) 

                # update file info in database
                elif request.form.get('submit') == "generate_file_sql":
                    template_dict['tab'] = "spatial"
                    ds = get_datasets([uid])[0]
                    url = ds['url']
                    dataset_dir = app.config['DATASET_FOLDER'] + url.replace(config['USAP_DOMAIN']+'dataset', '')
                    template_dict['file_sql'] = cf.get_file_info(uid, url, dataset_dir, True)
                    template_dict['file_sql'] += "COMMIT;"
                elif request.form.get('submit') == "update_file_info":
                    template_dict['tab'] = "spatial"
                    sql_str = request.form.get('file_sql').encode('utf-8')
                    try:
                        cur.execute(sql_str)
                        msg = "File Info successfully updated in database."
                        template_dict['message'].append(msg)
                        template_dict['landing_page'] = cf.getLandingPage(uid, cur)
                        template_dict['file_sql'] = ''
                    except Exception as err:
                        template_dict['error'] = "Error Updating File Info in database: " + str(err)                    

                # generate DIF XML from JSON file
                elif request.form.get('submit') == "generate_difxml":
                    template_dict.update(request.form.to_dict())
                    template_dict['tab'] = "difxml"
                    proj_data = get_project(uid)
                    difxml = cf.getDifXML(proj_data, uid)
                    if difxml.find("Error") >= 0:
                        template_dict['error'] = "Error: Unable to generate DIF XML."
                    template_dict['difxml'] = difxml

                # save changes to DIF XML in watch directory
                elif request.form.get('submit') == "save_difxml":
                    template_dict.update(request.form.to_dict())
                    template_dict['tab'] = "difxml"
                    xml_str = request.form.get('difxml').encode('utf-8')
                    difxml_file = cf.getDifXMLFileName(uid)
                    try:
                        with open(difxml_file, 'w', encoding='utf-8') as out_file:
                            out_file.write(xml_str.decode())
                            template_dict['message'].append("DIF XML file saved to watch directory.")
                        os.chmod(difxml_file, 0o664)
                        # Update submission table
                        update_status(uid, 'Completed')

                    except Exception as err:
                        template_dict['error'] = "Error saving DIF XML file to watch directory: " + str(err)

                #Validate the DIF XML
                elif request.form.get('submit') == "validate_difxml":
                    invalid_mark = " ✕"
                    valid_mark = " ✓"
                    template_dict['tab'] = "difxml"
                    xml_str = request.form.get("difxml").encode("utf-8")
                    #print("Validate DIF XML not yet implemented")
                    validation = cf.isXmlValid(xml_str)
                    if validation[0]:
                        template_dict['validation_symbol'] = valid_mark
                    else:
                        template_dict['validation_symbol'] = invalid_mark
                    root = ET.fromstring(validation[1])
                    #print("XML: ", validation[1])
                    #print("Now parsing the XML for user friendliness")
                    #print(root)
                    has_errors = False
                    has_warnings = False
                    for child in root:
                        #print(child.tag, child.text)
                        if child.tag == "errors":
                            for error in child:
                                has_errors = True
                                if 'xml_validation_errors' in template_dict:
                                    template_dict['xml_validation_errors'] += "<br>" + error.text
                                else:
                                    template_dict['xml_validation_errors'] = "Errors:<br>" + error.text
                        elif child.tag == "error":
                            has_errors = True
                            if 'xml_validation_errors' in template_dict:
                                template_dict['xml_validation_errors'] += "<br>" + child.text
                            else:
                                template_dict['xml_validation_errors'] = "Errors:<br>" + child.text
                        elif child.tag == "warnings":
                            has_warnings = True
                            template_dict['xml_validation_warnings'] = "Warnings:<br>" + child.text
                    template_dict['xml_validation_response'] = validation[1]
                    if has_errors:
                        template_dict['error'] = ("This DIF XML could not be validated. Check below for details.")
                    else:
                        template_dict['message'].append("Successfully validated DIF XML.")
                        if has_warnings:
                            template_dict['message'].append(" Validation completed with warnings. See below for details.")
                        difxml_file = cf.getDifXMLFileName(uid)
                        difxml_file_amd = cf.getDifXMLAMDFileName(uid)
                        try:
                            with open(difxml_file, 'w', encoding='utf-8') as out_file:
                                out_file.write(xml_str.decode())
                                template_dict['message'].append("DIF XML file saved to watch directory.")
                            os.chmod(difxml_file, 0o664)
                            with open(difxml_file_amd, 'w', encoding='utf-8') as out_file:
                                out_file.write(xml_str.decode())
                                template_dict['message'].append("DIF XML file saved to AMD directory.")
                            os.chmod(difxml_file_amd, 0o664)
                        except Exception as err:
                            template_dict['error'] = "Error saving validated DIF XML."

                # add DIF to DB
                elif request.form.get('submit') == "dif_to_db":
                    template_dict.update(request.form.to_dict())
                    template_dict['tab'] = "difxml"
                    (msg, status) = cf.addDifToDB(uid)
                    if status == 0:
                        template_dict['error'] = msg
                    else:
                        template_dict['message'].append(msg)

                        data = json.loads(request.form.get('proj_json'))
                        
                        template_dict['email_recipients'] = cf.getCreatorEmails(uid)
                        # add contact and submitter to list of recipients
                        template_dict['email_recipients'] = getEmailsFromJson(data, template_dict['email_recipients'])
                        
                        template_dict['email_text'] = get_email_template(edit, "project", uid, data, True)
                
                # generate CMR Submission text for AMD
                elif request.form.get('submit') == "generate_cmr_text":
                    template_dict.update(request.form.to_dict())
                    template_dict['tab'] = "cmr"
                    proj_data = get_project(uid)
                    cmr_text, error = cf.getCMRText(proj_data, uid)
                    if error:
                        template_dict['error'] = "Error: Unable to generate CMR text: %s" % error
                    else:
                        template_dict['cmr_text'] = cmr_text

            else:
                # display submission json file
                try:
                    submission_file = os.path.normpath(submission_file)
                    if not submission_file.startswith(submitted_dir):
                        raise Exception()
                    with open(submission_file) as infile:
                        data = json.load(infile)
                        submission_data = json.dumps(data, sort_keys=True, indent=4)
                        template_dict['json'] = submission_data
                        # add contact and submitter to list of recipients
                        template_dict['email_recipients'] = getEmailsFromJson(data, template_dict['email_recipients'])
                except:
                    # template_dict['error'] = "Can't read submission file: %s" % submission_file
                    template_dict['json'] = "Submitted JSON data not available for this project"
    reviewer_dict = {
        "file_name": "The filenames are descriptive and consistent",
        "file_format": "The file format is appropriate and can be opened",
        "file_organization": "The file organization is consistent and appropriate",
        "table_header": "Table header information is complete and consistent with documentation",
        "data_content": "The data set and its contents are clearly described",
        "data_process": "Processing information is adequate",
        "data_acquisition": "The process used to get the data is clearly described and appropriate",
        "data_spatial": "Geospatial and temporal informatioin are complete and described",
        "data_variable": "Variables and units follow standards or are well-defined",
        "data_issues": "Known issues and limitations are clearly described",
        "data_ref": "Publication or manuscript describing the data is provided"
    }
    template_dict['reviewer_dict'] = reviewer_dict

    return render_template('curator.html', **template_dict)


def getEmailsFromJson(data, email_recipients):
    # add contact and submitter to list of recipients
    email_recipients = ''
    if data.get('email') and email_recipients.find(data.get('email')) == -1:
        if data.get('pi_name_first') and data.get('pi_name_last'):
            contact = '\n"%s %s" <%s>' % (data['pi_name_first'], data['pi_name_last'], data['email'])
        elif data.get('authors') and data['authors'][0].get('first_name') and data['authors'][0].get('last_name'):
            contact = '\n"%s %s" <%s>' % (data['authors'][0]['first_name'], data['authors'][0]['last_name'], data['email'])
        else:
            contact = '\n<%s>' % data['email']
        email_recipients += contact
    if data.get('submitter_email') and email_recipients.find(data.get('submitter_email')) == -1:
        if data.get('submitter_name'):
            submitter = '\n"%s" <%s>' % (data['submitter_name'], data['submitter_email'])
        else:
            submitter = '\n<%s>' % data['submitter_email']
        email_recipients += submitter
    return email_recipients


def update_status(uid, status):
    (conn, cur) = connect_to_db(curator=True)
    today = datetime.now().strftime('%Y-%m-%d')
    query = "UPDATE submission SET (status, last_update) = ('%s', '%s') WHERE uid = '%s'; COMMIT;" \
        % (status, today, uid)
    cur.execute(query)


@app.route('/curator/help', methods=['GET', 'POST'])
def curator_help():
    template_dict = {}
    template_dict['submitted_dir'] = os.path.join(current_app.root_path, app.config['SUBMITTED_FOLDER'])
    template_dict['doc_dir'] = os.path.join(current_app.root_path, "doc/{dataset_id}")
    template_dict['upload_dir'] = os.path.join(current_app.root_path, app.config['UPLOAD_FOLDER'], "{upload timestamp}")
    template_dict['dataset_dir'] = os.path.join(current_app.root_path, app.config['DATASET_FOLDER'], 'usap-dc', "{dataset_id}", "{upload timestamp}")
    template_dict['watch_dir'] = os.path.join(current_app.root_path, "watch/isoxml")

    return render_template('curator_help.html', **template_dict)


def connect_to_gmail():
    creds = None
    if os.path.exists(app.config['GMAIL_PICKLE']):
        with open(app.config['GMAIL_PICKLE'], 'rb') as token:
            creds = pickle.load(token)
    else:
        # if the pickle doesn't exist, need to run the bin/gmail_quickstart.py on local system to
        # log in and create token.pickle. Then copy it to inc/token.pickle
        return None, "Unable to authorise connection to account"

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(gRequest())
        else:
            return None, "Gmail credentials are not valid"

    service = build('gmail', 'v1', credentials=creds)
    return service, None


@app.route('/emails', methods=['GET', 'POST'])
def emails():
    template_dict = {}
    template_dict['counts'] = []
    args = request.args.to_dict()
   
    date_fmt = "%Y-%m-%d"
    if args.get('start_date'):
        start_date = args['start_date']
    else:
        start_date = '2020-09-01'
    if args.get('end_date'):
        end_date = args['end_date']
    else:
        end_date = datetime.now().strftime(date_fmt)
    #need the day after the end date for gmail search
    end_date_gmail = datetime.strptime(end_date, date_fmt).date() + timedelta(days=1)    
    
    template_dict['start_date'] = start_date
    template_dict['end_date'] = end_date

    states = ['open', 'closed', 'all']
    if args.get('state'):
        state = args['state']
    else: 
        state ='all'
    template_dict['checked_states'] = {}
    for s in states:
        if s == state:
            template_dict['checked_states'][s] = 'checked'
        else:
           template_dict['checked_states'][s] = '' 

    # login
    if (not cf.isCurator()):
        session['next'] = request.url
        template_dict['need_login'] = True
        return render_template('emails.html', **template_dict)
    else:
        template_dict['need_login'] = False

    service, error = connect_to_gmail()
    if error:
        template_dict['error'] = error
        return render_template('emails.html', **template_dict)

    # display one thread
    if request.args.get('thread_id'):

        if request.form.get('submit') == "close_thread":
            cf.updateThreadState(request.args.get('thread_id'), 'closed')
        elif request.form.get('submit') == "reopen_thread":
            cf.updateThreadState(request.args.get('thread_id'), 'open')    

        thread = service.users().threads().get(userId='me', id=request.args['thread_id']).execute()

        if not thread.get('messages'):
            return
        message_0 = thread['messages'][0]
        message_last = thread['messages'][-1]
        thread['snippet'] = message_0['snippet']
        thread['num_messages'] = len(thread['messages'])
        for message in thread['messages']:
            raw_message = service.users().messages().get(userId='me', id=message['id'], format='raw').execute()
            msg_str = base64.urlsafe_b64decode(raw_message['raw'].encode()).decode()
            mime_msg = email.message_from_string(msg_str)
            subject = decode_header(mime_msg["Subject"])[0][0]
            if isinstance(subject, bytes):
                # if it's a bytes, decode to str
                subject = subject.decode()
            message['subject'] = subject
            message['sender'] = mime_msg.get("From")
            message['date'] = mime_msg.get("Date")
            message['attachments'] = []
         

            # if the email message is multipart
            if mime_msg.is_multipart():
                # iterate over email parts
                for part in mime_msg.walk():
                    # extract content type of email
                    content_type = part.get_content_type()
                    content_disposition = str(part.get("Content-Disposition"))
                    try:
                        # get the email body
                        body = part.get_payload(decode=True).decode()
                    except:
                        pass
                    if content_type == "text/plain" and "attachment" not in content_disposition:
                        # print text/plain emails and skip attachments
                        message['text'] = body
                    elif "attachment" in content_disposition:
                        # get attachment
                        filename = part.get_filename()
                        message['attachments'].append(filename)
                        ### If we want to download attachments, use this code.  
                        ### Not sure that we do, might be unsafe.
                        # if filename:
                        #     if not os.path.isdir(subject):
                        #         # make a folder for this email (named after the subject)
                        #         os.mkdir(subject)
                        #     filepath = os.path.join(subject, filename)
                        #     # download attachment and save it
                        #     open(filepath, "wb").write(part.get_payload(decode=True))
            else:
                # extract content type of email
                content_type = mime_msg.get_content_type()
                # get the email body
                body = mime_msg.get_payload(decode=True).decode()
                if content_type == "text/plain":
                    # print only text email parts
                    message['text'] = body


            if message == message_0:
                thread['subject'] = message['subject']
                thread['created_date'] = message['date'] 
                thread['originator'] = message['sender']
                thread['state'], thread['date_closed'] = cf.checkThreadInDb(thread['id'], thread['created_date'])
            elif message == message_last:
                thread['last_date'] = message['date']
                thread['last_sender'] = message['sender']
                thread['last_id'] = message['id']

        template_dict['thread'] = thread

        template_dict['email_recipients'] = "%s\n%s <%s>" % (thread['originator'], session['user_info'].get('name'), session['user_info'].get('email'))    

        if request.form.get('submit') == "send_email":
            try:
                sender = app.config['USAP-DC_GMAIL_ACCT']
                recipients_text = request.form.get('email_recipients')
                recipients = recipients_text.splitlines()
                recipients.append(app.config['USAP-DC_GMAIL_ACCT'])
                msg_raw = create_gmail_message(sender, recipients, thread.get('subject'), request.form.get('email_text'))
                msg_raw['threadId'] = thread['id']
 
                success, error = send_gmail(service, 'me', msg_raw)
                template_dict['message'] = success
                template_dict['error'] = error

            except Exception as err:
                template_dict['error'] = "Error sending email: " + str(err)
                print(err)
        

    # display list of all threads
    else:
        # Call the Gmail API
        q = "after:%s before:%s to:%s" % (start_date, end_date_gmail, app.config['USAP-DC_GMAIL_ACCT'] )
        results = service.users().threads().list(userId='me', q=q).execute()
        threads = results.get('threads', [])
        template_dict['threads'] = []
        if not threads:
            print('No threads found.')
        else:
            for t in threads:
                thread = service.users().threads().get(userId='me', id=t['id']).execute()
                
                if not thread.get('messages'):
                    continue
                message = thread['messages'][0]
                thread['snippet'] = message['snippet']
                thread['num_messages'] = len(thread['messages'])
                for header in message['payload']['headers']:
                    if header['name'] == 'From': 
                        thread['sender'] = header['value']
                    if header['name'] == 'Subject':
                        thread['subject'] = header['value']
                    if header['name'] == 'Date':
                        thread['date'] = header['value']
                
                if thread['sender'].find(app.config['USAP-DC_GMAIL_ACCT']) == -1:
                    thread['state'], thread['date_closed'] = cf.checkThreadInDb(t['id'], thread['date'])
                    # check date of most recent message - if after closed date, need to re-open thread
                    message_last = thread['messages'][-1]
                    for header in message_last['payload']['headers']:
                        if header['name'] == 'Date':
                            last_date = parse(header['value'])
                    if last_date and thread['date_closed'] and thread['date_closed'] < last_date:
                        cf.updateThreadState(t['id'], 'open')
                        thread['state'] = 'open'

                    if thread['state'] == state or state == 'all':
                        template_dict['threads'].append(thread)

        template_dict['counts'] = cf.getThreadNumbers(start_date, end_date)

        
    return render_template('emails.html', **template_dict)


def is_ascii(s):
    return all(ord(c) < 128 for c in s)


def parse_email_list(recipients):
    return [parse_email(r) for r in recipients]


# remove any names with non-ascii characters
def parse_email(r):
    if is_ascii(r):
        return r
    else:
        start = r.find('<')+1
        end = r.find('>')
        return r[start:end]

def create_gmail_message(sender, recipients, subject, message_text):
    """Create a message for an email.

    Args:
        sender: Email address of the sender.
        to: Email address of the receiver.
        subject: The subject of the email message.
        message_text: The text of the email message.

    Returns:
        An object containing a base64url encoded email object.
    """
    message = MIMEText(message_text)
    recipients = parse_email_list(recipients)
    message['To'] = ', '.join(recipients)
    message['From'] = parse_email(sender)
    message['Subject'] = subject
    return {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}


def send_gmail(service, user_id, message):
    """Send an email message.

    Args:
        service: Authorized Gmail API service instance.
        user_id: User's email address. The special value "me"
        can be used to indicate the authenticated user.
        message: Message to be sent.

    Returns:
        success and error messages.
    """
    success = None
    error = None
    try:
        message = (service.users().messages().send(userId=user_id, body=message)
                    .execute())
        print('Message Id: %s' % message['id'])
        success = "Email sent"
        return success, error
    except Exception as error:
        print('An error occurred: %s' % error)
        err = "Error sending email: " + str(error)
        return success, err


def get_threadid(uid):
    print('get_threadid')
    service, error = connect_to_gmail()
    if error:
        print(error)
        return None
    results = service.users().threads().list(userId='me').execute()
    threads = results.get('threads', [])

    for t in threads:
        thread = service.users().threads().get(userId='me', id=t['id']).execute()
        message = thread['messages'][0]
        raw_message = service.users().messages().get(userId='me', id=message['id'], format='raw').execute()
        msg_str = base64.urlsafe_b64decode(raw_message['raw'].encode('ASCII')).decode()
        mime_msg = email.message_from_string(msg_str.decode())
        subject = decode_header(mime_msg["Subject"])[0][0]
        if isinstance(subject, bytes):
            subject = subject.decode()
        substr = '[uid:%s]' % uid
        if subject.find(substr) >=0:
            return t['id']
    return None


@app.route('/NSF-ANT05-37143_datasets')
def genBank_datasets():
    genbank_url = "https://www.ncbi.nlm.nih.gov/nuccore/"
    (conn, cur) = connect_to_db()
    ds_query = "SELECT title FROM dif_data_url_map WHERE dif_id = 'NSF-ANT05-37143'"
    ds_query_string = cur.mogrify(ds_query)
    cur.execute(ds_query_string)
    title = cur.fetchone()
    datasets = re.split(', |\) |\n', title['title'])

    lines = []
    for ds in datasets:
        if ds.strip()[0] == "(":
            lines.append({'title': ds.replace("(", "")})
        else:
            lines.append({'title': ds, 'url': genbank_url + ds.replace(' ', '')})

    template_dict = {'lines': lines}
    return render_template("NSF-ANT05-37143_datasets.html", **template_dict)


@app.route('/getfeatureinfo')
def getfeatureinfo():
    if request.args.get('layers') != "":
        url = unquote(app.config['MAPSERVER_URL'] + urlencode(request.args))
        return requests.get(url).text
    return None


@app.route('/polygon')
def polygon_count():
    wkt = request.args.get('wkt')
    (conn, cur) = connect_to_db()
    query = cur.mogrify('''SELECT COUNT(*), program FROM (SELECT DISTINCT
           ds.id,
           apm.program_id as program
           FROM dataset_spatial_map dspm 
           JOIN dataset ds ON (dspm.dataset_id=ds.id)
           JOIN dataset_award_map dam ON (ds.id=dam.dataset_id)
           JOIN award_program_map apm ON (dam.award_id=apm.award_id)
           WHERE st_within(st_transform(dspm.geometry,3031),st_geomfromewkt('srid=3031;'||%s))) foo GROUP BY program''', (wkt,))
    cur.execute(query)
    return flask.jsonify(cur.fetchall())


@app.route('/titles')
def titles():
    term = request.args.get('term')
    (conn, cur) = connect_to_db()
    query_string = cur.mogrify('SELECT title FROM dataset WHERE title ILIKE %s', ('%' + term + '%',))
    cur.execute(query_string)
    rows = cur.fetchall()
    titles = []
    for r in rows:
        titles.append(r['title'])
    return flask.jsonify(titles)


@app.route('/geometries')
def geometries():
    (conn, cur) = connect_to_db()
    query = "SELECT st_asgeojson(st_transform(st_geomfromewkt('srid=4326;' || 'POLYGON((' || west || ' ' || south || ', ' || west || ' ' || north || ', ' || east || ' ' || north || ', ' || east || ' ' || south || ', ' || west || ' ' || south || '))'),3031)) as geom FROM dataset_spatial_map;"
    cur.execute(query)
    return flask.jsonify([row['geom'] for row in cur.fetchall()])


@app.route('/parameter_search')
def parameter_search():
    (conn, cur) = connect_to_db()
    expr = '%' + request.args.get('term') + '%'
    query = cur.mogrify("SELECT id FROM parameter WHERE category ILIKE %s OR topic ILIKE %s OR term ILIKE %s OR varlev1 ILIKE %s OR varlev2 ILIKE %s OR varlev3 ILIKE %s", (expr, expr, expr, expr, expr, expr))
    cur.execute(query)
    return flask.jsonify([row['id'] for row in cur.fetchall()])

# @app.route('/test_autocomplete')
# def test_autocomplete():
#     return '\n'.join([render_template('header.html'),
#                       render_template('test_autocomplete.html'),
#                       render_template('footer.html')])


@app.route('/dataset_json/<dataset_id>')
def dataset_json(dataset_id):
    return flask.jsonify(get_datasets([dataset_id]))


@app.route('/project_json/<project_id>')
def project_json(project_id):
    return flask.jsonify(get_project(project_id))


@app.route('/platforms_json')
def platforms_json():
    return flask.jsonify(get_gcmd_platforms())


def isNsfFunder(funders, award):
    nsf_dois = ['10.13039/100000001', '10.13039/100000087', '10.13039/100000162', '10.13039/100007352', '10.13039/100006447']
    nsf_names = ['National Science Foundation', 'NSF', 'Polar', 'Antarctic', 'Ice Sheets', 'WAIS', 'LTER', 'Southern Ocean',
                 'National Stroke Foundation', 'National Sleep Foundation']

    award_dash = award[0:2] + "-" + award[2:]
    for funder in funders:
        if funder.get('DOI') and funder['DOI'] in nsf_dois and funder.get('award') and (award in funder['award'] or award_dash in funder['award']): 
           return True
        if funder.get('name') and any(n in funder['name'] for n in nsf_names) and funder.get('award') and (award in funder['award'] or award_dash in funder['award']):
           return True

    return False


def crossref2ref_text(item):
    # probably not needed - all publications appear to have a DOI that works with the API
    ref_text = ""
    if item.get('author'):
        for author in item['author']:
            first_names = author['given'].split(' ')
            initials = ""
            for name in first_names:
                initials += "%s." % name[0]

            ref_text += "%s, %s" % (author['family'], initials)

    year = ""
    if item.get('published_online'):
        year = item['published_online']['date-parts'][0][0]
    elif item.get('published_print'):
        year = item['published_print']['date-parts'][0][0]
    elif item.get('created'):
        year = item['created']['date-parts'][0][0]
    
    if year != "":
        ref_text += "(%s). " % year

    if item.get('title'):
        ref_text += " %s." % item['title'][0]

    if item.get('container-title'):
        ref_text += " %s" % item['container-title'][0]

    if item.get('volume'):
        ref_text += ", %s" % item['volume']

    if item.get('issue'):
        ref_text += "(%s)" % item['issue']

    if item.get('page'):
        ref_text += ", %s" % item['page']

    ref_text += "."

    print(("*****REF_TEXT GENERATED FROM CROSSREF:\n%s") % ref_text)

    return ref_text


@app.route('/crossref_pubs', methods=['POST'])
def crossref_pubs():

    api = 'https://api.crossref.org/works?filter=funder:10.13039/100000001,funder:10.13039/100000087,award.number:'
    bib_api = 'https://api.crossref.org/works/%s/transform/text/x-bibliography'

    awards = request.form.getlist('awards[]')
    pubs = []
    if awards:
        for award_id in awards:
            if award_id != "None": 

                r = requests.get(api + award_id).json()
                items = r['message']['items']
                # for each publication, see if it has a DOI
                for item in items:
                    if not isNsfFunder(item.get('funder'), award_id):
                        continue
                    ref_doi = item.get('DOI')
                    if ref_doi and ref_doi != '':
                        # use the DOI to get the cite-as text from the x-bibliography API
                        bib_url = bib_api % ref_doi
                        r_bib = requests.get(bib_url)
                        if r_bib.status_code == 200 and r_bib.content:
                            # split off the DOI in the cite-as string, as we don't need in our ref_text
                            ref_text = r_bib.content.decode().rsplit(' doi', 1)[0]
                        else:
                            # if x-bibliography API doesn't return anything, generate ref_text from what we have in crossref
                            ref_text = crossref2ref_text(item)
                    else:
                        # if no DOI, generate ref_text from what we have in crossref
                        ref_text = crossref2ref_text(item)

                    pub = {'doi': ref_doi, 'ref_text': ref_text}
                    pubs.append(pub)

    return flask.jsonify(pubs)


@app.route('/stats', methods=['GET'])
def stats():
    args = request.args.to_dict()

    template_dict = {}
    date_fmt = "%Y-%m-%d"
    if args.get('start_date'):
        start_date = datetime.strptime(args['start_date'], date_fmt)
    else:
        start_date = datetime.strptime('2011-01-01', date_fmt)
    if args.get('end_date'):
        end_date = datetime.strptime(args['end_date'], date_fmt)
    else:
        end_date = datetime.now()
    if args.get('exclude'):
        exclude = args.get('exclude')
    else:
        exclude = False

    template_dict['start_date'] = start_date
    template_dict['end_date'] = end_date
    template_dict['exclude'] = exclude
    proj_catalog_date = dt_date(2019,5,1)
    template_dict['proj_catalog_date'] = proj_catalog_date
    template_dict['download_stats_date'] = dt_date(2017,3,1)
    template_dict['ext_tracker_date'] = dt_date(2022,2,1)

    # get download information from the database
    (conn, cur) = connect_to_db()
    query = "SELECT time, resource_size, remote_host, resource_requested FROM access_logs_downloads WHERE time >= '%s' AND time <= '%s';" % (start_date, end_date)
    cur.execute(query)
    data = cur.fetchall()
    downloads = {}
    for row in data:
        time = row['time']
        month = "%s-%02d-01" % (time.year, time.month)  
        bytes = row['resource_size']
        user = row['remote_host']
        resource = row['resource_requested']
        user_file = "%s-%s" % (user, resource) 
        if downloads.get(month):
            downloads[month]['bytes'] += bytes
            downloads[month]['num_files'] += 1 
            downloads[month]['users'].add(user)
            downloads[month]['user_files'].add(user_file)
        else:
            downloads[month] = {'bytes': bytes, 'num_files': 1, 'users': {user}, 'user_files': {user_file}}

    # ftp downloads of large datasets
    if not exclude :
        query = "SELECT date, resource_size, request_email, dataset_id, file_count FROM access_ftp_downloads WHERE date >= '%s' AND date <= '%s';" % (start_date, end_date)
        cur.execute(query)
        data = cur.fetchall()
        for row in data:
            date = row['date'].split('-')
            month = "%s-%s-01" % (date[0], date[1])
            bytes = row['resource_size']
            user = row['request_email']
            resource = row['dataset_id']
            user_file = "%s-%s" % (user, resource) 
            if downloads.get(month):
                downloads[month]['bytes'] += bytes
                downloads[month]['num_files'] += row['file_count']
                downloads[month]['users'].add(user)
                downloads[month]['user_files'].add(user_file)
            else:
                downloads[month] = {'bytes': bytes, 'num_files': row['file_count'], 'users': {user}, 'user_files': {user_file}}

    download_numfiles_bytes = []
    download_users_downloads = []
    downloads_total = 0
    download_users_total = 0
    download_files_total = 0
    download_size_total = 0
    months_list = sorted(downloads)
    for month in months_list:
        download_numfiles_bytes.append([month, downloads[month]['num_files'], downloads[month]['bytes']])
        download_files_total += downloads[month]['num_files']
        download_size_total += downloads[month]['bytes']
        download_users_downloads.append([month, len(downloads[month]['users']), len(downloads[month]['user_files'])])
        downloads_total += len(downloads[month]['user_files'])
        download_users_total += len(downloads[month]['users'])
    template_dict['download_numfiles_bytes'] = download_numfiles_bytes
    template_dict['download_users_downloads'] = download_users_downloads
    template_dict['downloads_total'] = "{:,}".format(downloads_total)
    template_dict['download_users_total'] = "{:,}".format(download_users_total)
    template_dict['download_files_total'] = "{:,}".format(download_files_total)
    template_dict['download_size_total'] = human_size(download_size_total)

    # get project views from the database
    query = "SELECT * FROM access_views_projects_matview WHERE time >= '%s' AND time <= '%s';" % (start_date, end_date)
    cur.execute(query)
    data = cur.fetchall()
    proj_views = {}
    tracker = {}
    blocked_hosts = set()
    for row in data:
        if row['country'].lower() in ['china', 'russia', 'ukraine', 'ru']: continue
        if row['time'].date() < proj_catalog_date: continue
        host = row['remote_host']
        time = row['time']
        month = "%s-%02d-01" % (time.year, time.month)  

        # try and weed out bots by blocking any IPs that view 100 or more projects on a single day
        day = "%s-%02d-%02d" % (time.year, time.month, time.day)
        host_day = '%s_%s' % (host, day)
        if tracker.get(host_day):
            tracker[host_day] += 1
            if tracker[host_day] == 100:
                blocked_hosts.add(host)
        else:
            tracker[host_day] = 1

        if proj_views.get(month):
            if proj_views[month].get(host):
                proj_views[month][host] += 1
            else:
               proj_views[month][host] = 1 
        else:
            proj_views[month] = {host:1}

    num_project_views = []
    project_views_total = 0
    months_list = sorted(proj_views)
    for month in months_list:
        num_month_views = 0
        for host in list(proj_views[month].keys()):
            if host not in blocked_hosts:
                num_month_views += proj_views[month][host]
                project_views_total += proj_views[month][host]
        
        num_project_views.append([month, num_month_views])
    template_dict['num_project_views'] = num_project_views
    template_dict['project_views_total'] = "{:,}".format(project_views_total)

  # get clicks to external datasets from the database
    query = "SELECT resource_requested, remote_host, country, time FROM access_logs_external WHERE resource_requested ~* 'type=dataset' AND time >= '%s' AND time <= '%s' ORDER BY time;" % (start_date, end_date)
    cur.execute(query)
    data = cur.fetchall()
    ext_clicks = {}
    tracker = {}
    blocked_hosts = set()
    for row in data:
        if row['country'].lower() in ['china', 'russia', 'ukraine', 'ru']: continue
        host = row['remote_host']
        time = row['time']
        month = "%s-%02d-01" % (time.year, time.month)  

        # try and weed out bots by blocking any IPs that make 100 or more referrals on a single day
        day = "%s-%02d-%02d" % (time.year, time.month, time.day)
        host_day = '%s_%s' % (host, day)
        if tracker.get(host_day):
            tracker[host_day] += 1
            if tracker[host_day] == 100:
                blocked_hosts.add(host)
        else:
            tracker[host_day] = 1

        if ext_clicks.get(month):
            if ext_clicks[month].get(host):
                ext_clicks[month][host] += 1
            else:
               ext_clicks[month][host] = 1 
        else:
            ext_clicks[month] = {host:1}

    num_ext_clicks = []
    ext_clicks_total = 0
    months_list = sorted(ext_clicks)
    for month in months_list:
        num_month_views = 0
        for host in list(ext_clicks[month].keys()):
            if host not in blocked_hosts:
                num_month_views += ext_clicks[month][host]
                ext_clicks_total += ext_clicks[month][host]
        
        num_ext_clicks.append([month, num_month_views])
    template_dict['num_ext_clicks'] = num_ext_clicks
    template_dict['ext_clicks_total'] = "{:,}".format(ext_clicks_total)

    # get search information from the database
    if cf.isCurator():
        query = "SELECT resource_requested FROM access_logs_searches WHERE time >= '%s' AND time <= '%s';" % (start_date, end_date)
        cur.execute(query)
        data = cur.fetchall()
        searches = {'repos': {}, 'sci_progs': {}, 'nsf_progs': {}, 'persons': {}, 'awards': {}, 'free_texts': {}, 'titles': {}}
        params = {'repo': 'repos', 'sci_program': 'sci_progs', 'nsf_program': 'nsf_progs', 'person': 'persons', 'award': 'awards', 
                'free_text': 'free_texts', 'dp_title': 'titles'}
        for row in data:
            resource = row['resource_requested']
            search = parseSearch(resource)

            for search_param, searches_param in list(params.items()):
                searches = binSearch(search, searches, search_param, searches_param)

        template_dict['searches'] = searches

    # get referer information from the database
    if cf.isCurator():
        query = "SELECT country, referer, remote_host, resource_requested, time FROM access_logs_referers WHERE time >= '%s' AND time <= '%s';" % (start_date, end_date)
        cur.execute(query)
        data = cur.fetchall()
        pages = {'.all': {'All': {'count':0}}, '/': {'All': {'count': 0}} ,'/view/dataset': {'All': {'count': 0}}, '/view/project': {'All': {'count': 0}}, 
                '/submit': {'All': {'count': 0}}, '/readme': {'All': {'count': 0}}, '/search': {'All': {'count': 0}}, 
                '/contact': {'All': {'count': 0}}, '/faq': {'All': {'count': 0}}, '/dataset_search': {'All': {'count': 0}}, 
                '/dataset': {'All': {'count': 0}}, '/news': {'All': {'count': 0}}, '/api':{'All': {'count': 0}}}

        tracker = {}
        blocked_hosts = set()
        for row in data:
            host = row['remote_host']
            time = row['time']
            month = "%s-%02d-01" % (time.year, time.month)  

            # try and weed out bots by blocking any IPs that view 100 or more pages on a single day
            day = "%s-%02d-%02d" % (time.year, time.month, time.day)
            host_day = '%s_%s' % (host, day)
            if tracker.get(host_day):
                tracker[host_day] += 1
                if tracker[host_day] == 200:
                    blocked_hosts.add(host)
            else:
                tracker[host_day] = 1

        for row in data:
            if row['country'].lower() in ['china', 'russia', 'ukraine', 'ru']: continue
            if row['remote_host'] in blocked_hosts: continue
            referer = row['referer']
            if referer.endswith('/'): referer = referer[:-1]
            referer_domain = urlparse(referer).netloc
            if referer_domain.endswith('.ru') or 'baidu' in referer: continue
            # if 'google' in referer_domain: referer_domain = 'www.google.com'
            resource = row['resource_requested']
            page = resource.split('?')[0]
            if '/view/dataset' in page: page = '/view/dataset'
            elif '/view/project' in page: page = '/view/project'
            elif '/edit/dataset' in page: page = '/edit/dataset'
            elif '/edit/project' in page: page = '/edit/project'
            elif '/submit' in page: page = '/submit'
            elif '/readme' in page: page = '/readme'
            elif '/login' in page: continue
            elif '/curator' in page: continue
            elif '/search_result' in page: continue
            elif '/news' in page: page = '/news'
            elif '/dataset_search' in page: page = '/dataset_search'
            elif '/api' in page: page = '/api'
            elif page in ['/home', '/index']: page = '/'
            elif page.startswith('/dataset/ldeo') or page.startswith('/dataset/usap-dc') or page.startswith('/dataset/nsidc'): page = '/dataset'
            if page not in pages:
                pages[page] =  {'All': {'count': 0}}
            if referer_domain not in pages[page]['All']:
                pages[page]['All'][referer_domain] = 1       
            else:
                pages[page]['All'][referer_domain] += 1

            if referer_domain not in pages['.all']['All']:
                pages['.all']['All'][referer_domain] = 1
            else:
                pages['.all']['All'][referer_domain] += 1

            resource_referer = resource + '_' + referer
            if resource_referer not in pages[page]:
                pages[page][resource_referer] = {'resource': resource, 'referer': referer, 'count': 1}
            else:
                pages[page][resource_referer]['count'] += 1

            if resource_referer not in pages['.all']:
                pages['.all'][resource_referer] = {'resource': resource, 'referer': referer, 'count': 1}
            else:
                pages['.all'][resource_referer]['count'] += 1

        template_dict['referers'] = pages

    else:
       template_dict['referers'] = {}
    
    # get submission information from the database
    query = cur.mogrify('''SELECT dsf.*, d.date_created::text, dif.date_created::text AS dif_date FROM dataset_file_info dsf 
            JOIN dataset d ON d.id = dsf.dataset_id
            LEFT JOIN dif ON d.id_orig = dif.dif_id;''') 
    cur.execute(query)
    data = cur.fetchall()

    submission_bytes = 0
    submission_num_files = 0
    submissions_total = 0

    q_dates = pd.date_range(start_date,end_date,freq='Q')
    end_dt = pd.to_datetime(end_date)
    quarters = ['%s-Q%s' % (q.year, q.quarter) for q in q_dates]
    end_q = '%s-Q%s' % (end_dt.year, end_dt.quarter)
    if end_q not in quarters:
        quarters.append(end_q)

    submissions = {q: {'submissions': set()} for q in quarters}
    for row in data:
        if row['dif_date'] is not None:
            date = row['dif_date']
        else:
            date = row['date_created']
        if date is None or date.count("-") != 2:
            # if only a year is given, make the date Dec 1st of that year
            if len(date) == 4:
                date += "-12-01"
            else:
                continue 
        # throw out any rows not in the chosen date range
        if (datetime.strptime(date, date_fmt) < start_date or
           datetime.strptime(date, date_fmt) > end_date):
            continue
        date = pd.to_datetime(date)
        qt = "%s-Q%s" % (date.year, pd.Timestamp(date).quarter)
        bytes = row['file_size_uncompressed'] if row.get('file_size_uncompressed') else row['file_size_on_disk']
        num_files = row['file_count']
        submission = row['dataset_id']
        submission_bytes += bytes
        submission_num_files += num_files
        submissions[qt]['submissions'].add(submission)

    submission_submissions = []
    quarters_list = sorted(submissions)
    for qt in quarters_list:
        submission_submissions.append([qt, len(submissions[qt]['submissions'])])
        submissions_total += len(submissions[qt]['submissions'])
    template_dict['submission_size'] = human_size(submission_bytes)
    template_dict['submission_num_files'] = "{:,}".format(submission_num_files)
    template_dict['submissions_total'] = "{:,}".format(submissions_total)
    template_dict['submission_submissions'] = submission_submissions
    template_dict['download_numbers'] = getDownloadsForDatasets(start_date, end_date)

    query = cur.mogrify('''SELECT * from project WHERE date_created BETWEEN '%s' AND '%s';''' % (start_date, end_date)) 
    cur.execute(query)
    data = cur.fetchall()

    projects_total = 0
    projects = {q:{'before':0, 'after':0} for q in quarters}
    for row in data:
        date = row['date_created']
        qt = "%s-Q%s" % (date.year, pd.Timestamp(date).quarter)
        if date < proj_catalog_date:
            projects[qt]['before'] += 1
        else: 
            projects[qt]['after'] += 1
        projects_total += 1

    projects_created = []
    quarters_list = sorted(projects)
    cumulative = 0
    for qt in quarters_list:
        cumulative += projects[qt]['before'] + projects[qt]['after']
        projects_created.append([qt, 
                                 projects[qt]['before'] if projects[qt]['before'] != 0 else None, 
                                 projects[qt]['after'] if projects[qt]['after'] != 0 else None,
                                 cumulative
                                ])
    template_dict['projects_created'] = projects_created
    template_dict['projects_total'] = "{:,}".format(projects_total)

    return render_template('statistics.html', **template_dict) 


def binSearch(search, searches, search_param, searches_param):
    if search.get(search_param) and search[search_param] != '':
        search[search_param] = unquote(search[search_param]).replace('+', ' ').replace('%2C', ',')
        if search_param == 'free_text':
            search[search_param] = search[search_param].lower()
            # some search end with slashes for some reason
            while search[search_param].endswith('/'): 
                search[search_param] = search[search_param][:-1]

        if search_param == 'dp_title' or search_param == 'sci_program':
            # make binning case insensitive for sci prog and title
            s_lower = {k.lower():k for k in list(searches[searches_param].keys())}
            param_lower = search[search_param].lower()
            if s_lower.get(param_lower):
                searches[searches_param][s_lower[param_lower]] += 1
            else:
                searches[searches_param][search[search_param]] = 1
        else:
            if searches[searches_param].get(search[search_param]):
                searches[searches_param][search[search_param]] += 1
            else:
                searches[searches_param][search[search_param]] = 1
    return searches


def getDownloadsForDatasets(start_date, end_date):
    (conn, cur) = connect_to_db()

    query = '''SELECT id, title, creator, release_date, SUM(num_downloads) AS count FROM (               
                    SELECT d.*, COUNT(afd.*) as num_downloads
                         FROM dataset d JOIN access_ftp_downloads afd ON 
                         d.id=afd.dataset_id
                         WHERE date >= '%s' AND date <= '%s'
                         GROUP BY d.id
                     UNION ALL
                     SELECT d.*,COUNT(DISTINCT(remote_host,TO_CHAR(time,'YY-MM-DD'))) AS num_downloads
                          FROM dataset d JOIN access_logs_downloads ald ON d.id=SUBSTR(ald.resource_requested,18,6)
                          WHERE resource_requested !~ 'Please_contact_us_to_arrange_for_download.txt' 
                          AND resource_requested !~ 'image_file_list'
                          AND time >= '%s' AND time <= '%s'
                          GROUP BY d.id
                   ) downloads GROUP BY id, title, creator, release_date
                   ORDER BY count DESC, id ASC;''' % (start_date, end_date, start_date, end_date)
    cur.execute(query)
    res = cur.fetchall()  
    return res


def parseSearch(resource):
    if '?' not in resource:
        return {}
    resource = resource.split('?')[1]
    filters = resource.split('&')
    search = {}
    for f in filters:
        try:
            filter, value = f.split('=', 1)
            value = unquote(value).replace('%20', ' ').replace('+', ' ')
            search[filter] = value 
        except:
            continue
    return search


def human_size(bytes, units=[' bytes','KB','MB','GB','TB', 'PB', 'EB']):
    """ Returns a human readable string representation of bytes """
    return str(bytes) + units[0] if bytes < 1024 else human_size(bytes>>10, units[1:])


@app.route('/view/project/<project_id>')
def project_landing_page(project_id):
    metadata = get_project(project_id)
    if metadata is None:
        return redirect(url_for('not_found'))

    metadata['excluded_keywords'] = ['AMD', 'AMD/US', 'USA/NSF', 'USAP-DC', 'NSIDC', 'Not provided', 'NOT APPLICABLE']

    # add paleo times to keywords list
    if metadata.get('gcmd_paleo_time'):
        for pt in metadata['gcmd_paleo_time']:
            metadata['keywords'] += '; ' + '; '.join(pt['paleo_time']['id'].split(' > '))

    # get platform and instrument keywords
    if metadata.get('gcmd_platforms'):
        plat_inst_keywords = []
        for platform in metadata['gcmd_platforms']:
            if platform['short_name'] != 'Not provided':
                print(platform['short_name'])
                if platform['short_name'] != '':
                    plat_inst_keywords.append(platform['short_name'])
                else:
                    plat_inst_keywords.append(platform['id'].split(' > ')[-1])
            if platform.get('gcmd_instruments'):
                for instr in platform['gcmd_instruments']:
                    if instr['short_name'] != '':
                        plat_inst_keywords.append(instr['short_name'])
                    else:
                        plat_inst_keywords.append(instr['id'].split(' > ')[-1])
        metadata['plat_inst_keywords'] = plat_inst_keywords
            

    # make a list of all the unique Data Management Plans
    dmps = set()
    if metadata.get('funding'):
        for f in metadata['funding']:
            if f.get('dmp_link') and f['dmp_link'] != 'None':
                dmps.add(f['dmp_link'])
        if len(dmps) == 0:
            dmps = None
    metadata['dmps'] = dmps

    # get CMR/GCMD URLs for dif records
    getCMRUrls(metadata['dif_records'])

    # get count of how many times this dataset has been downloaded
    metadata['views'] = getProjectViews(project_id)

    return render_template('project_landing_page.html', data=metadata, contact_email=app.config['USAP-DC_GMAIL_ACCT'])


@app.route('/data_management_plan', methods=['POST'])
def data_management_plan():
    if request.method != 'POST':
        return redirect(url_for('not_found'))

    dmp_link = request.form.get('dmp_link')
    if dmp_link.startswith('/'):
        dmp_link = dmp_link[1:]
    fullpath = os.path.normpath(os.path.join(current_app.root_path, dmp_link))

    try:
        if not validate_dmp_link(dmp_link) or not fullpath.startswith(current_app.root_path):
            return redirect(url_for('not_found'))
        return send_file(fullpath, attachment_filename=os.path.basename(dmp_link))
    except:
        return redirect(url_for('not_found'))


def validate_dmp_link(dmp_link):
    if "../" in dmp_link:
        return False

    (conn, cur) = connect_to_db()
    query = "SELECT count(*) FROM award where dmp_link ~* %s;"
    cur.execute(query, (dmp_link,))
    res = cur.fetchone()
    if res['count'] > 0:
        return True
    return False
    

def get_project(project_id):
    if project_id is None:
        return None
    else:
        (conn, cur) = connect_to_db()
        query_string = cur.mogrify('''SELECT *
                        FROM
                        project p
                        LEFT JOIN (
                            SELECT pam.proj_uid AS a_proj_uid, json_agg(json_build_object('program',prog.id, 'award',a.award ,'dmp_link', a.dmp_link, 
                            'is_main_award', pam.is_main_award, 'is_previous_award', pam.is_previous_award, 'pi_name', a.name, 'title', a.title,
                            'abstract', a.sum)) funding
                            FROM project_award_map pam 
                            JOIN award a ON a.award=pam.award_id
                            LEFT JOIN award_program_map apm ON apm.award_id=a.award
                            LEFT JOIN program prog ON prog.id=apm.program_id
                            WHERE a.award != 'XXXXXXX'
                            GROUP BY pam.proj_uid
                        ) a ON (p.proj_uid = a.a_proj_uid)
                        LEFT JOIN (
                            SELECT pperm.proj_uid AS per_proj_uid, json_agg(json_build_object('role', pperm.role ,'id', per.id, 'name_last', per.last_name, 
                            'name_first', per.first_name, 'org', per.organization, 'email', per.email, 'orcid', per.id_orcid)
                            ) persons
                            FROM project_person_map pperm JOIN person per ON (per.id=pperm.person_id)
                            GROUP BY pperm.proj_uid
                        ) per ON (p.proj_uid = per.per_proj_uid)
                        LEFT JOIN (
                            SELECT pdifm.proj_uid AS dif_proj_uid, json_agg(dif) dif_records
                            FROM project_dif_map pdifm JOIN dif ON (dif.dif_id=pdifm.dif_id)
                            GROUP BY pdifm.proj_uid
                        ) dif ON (p.proj_uid = dif.dif_proj_uid)
                        LEFT JOIN (
                            SELECT pim.proj_uid AS init_proj_uid, json_agg(init) initiatives
                            FROM project_initiative_map pim JOIN initiative init ON (init.id=pim.initiative_id)
                            GROUP BY pim.proj_uid
                        ) init ON (p.proj_uid = init.init_proj_uid)
                        LEFT JOIN (
                            SELECT prefm.proj_uid AS ref_proj_uid, json_agg(ref) reference_list
                            FROM project_ref_map prefm JOIN reference ref ON (ref.ref_uid=prefm.ref_uid)
                            GROUP BY prefm.proj_uid
                        ) ref ON (p.proj_uid = ref.ref_proj_uid)
                        LEFT JOIN (
                            SELECT pdm.proj_uid AS ds_proj_uid, json_agg(dataset) datasets
                            FROM project_dataset_map pdm JOIN project_dataset dataset ON (dataset.dataset_id=pdm.dataset_id)
                            GROUP BY pdm.proj_uid
                        ) dataset ON (p.proj_uid = dataset.ds_proj_uid)
                        LEFT JOIN (
                            SELECT pw.proj_uid AS website_proj_uid, json_agg(pw) website
                            FROM project_website pw
                            GROUP BY pw.proj_uid
                        ) website ON (p.proj_uid = website.website_proj_uid)
                        LEFT JOIN (
                            SELECT pdep.proj_uid AS dep_proj_uid, json_agg(pdep) deployment
                            FROM project_deployment pdep
                            GROUP BY pdep.proj_uid
                        ) deployment ON (p.proj_uid = deployment.dep_proj_uid)
                         LEFT JOIN (
                            SELECT pf.proj_uid AS f_proj_uid, json_agg(pf) feature
                            FROM project_feature pf
                            GROUP BY pf.proj_uid
                        ) feature ON (p.proj_uid = feature.f_proj_uid)                       
                        LEFT JOIN (
                            SELECT psm.proj_uid AS sb_proj_uid, json_agg(psm) spatial_bounds
                            FROM project_spatial_map psm
                            GROUP BY psm.proj_uid
                        ) sb ON (p.proj_uid = sb.sb_proj_uid)
                        LEFT JOIN (
                            SELECT pgskm.proj_uid AS param_proj_uid, json_agg(gsk) parameters
                            FROM project_gcmd_science_key_map pgskm JOIN gcmd_science_key gsk ON (gsk.id=pgskm.gcmd_key_id)
                            GROUP BY pgskm.proj_uid
                        ) parameters ON (p.proj_uid = parameters.param_proj_uid)
                        LEFT JOIN (
                            SELECT proj_uid AS loc_proj_uid, json_agg(keyword_label) locations
                                FROM vw_project_location vdl
                                GROUP BY proj_uid
                        ) locations ON (p.proj_uid = locations.loc_proj_uid)
                        LEFT JOIN (
                            SELECT pglm.proj_uid AS gcmd_loc_proj_uid, json_agg(gl) gcmd_locations
                            FROM project_gcmd_location_map pglm JOIN gcmd_location gl ON (gl.id=pglm.loc_id)
                            GROUP BY pglm.proj_uid
                        ) gcmd_locations ON (p.proj_uid = gcmd_locations.gcmd_loc_proj_uid)
                        LEFT JOIN (
                            SELECT gpi.proj_uid AS gcmd_plat_proj_uid, json_agg(gpi) gcmd_platforms
                            FROM 
                            (
                                SELECT * FROM project_gcmd_platform_map pgpm
                                JOIN gcmd_platform gp ON (gp.id=pgpm.platform_id)
                                LEFT JOIN (
                                    SELECT pgpim.proj_uid as gcmd_inst_proj_uid, pgpim.platform_id as gcmd_plat_proj_uid, json_agg(gi) gcmd_instruments
                                    FROM project_gcmd_platform_instrument_map pgpim JOIN gcmd_instrument gi ON (gi.id=pgpim.instrument_id)
                                    GROUP BY pgpim.proj_uid, pgpim.platform_id
                                ) gcmd_instruments 
                                ON pgpm.proj_uid = gcmd_instruments.gcmd_inst_proj_uid AND pgpm.platform_id = gcmd_instruments.gcmd_plat_proj_uid
                            ) gpi
                            GROUP BY gpi.proj_uid
                        ) gcmd_platforms ON (p.proj_uid = gcmd_platforms.gcmd_plat_proj_uid)
                        LEFT JOIN (
                            SELECT pgptm.proj_uid AS gcmd_paleo_proj_uid, json_agg(json_build_object('paleo_time', gpt, 'paleo_start_date', paleo_start_date,
                                'paleo_stop_date', paleo_stop_date)) gcmd_paleo_time
                                FROM project_gcmd_paleo_time_map pgptm JOIN gcmd_paleo_time gpt ON (gpt.id=pgptm.paleo_time_id)
                                GROUP BY pgptm.proj_uid
                        ) gcmd_paleo_times ON (p.proj_uid = gcmd_paleo_times.gcmd_paleo_proj_uid)
                        LEFT JOIN ( 
                            SELECT k_1.proj_uid AS kw_proj_uid, string_agg(k_1.keywords, '; '::text) AS keywords
                            FROM (SELECT pskm.proj_uid, reverse(split_part(reverse(pskm.gcmd_key_id), ' >'::text, 1)) AS keywords
                                  FROM project_gcmd_science_key_map pskm
                                  UNION
                                  -- SELECT plm.proj_uid, reverse(split_part(reverse(plm.loc_id), ' >'::text, 1)) AS keywords
                                  -- FROM project_gcmd_location_map plm
                                  -- UNION
                                  SELECT pim.proj_uid, reverse(split_part(reverse(pim.gcmd_iso_id), ' >'::text, 1)) AS keywords
                                  FROM project_gcmd_isotopic_map pim
                                  UNION
                                  -- SELECT ppm.proj_uid, reverse(split_part(reverse(ppm.platform_id), ' >'::text, 1)) AS keywords
                                  -- FROM project_gcmd_platform_map ppm
                                  -- UNION
                                  SELECT pkm.proj_uid, ku.keyword_label AS keywords
                                  FROM project_keyword_map pkm JOIN keyword_usap ku ON (ku.keyword_id=pkm.keyword_id)
                                  UNION
                                  SELECT pkm.proj_uid, ki.keyword_label AS keywords
                                  FROM project_keyword_map pkm JOIN keyword_ieda ki ON (ki.keyword_id=pkm.keyword_id) 
                                  ) k_1
                            GROUP BY k_1.proj_uid
                        ) keywords ON keywords.kw_proj_uid = p.proj_uid
                        LEFT JOIN ( 
                            SELECT k_1.proj_uid AS kw_proj_uid, string_agg(k_1.keyword_label, '; '::text) AS aux_keywords
                            FROM (
                                  SELECT pkm.proj_uid, ku.keyword_label, ku.keyword_id
                                  FROM project_keyword_map pkm JOIN keyword_usap ku ON (ku.keyword_id=pkm.keyword_id)
                                  WHERE ku.keyword_id NOT IN (SELECT keyword_id FROM vw_location)
                                  ) k_1
                            GROUP BY k_1.proj_uid
                        ) aux_keywords ON aux_keywords.kw_proj_uid = p.proj_uid
                        LEFT JOIN (
                            SELECT pdm.proj_uid AS df_proj_uid, pd.data_format FROM project_dataset pd
                            JOIN project_dataset_map pdm on pdm.dataset_id = pd.dataset_id
                        ) data_format ON (p.proj_uid = data_format.df_proj_uid)
                        LEFT JOIN (
                            SELECT pl.id AS product_level_id, pl.description AS product_level_description FROM product_level pl
                        ) product_level ON p.product_level_id = product_level.product_level_id
                        WHERE p.proj_uid = '%s' ORDER BY p.title''' % project_id)
        cur.execute(query_string)
        return cur.fetchone()


@app.route('/search', methods=['GET'])
def search():
    template_dict = {}

    params = request.args.to_dict()
    params['dp_type'] = 'Project'

    template_dict['show_map'] = False
    if params.get('show_map'):
        template_dict['show_map'] = params.pop('show_map')
    
    # block searches made by bots
    checkHoneypot(params, 'There was an issue with your search, please try again.', url_for('search'))
    params.pop('email', None)
    params.pop('name', None)

    rows = filter_datasets_projects(**params)
    session['search_params'] = params

    for row in rows:
        if row['datasets']:
            items = json.loads(row['datasets'])
            # if row['type'] == 'Project':
            row['datasets'] = items
            # elif row['type'] == 'Dataset':
            #     row['projects'] = items\
            if type(items) is dict:
                row['repo'] = items['repository']
                row['datasets'] = [items]
            elif type(items) is list and len(items) > 0:
                row['repo'] = items[0]['repository']                         
    template_dict['records'] = rows

    template_dict['search_params'] = session.get('search_params')
    return render_template('search.html', **template_dict)  


@app.route('/dataset_search', methods=['GET'])
def dataset_search():
    template_dict = {}

    params = request.args.to_dict()

    template_dict['show_map'] = False
    if params.get('show_map'):
        template_dict['show_map'] = params.pop('show_map')

    # block searches made by bots
    checkHoneypot(params, 'There was an issue with your search, please try again.', url_for('search'))
    params.pop('email', None)
    params.pop('name', None)

    params['dp_type'] = 'Dataset'
    rows = filter_datasets_projects(**params)
    session['search_params'] = params

    for row in rows:
        if row['projects']:
            items = json.loads(row['projects'])
            row['projects'] = items
            if type(items) is dict:
                row['repo'] = items['repository']
                row['projects'] = [items]
            elif type(items) is list and len(items) > 0:
                row['repo'] = items[0]['repository']                 
    template_dict['records'] = rows

    template_dict['search_params'] = session.get('search_params')
    return render_template('dataset_search.html', **template_dict)  


@app.route('/filter_joint_menus', methods=['GET'])
def filter_joint_menus():
    keys = ['person', 'free_text', 'sci_program', 'award', 'dp_title', 'nsf_program', 'spatial_bounds_interpolated', 'dp_type', 'repo', 'location']
    params = request.args.to_dict()
    # if reseting:
    if params == {}:
        session['search_params'] = {}

    dp_titles = filter_datasets_projects(**{k: params.get(k) for k in keys if k != 'dp_title'})
    titles = set()
    for d in dp_titles:
        if d['title']:
            titles.add(d['title'])
        if d.get('dataset_titles'):
            ds_titles = d['dataset_titles'].split(';')
            for ds in ds_titles:
                if ds:
                    titles.add(ds)
        if d.get('project_titles'):
            p_titles = d['project_titles'].split(';')
            for p in p_titles:
                if p:
                    titles.add(p)
  
    dp_persons = filter_datasets_projects(**{k: params.get(k) for k in keys if k != 'person'})
    persons = set()
    for d in dp_persons:
        if d['persons']:
            for p in d['persons'].split(';'):
                persons.add(p.strip())

    dp_sci_programs = filter_datasets_projects(**{k: params.get(k) for k in keys if k != 'sci_program'})
    sci_programs = set()
    for d in dp_sci_programs:
        if d['science_programs']:
            for p in d['science_programs'].split(';'):
                sci_programs.add(p.strip())
        # include dataset science programs in project search
        if d.get('datasets') and d['uid']:
            for ds in json.loads(d['datasets']):
                if ds.get('science_program'): sci_programs.add(ds['science_program'])

    dp_nsf_programs = filter_datasets_projects(**{k: params.get(k) for k in keys if k != 'nsf_program'})
    nsf_programs = set()
    for d in dp_nsf_programs:
        if d['nsf_funding_programs']:
            for p in d['nsf_funding_programs'].split(';'):
                nsf_programs.add(p.strip())

    dp_awards = filter_datasets_projects(**{k: params.get(k) for k in keys if k != 'award'})
    awards = set()
    for d in dp_awards:
        if d['awards']:
            for p in d['awards'].split(';'):
                awards.add(p.strip())

    # dp_dptypes = filter_datasets_projects(**{k: params.get(k) for k in keys if k != 'dp_type'})
    # dp_types = set([d['type'] for d in dp_dptypes])

    dp_repos = filter_datasets_projects(**{k: params.get(k) for k in keys if k != 'repo'})
    repos = set()
    for d in dp_repos:
        if d['repositories']:
            for r in d['repositories'].split(';'):
                repos.add(r.strip())

    # Not using yet
    # dp_locations = filter_datasets_projects(**{k: params.get(k) for k in keys if k != 'location'})
    # locations = set()
    # for d in dp_locations:
    #     if d['locations']:
    #         for l in d['locations'].split(';'):
    #             locations.add(l.strip())

    return flask.jsonify({
        'dp_title': sorted(titles),
        'person': sorted(persons),
        'nsf_program': sorted(nsf_programs),
        'award': sorted(awards),
        'sci_program': sorted(sci_programs),
        # 'dp_type': sorted(dp_types),
        'repo': sorted(repos),
        # 'location': sorted(locations)
    })


def filter_datasets_projects(uid=None, free_text=None, dp_title=None, award=None, person=None, spatial_bounds_interpolated=None, sci_program=None,
                             nsf_program=None, dp_type=None, spatial_bounds=None, repo=None, location=None, exclude=False):

    (conn, cur) = connect_to_db()

    if dp_type == 'Project':
        d_or_p = 'datasets'
        query_string = '''SELECT * FROM project_view dpv''' 
        if spatial_bounds_interpolated:
            query_string += ''', text(dpv.bounds_geometry) AS b''' 
        titles = 'dataset_titles'
    elif dp_type == 'Dataset':
        d_or_p = 'projects'
        query_string = '''SELECT * FROM dataset_view dpv'''
        if spatial_bounds_interpolated:
            query_string = '''SELECT * FROM (
                    SELECT *, text(value) AS b FROM dataset_view, JSON_ARRAY_ELEMENTS(bounds_geometry) 
                ) AS dpv'''
        titles = 'project_titles'
    else:
        return

    conds = []
    if uid:
        conds.append(cur.mogrify('dpv.uid = %s', (uid,)))
    if dp_title:
        dp_title = escapeChars(dp_title)
        conds.append("dpv.title ~* '%s' OR dpv.%s ~* '%s'" % (dp_title, titles, dp_title))
    if award:
        conds.append(cur.mogrify('dpv.awards ~* %s', (award,)))
    if person:
        conds.append(cur.mogrify('dpv.persons ~* %s', (person,)))
    if spatial_bounds_interpolated:
        conds.append(cur.mogrify("st_intersects(('srid=4326;'||replace(b,'\"',''))::geography,st_transform(st_geomfromewkt('srid=3031;'||%s),4326))", (spatial_bounds_interpolated,)))
        conds.append("b is not null and b!= 'null'")
    if exclude:
        conds.append(cur.mogrify("NOT ((dpv.east=180 AND dpv.west=-180) OR (dpv.east=360 AND dpv.west=0))"))
    if sci_program:
        if dp_type == 'Project':
            conds.append(cur.mogrify('dpv.science_programs ~* %s OR dpv.datasets ~* %s ', (escapeChars(sci_program), '"science_program" : "%s"' %escapeChars(sci_program))))
        else:
            conds.append(cur.mogrify('dpv.science_programs ~* %s ', (escapeChars(sci_program),)))
    if nsf_program:
        conds.append(cur.mogrify('dpv.nsf_funding_programs ~* %s ', (escapeChars(nsf_program),)))
    # if dp_type and dp_type != 'Both':
    #     conds.append(cur.mogrify('dpv.type=%s ', (dp_type,)))
    # if location:
    #     conds.append(cur.mogrify('dpv.locations ~* %s ', (location,)))
    if free_text:
        free_text = escapeChars(free_text) 
        conds.append(cur.mogrify("title ~* %s OR description ~* %s OR keywords ~* %s OR persons ~* %s OR platforms ~* %s OR instruments ~* %s OR paleo_time ~* %s OR " + d_or_p + " ~* %s", 
                                 (free_text, free_text, free_text, free_text, free_text, free_text, free_text, free_text)))
    if repo:
        conds.append(cur.mogrify('repositories = %s ', (escapeChars(repo),)))

    if len(conds) > 0:
        q_conds = []
        for c in conds:
            if isinstance(c, bytes):
                c = c.decode()
            q_conds.append('(%s)' % c)
        query_string += ' WHERE ' + ' AND '.join(q_conds)
    cur.execute(query_string)
    return cur.fetchall()


def escapeChars(string) :
    chars = ["{", "}", "[", "]", "\\", "|", "(", ")"]
    for c in chars:
        string = string.replace(c, "\\"+c)
    string = string.replace("'","''")
    return string 


def escapeQuotes(string):
    if not string:
        return ''
    return string.replace("'","''")

def initcap(s):
    parts = re.split("( |_|-|>)+", s)
    name = ' '.join([p.lower().capitalize() for p in parts])
    # handle names with apostrophes
    if "'" in name:
        parts = name.split("'")
        name = "'".join([p.lower().capitalize() for p in parts])
    return name


@app.route('/dashboard', methods=['GET'])
def dashboard():
    user_info = session.get('user_info')
    if user_info is None:
        session['next'] = request.path
        return redirect(url_for('login'))

    (conn, cur) = connect_to_db()
    query = """SELECT DISTINCT d.* FROM dataset d
                JOIN dataset_person_map dpm ON d.id = dpm.dataset_id
                JOIN person p ON dpm.person_id = p.id
                WHERE p.email = '%s'
                OR p.id_orcid = '%s'
                ORDER BY d.date_created DESC;""" % (user_info.get('email'), user_info.get('orcid'))

    cur.execute(query)
    datasets = cur.fetchall()

    query = """SELECT DISTINCT project.* FROM project 
            JOIN project_person_map ppm ON project.proj_uid = ppm.proj_uid
            JOIN person p ON ppm.person_id = p.id
            WHERE p.email = '%s'
            OR p.id_orcid = '%s'
            ORDER BY project.date_created DESC;""" % (user_info.get('email'), user_info.get('orcid'))

    cur.execute(query)
    projects = cur.fetchall()

    query = """SELECT award.* FROM award 
            JOIN person p ON award.name = p.id
            OR award.copi ~ p.id
            WHERE p.email = '%s'
            OR p.id_orcid = '%s'
            ORDER BY award::integer;""" % (user_info.get('email'), user_info.get('orcid'))

    cur.execute(query)
    awards = cur.fetchall()

    # get created date for each award from NSF awards API
    for a in awards:
        url = "%s%s.json" % (app.config['NSF_AWARD_API'], a['award'])
        json_url = urlopen(url)
        data = json.loads(json_url.read())
    
        a['date_created'] = ''
        if data.get('response') and data['response'].get('award') and len(data['response']['award']) > 0:
            date = data['response']['award'][0].get('date')
            # change date format
            a['date_created'] = datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')

    return render_template('dashboard.html', user_info=user_info, datasets=datasets, projects=projects, awards=awards) 


@app.route('/curator/award_letters', methods=['GET', 'POST'])
def award_letters():
    template_dict = {}
    template_dict['message'] = []
    template_dict['errors'] = []
    template_dict['welcome_awards'] = []
    template_dict['final_awards'] = []
    template_dict['tab'] = 'welcome'
    template_dict['animate'] = True
    
    # login
    if (not cf.isCurator()):
        session['next'] = request.url
        template_dict['need_login'] = True
    else:
        template_dict['need_login'] = False

    # handle form submission
    if request.method == 'POST':
        res = request.form.to_dict()
        template_dict['tab'] = res.get('tab')
        template_dict['animate'] = res.get('animate')
       
        # send all emails
        if 'send_all_' in res.get('submit_type'):
            success, errors = send_all_award_emails(res)
            if errors:
                for error in errors:
                    template_dict['errors'].append(error)
            if success:
                template_dict['message'].append(success)
        
        # send individual email 
        elif 'send_' in res.get('submit_type'):
            success, error = send_award_email(res)
            if error:
                template_dict['errors'].append(error)
            if success:
                template_dict['message'].append(success)

        # project not needed
        if 'proj_not_needed' in res.get('submit_type'):
            award_id = res.get('submit_type').split('_')[-1]
            (conn, cur) = connect_to_db(curator=True)
            query = """UPDATE award SET project_needed=false WHERE award='%s'; COMMIT;""" % award_id 
            cur.execute(query)

        # welcome email not needed
        if 'welcome_email_not_needed' in res.get('submit_type'):
            award_id = res.get('submit_type').split('_')[-1]
            (conn, cur) = connect_to_db(curator=True)
            query = """UPDATE award SET letter_welcome=true WHERE award='%s'; COMMIT;""" % award_id 
            cur.execute(query)


    # find awards that need the Welcome Letter
    query = """SELECT DISTINCT award, title, sum, start, expiry, name, email, po_name, po_email, is_lead_award, lead_award_id FROM award 
                LEFT JOIN project_award_map pam ON pam.award_id=award.award
                LEFT JOIN award_program_map apm on apm.award_id=award.award
                WHERE expiry > NOW()
                AND apm.program_id != 'Arctic Natural Sciences' 
                AND apm.program_id != 'Polar Special Initiatives'
                AND project_needed
                AND proj_uid IS NULL
                AND NOT letter_welcome
                AND NOT letter_final_year"""
    template_dict['welcome_awards'], template_dict['welcome_award_ids'] = get_letter_awards(query, app.config['AWARD_WELCOME_EMAIL'], 'Welcome to USAP-DC')

    # find awards that need the Final Letter
    three_months = (datetime.now() + timedelta(3*31)).strftime('%m/%d/%Y')
    query = """SELECT DISTINCT award, title, sum, start, expiry, name, email, po_name, po_email, is_lead_award, lead_award_id FROM award 
                LEFT JOIN project_award_map pam ON pam.award_id=award.award
                JOIN award_program_map apm on apm.award_id=award.award
                WHERE expiry > NOW()
                AND expiry < '%s' 
                AND apm.program_id != 'Arctic Natural Sciences'
                AND apm.program_id != 'Polar Special Initiatives'
                AND project_needed
                AND NOT letter_final_year""" %three_months
    template_dict['final_awards'], template_dict['final_award_ids'] = get_letter_awards(query, app.config['AWARD_FINAL_EMAIL'], 'USAP-DC Project Close Out Actions')

    return render_template('award_letters.html', **template_dict)


def get_letter_awards(query, letter, email_subject):
    (conn, cur) = connect_to_db()
    cur.execute(query)
    awards = cur.fetchall()
    award_ids = []
    for award in awards:
        with open(letter) as infile:
            email = infile.read()
            # substitute in values from award into email
            email = email.replace('***Program Officer***', '%s &lt%s&gt' % (award['po_name'], award['po_email'])) \
                         .replace('***DATE***', datetime.now().strftime('%Y-%m-%d')) \
                         .replace('***PI LAST NAME***', award['name'].split(',')[0]) \
                         .replace('***NUMBER - TITLE***', '%s - %s' % (award['award'], award['title']))
            award['email_text'] = email
            award['email_recipients'] = '"%s" <%s>\n"%s" <%s>' % (award['name'], award['email'], award['po_name'], award['po_email'])          
            award['email_subject'] = 'OPP award %s: %s' % (award['award'], email_subject)
            award_ids.append(award['award'])
    return awards, award_ids


def send_all_award_emails(res):
    submit_type = res.get('submit_type').split('_')
    letter_type = submit_type[2]
    awards = json.loads(res.get(letter_type+'_award_ids').replace('\'', '"'))
    errors = []

    for award in awards:
        res['submit_type'] = 'send_%s_%s' % (letter_type, award)
        success, error = send_award_email(res)
        if error:
            errors.append('Award: %s %s' %(award, error))
    
    if len(errors) == 0:
        return 'All emails successfully sent', None
    elif len(errors) == len(awards):
        return None, errors
    else:
        return 'Some emails successfully sent', errors


def send_award_email(res):
    (conn, cur) = connect_to_db(curator=True)
    submit_type = res.get('submit_type').split('_')
    award_id = submit_type[-1]
    letter_type = submit_type[1]
    try:
        sender = app.config['USAP-DC_GMAIL_ACCT']
        recipients_text = res.get('%s_email_recipients_%s' %(letter_type, award_id))
        # FOR TESTING - use curator's email
        # recipients_text = session.get('user_info').get('email')

        recipients = recipients_text.splitlines()
        recipients.append(app.config['USAP-DC_GMAIL_ACCT'])

        banner_path = app.config['AWARD_EMAIL_BANNER']
        email_text = res.get('%s_email_text_%s' %(letter_type, award_id)).replace(banner_path, 'cid:image1')
        if banner_path.startswith('/'):
            banner_path = banner_path[1:]
        success, error = send_gmail_message(sender, recipients, res.get('%s_email_subject_%s' %(letter_type, award_id)), email_text, 
                                  None, banner_path)
        if success:
            # update database
            if letter_type == 'welcome':
                letter_col = 'letter_welcome'
            else:
                letter_col = 'letter_final_year'
            query = """UPDATE award SET %s=true WHERE award='%s'; COMMIT;""" % (letter_col, award_id) 
            cur.execute(query)
            
        return success, error

    except Exception as err:
        return None, "Error sending email: " + str(err)


@app.route('/curator/dif_harvest', methods=['GET', 'POST'])
def dif_harvest():
    template_dict = {}
    template_dict['message'] = []
    template_dict['errors'] = []

    # login
    if (not cf.isCurator()):
        session['next'] = request.url
        template_dict['need_login'] = True
    else:
        template_dict['need_login'] = False

    (conn, cur) = connect_to_db(curator=True)

    print(request.method)
    
    if request.method == 'POST' and request.form.get('submit') == 'dif_to_db':
        sql = request.form.get('sql')
        template_dict['type'] = 'proj' 
        proj_uid = request.args.get('proj_uid')
        dif_id = request.args.get('dif_id')         
        template_dict['proj_uid'] = proj_uid
        template_dict['sql'] = sql
        print("IMPORTING TO DB")
        try:
            # run sql to import data into the database
            cur.execute(sql)

            cmd = "UPDATE project_dif_map SET is_updated = true WHERE proj_uid = %s and dif_id = %s; COMMIT;"
            cur.execute(cmd, (proj_uid, dif_id))

            template_dict['message'].append("Successfully imported to database")
        except Exception as err:
            template_dict['error'] = "Error Importing to database: " + str(err)
            print(err)


    elif not request.args.get('proj_uid'):
        query = "SELECT * from project_dif_map WHERE proj_uid ~* 'p00' ORDER BY is_updated, proj_uid desc"
        cur.execute(query)
        res = cur.fetchall()
        template_dict['projects'] = res
        template_dict['type'] = 'list'

        query2 = "SELECT count(*) from project_dif_map WHERE proj_uid ~* 'p00' AND NOT is_updated"
        cur.execute(query2)
        res2 = cur.fetchone()
        template_dict['count'] = res2['count']

        if request.method == 'POST' and request.form.get('submit') == 'import_all':
            for row in res:
                try:
                    sql = dh.getUpdateSQL(row['proj_uid'], row['dif_id'])
                    cur.execute(sql)
                    print("%s, %s - SUCCESS" % (row['proj_uid'], row['dif_id']))
                    conn.rollback()
                    # cmd = "UPDATE project_dif_map SET is_updated = true WHERE proj_uid = %s and dif_id = %s; COMMIT;"
                    # cur.execute(cmd, (row['proj_uid'], row['dif_id']))
                except Exception as err:
                    print(str(err))
                    if str(err) == "can't execute an empty query":
                        # cmd = "UPDATE project_dif_map SET is_updated = true WHERE proj_uid = %s and dif_id = %s; COMMIT;"
                        # cur.execute(cmd, (row['proj_uid'], row['dif_id']))
                        continue
                    template_dict['error'] = "%s, %s - ERROR: %s" % (row['proj_uid'], row['dif_id'], err)
                    break

    else:
        template_dict['type'] = 'proj' 
        proj_uid = request.args.get('proj_uid')
        dif_id = request.args.get('dif_id')         
        template_dict['proj_uid'] = proj_uid
        template_dict['sql'] = dh.getUpdateSQL(proj_uid, dif_id)



    return render_template('dif_harvest.html', **template_dict)

@app.route('/view/dataset/sitemap.xml', methods=['GET'])
def sitemap():
    (conn, cur) = connect_to_db()
    query = 'SELECT DISTINCT * FROM dataset ORDER BY id'
    cur.execute(query)
    datasets = cur.fetchall()

    values = []
    for ds in datasets:
        values.append({
            'loc':url_for('landing_page', dataset_id=ds['id'], _external=True), 
            'lastmod': ds['date_modified']
            })

    template = render_template('sitemap.xml', values=values)
    response = make_response(template)
    response.headers['content-type'] = 'application/xml'

    return response


@app.route('/tracker', methods=['GET'])
def tracker():
    # use to track clicks to external websites - will show up in Apache logs
    params = request.args.to_dict()
    url = params.get('url')
    print(url)
    if url:
        return redirect(url)
    return redirect(url_for('not_found'))


@app.route('/favicon.ico', methods=['GET'])
def favicon():
    return redirect(url_for('static', filename='imgs/favicon.ico'))


@app.errorhandler(500)
def internal_error(error):
    return redirect(url_for('not_found'))


@app.errorhandler(404)
def page_not_found(error):
    return redirect(url_for('not_found'))


app.jinja_env.globals.update(map=map)
app.jinja_env.globals.update(initcap=initcap)
app.jinja_env.globals.update(randint=randint)
app.jinja_env.globals.update(str=str)
app.jinja_env.globals.update(basename=os.path.basename)
app.jinja_env.globals.update(pathjoin=os.path.join)
app.jinja_env.globals.update(len=len)
app.jinja_env.globals.update(repr=repr)
app.jinja_env.globals.update(ceil=math.ceil)
app.jinja_env.globals.update(int=int)
app.jinja_env.globals.update(filter_awards=lambda awards: [aw for aw in awards if aw['award'] != 'XXXXXXX'])
app.jinja_env.globals.update(json_dumps=json.dumps)
