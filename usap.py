from __future__ import print_function

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import math
import flask
from flask import Flask, session, render_template, redirect, url_for, request, flash, send_from_directory, send_file, current_app
from random import randint
import os
from flask_oauth import OAuth
import json
from urllib2 import Request, urlopen
from werkzeug.utils import secure_filename
import smtplib
from email.mime.text import MIMEText
import psycopg2
import psycopg2.extras
import requests
import re
import copy
from datetime import datetime
import csv
from collections import namedtuple
import humanize
import urllib
import lib.json2sql as json2sql
import shutil
import lib.curatorFunctions as cf
from functools import partial


app = Flask(__name__)


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
    DOI_REF_FILE="inc/doi_ref",
    PROJECT_REF_FILE="inc/project_ref",
    DEBUG=True
)

app.config.update(json.loads(open('config.json', 'r').read()))

app.debug = app.config['DEBUG']
app.secret_key = app.config['SECRET_KEY']

oauth = OAuth()
google = oauth.remote_app('google',
                          base_url='https://www.google.com/accounts/',
                          authorize_url='https://accounts.google.com/o/oauth2/auth',
                          request_token_url=None,
                          request_token_params={'scope': 'https://www.googleapis.com/auth/userinfo.email',
                                                'response_type': 'code'},
                          access_token_url='https://accounts.google.com/o/oauth2/token',
                          access_token_method='POST',
                          access_token_params={'grant_type': 'authorization_code'},
                          consumer_key=app.config['GOOGLE_CLIENT_ID'],
                          consumer_secret=app.config['GOOGLE_CLIENT_SECRET'])


orcid = oauth.remote_app('orcid',
                         base_url='https://orcid.org/oauth/',
                         authorize_url='https://orcid.org/oauth/authorize',
                         request_token_url=None,
                         request_token_params={'scope': '/authenticate',
                                               'response_type': 'code',
                                               'show_login': 'true'},
                         access_token_url='https://pub.orcid.org/oauth/token',
                         access_token_method='POST',
                         access_token_params={'grant_type': 'authorization_code'},
                         consumer_key=app.config['ORCID_CLIENT_ID'],
                         consumer_secret=app.config['ORCID_CLIENT_SECRET'])

config = json.loads(open('config.json', 'r').read())


def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER'],
                            password=info['PASSWORD'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


def get_nsf_grants(columns, award=None, only_inhabited=True):
    (conn, cur) = connect_to_db()
    query_string = """SELECT %s FROM award a WHERE a.award != 'XXXXXXX' and a.award != 'None' and a.award ~ '^[0-9]' 
                      and a.award::integer<8000000 and a.award::integer>0400000""" % ','.join(columns) 
    
    if only_inhabited:
        query_string += ' AND EXISTS (SELECT award_id FROM dataset_award_map dam WHERE dam.award_id=a.award)'
    query_string += ' ORDER BY name,award'

    cur.execute(query_string)
    return cur.fetchall()


def filter_datasets(dataset_id=None, award=None, parameter=None, location=None, person=None, platform=None,
                    sensor=None, west=None, east=None, south=None, north=None, spatial_bounds=None, spatial_bounds_interpolated=None, start=None, stop=None, program=None,
                    project=None, title=None, limit=None, offset=None):
    (conn, cur) = connect_to_db()
    query_string = '''SELECT DISTINCT d.id
                      FROM dataset d
                      LEFT JOIN dataset_award_map dam ON dam.dataset_id=d.id
                      LEFT JOIN award a ON a.award = dam.award_id
                      LEFT JOIN dataset_keyword_map dkm ON dkm.dataset_id=d.id
                      LEFT JOIN keyword k ON k.id=dkm.keyword_id
                      LEFT JOIN dataset_parameter_map dparm ON dparm.dataset_id=d.id
                      LEFT JOIN parameter par ON par.id=dparm.parameter_id
                      LEFT JOIN dataset_location_map dlm ON dlm.dataset_id=d.id
                      LEFT JOIN location l ON l.id=dlm.location_id
                      LEFT JOIN dataset_person_map dperm ON dperm.dataset_id=d.id
                      LEFT JOIN person per ON per.id=dperm.person_id
                      LEFT JOIN dataset_platform_map dplm ON dplm.dataset_id=d.id
                      LEFT JOIN platform pl ON pl.id=dplm.platform_id
                      LEFT JOIN dataset_sensor_map dsenm ON dsenm.dataset_id=d.id
                      LEFT JOIN sensor sen ON sen.id=dsenm.sensor_id
                      LEFT JOIN dataset_spatial_map sp ON sp.dataset_id=d.id
                      LEFT JOIN dataset_temporal_map tem ON tem.dataset_id=d.id
                      LEFT JOIN award_program_map apm ON apm.award_id=a.award
                      LEFT JOIN program prog ON prog.id=apm.program_id
                      LEFT JOIN dataset_initiative_map dprojm ON dprojm.dataset_id=d.id
                      LEFT JOIN initiative proj ON proj.id=dprojm.initiative_id
                   '''
    conds = []
    if dataset_id:
        conds.append(cur.mogrify('d.id=%s', (dataset_id,)))
    if title:
        conds.append(cur.mogrify('d.title ILIKE %s', ('%' + title + '%',)))
    if award:
        [num, name] = award.split(' ', 1)
        conds.append(cur.mogrify('a.award=%s', (num,)))
        conds.append(cur.mogrify('a.name=%s', (name,)))
    if parameter:
        conds.append(cur.mogrify('par.id ILIKE %s', ('%' + parameter + '%',)))
    if location:
        conds.append(cur.mogrify('l.id=%s', (location,)))
    if person:
        conds.append(cur.mogrify('per.id=%s', (person,)))
    if platform:
        conds.append(cur.mogrify('pl.id=%s', (platform,)))
    if sensor:
        conds.append(cur.mogrify('sen.id=%s', (sensor,)))
    if west:
        conds.append(cur.mogrify('%s <= sp.east', (west,)))
    if east:
        conds.append(cur.mogrify('%s >= sp.west', (east,)))
    if north:
        conds.append(cur.mogrify('%s >= sp.south', (north,)))
    if south:
        conds.append(cur.mogrify('%s <= sp.north', (south,)))
    if spatial_bounds_interpolated:
        conds.append(cur.mogrify("st_intersects(st_transform(sp.bounds_geometry,3031),st_geomfromewkt('srid=3031;'||%s))", (spatial_bounds_interpolated,)))
    if start:
        conds.append(cur.mogrify('%s <= tem.stop_date', (start,)))
    if stop:
        conds.append(cur.mogrify('%s >= tem.start_date', (stop,)))
    if program:
        conds.append(cur.mogrify('prog.id=%s ', (program,)))
    if project:
        conds.append(cur.mogrify('proj.id=%s ', (project,)))
    conds.append(cur.mogrify('url IS NOT NULL '))
    conds = ['(' + c + ')' for c in conds]
    if len(conds) > 0:
        query_string += ' WHERE ' + ' AND '.join(conds)

    cur.execute(query_string)
    return [d['id'] for d in cur.fetchall()]


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
                             CASE WHEN rel_proj.rel_projects IS NULL THEN '[]'::json ELSE rel_proj.rel_projects END
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
                        WHERE d.id IN %s ORDER BY d.title''',
                       (tuple(dataset_ids),))
        cur.execute(query_string)
        return cur.fetchall()


def get_parameters(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT DISTINCT id FROM gcmd_science_key'
    query += cur.mogrify(' ORDER BY id')
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
    query += cur.mogrify(' ORDER BY id')
    cur.execute(query)
    return cur.fetchall()


def get_usap_locations(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = "SELECT * FROM vw_location"
    query += cur.mogrify(' ORDER BY keyword_label')
    cur.execute(query)
    return cur.fetchall()


def get_keywords(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM keyword'
    if dataset_id:
        query += cur.mogrify(' WHERE id in (SELECT keyword_id FROM dataset_keyword_map WHERE dataset_id=%s)', (dataset_id,))
    query += cur.mogrify(' ORDER BY id')
    cur.execute(query)
    return cur.fetchall()


def get_platforms(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM platform'
    if dataset_id:
        query += cur.mogrify(' WHERE id in (SELECT platform_id FROM dataset_platform_map WHERE dataset_id=%s)', (dataset_id,))
    query += cur.mogrify(' ORDER BY id')
    cur.execute(query)
    return cur.fetchall()


def get_persons(conn=None, cur=None, dataset_id=None, order=True):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM person'
    if dataset_id:
        query += cur.mogrify(' WHERE id in (SELECT person_id FROM dataset_person_map WHERE dataset_id=%s)', (dataset_id,))
    if order:
        query += cur.mogrify(' ORDER BY id')
    cur.execute(query)
    return cur.fetchall()


def get_project_persons(conn=None, cur=None, project_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM person'
    if project_id:
        query += cur.mogrify(' WHERE id in (SELECT person_id FROM project_person_map WHERE proj_uid=%s)', (project_id,))
    query += cur.mogrify(' ORDER BY id')
    cur.execute(query)
    return cur.fetchall()


def get_person(person_id):
    (conn, cur) = connect_to_db()
    query = 'SELECT * FROM person'
    if person_id:
        query += cur.mogrify(' WHERE id = %s', (person_id,))
    cur.execute(query)
    return cur.fetchone()


def get_sensors(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM sensor'
    if dataset_id:
        query += cur.mogrify(' WHERE id in (SELECT sensor_id FROM dataset_sensor_map WHERE dataset_id=%s)', (dataset_id,))
    query += cur.mogrify(' ORDER BY id')
    cur.execute(query)
    return cur.fetchall()


def get_references(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM reference'
    if dataset_id:
        query += cur.mogrify(' WHERE ref_uid in (SELECT ref_uid FROM dataset_reference_map WHERE dataset_id=%s)', (dataset_id,))
    cur.execute(query)
    return cur.fetchall()


def get_spatial_extents(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM dataset_spatial_map'
    if dataset_id:
        query += cur.mogrify(' WHERE dataset_id=%s', (dataset_id,))
    cur.execute(query)
    return cur.fetchall()


def get_temporal_extents(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM dataset_temporal_map'
    if dataset_id:
        query += cur.mogrify(' WHERE dataset_id=%s', (dataset_id,))
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
        if 'dataset_metadata' not in session:
            session['dataset_metadata'] = dict()   
        session['dataset_metadata'].update(request.form.to_dict())

        publications_keys = [s for s in request.form.keys() if "publication" in s]
        remove_pub_keys = []
        if len(publications_keys) > 0:
            session['dataset_metadata']['publications'] = []
            publications_keys.sort(key=partial(sortNumerically, replace_str='publication'))
            #remove any empty values
            for key in publications_keys:
                if session['dataset_metadata'][key] == "":
                    remove_pub_keys.append(key)
                del session['dataset_metadata'][key]
                del session['dataset_metadata'][key.replace('publication', 'pub_doi')]
            for k in remove_pub_keys: 
                publications_keys.remove(k)
            for key in publications_keys:
                pub_text = request.form.get(key)
                pub_doi = request.form.get(key.replace('publication', 'pub_doi'))
                publication = {'text': pub_text, 'doi': pub_doi}
                session['dataset_metadata']['publications'].append(publication)

        awards_keys = [s for s in request.form.keys() if "award" in s and "user" not in s]
        awards = []
        if len(awards_keys) > 0:
            awards_keys.sort(key=partial(sortNumerically, replace_str='award'))
            for key in awards_keys:
                if session['dataset_metadata'][key] != "" and session['dataset_metadata'][key] != "None":
                    if session['dataset_metadata'][key] == 'Not In This List':
                        user_award_fld = 'user_' + key
                        award_name = "Not_In_This_List:" + session['dataset_metadata'].get(user_award_fld)
                        del session['dataset_metadata'][user_award_fld]
                    else:
                        award_name = session['dataset_metadata'].get(key)
                    awards.append(award_name)
                del session['dataset_metadata'][key]
            session['dataset_metadata']['awards'] = awards

        locations_keys = [s for s in request.form.keys() if "location" in s and "user" not in s]
        locations = []
        if len(locations_keys) > 0:
            locations_keys.sort(key=partial(sortNumerically, replace_str='location'))
            for key in locations_keys:
                if session['dataset_metadata'][key] != "" and session['dataset_metadata'][key] != "None":
                    if session['dataset_metadata'][key] == 'Not In This List':
                        user_loc_fld = 'user_' + key
                        location_name = "Not_In_This_List:" + session['dataset_metadata'].get(user_loc_fld)
                        del session['dataset_metadata'][user_loc_fld]
                    else:
                        location_name = session['dataset_metadata'].get(key)
                    locations.append(location_name)
                del session['dataset_metadata'][key]
            session['dataset_metadata']['locations'] = locations

        author_keys = [s for s in request.form.keys() if "author_name_last" in s]
        remove_author_keys = []
        if len(author_keys) > 0:
            session['dataset_metadata']['authors'] = []
            author_keys.sort(key=partial(sortNumerically, replace_str='author_name_last'))
            #remove any empty values
            for key in author_keys:
                if session['dataset_metadata'][key] == "":
                    remove_author_keys.append(key)
                del session['dataset_metadata'][key]
                del session['dataset_metadata'][key.replace('last', 'first')]
            for k in remove_author_keys: 
                author_keys.remove(k)
 
            for key in author_keys:
                last_name = request.form.get(key)
                first_name = request.form.get(key.replace('last', 'first'))
                author = {'first_name': first_name, 'last_name': last_name}
                session['dataset_metadata']['authors'].append(author) 
            
        session['dataset_metadata']['agree'] = 'agree' in request.form
        # for some reason, the flash command makes sure that the session variable remains in tact when saving or restoring
        flash('test message')

        if request.form.get('action') == "Previous Page":
            return render_template('dataset.html', name=user_info['name'], email="", error=error, success=success, 
                                   dataset_metadata=session.get('dataset_metadata', dict()), nsf_grants=get_nsf_grants(['award', 'name', 'title'], 
                                   only_inhabited=False), projects=get_projects(), persons=get_persons(), locations=get_usap_locations(),)

        elif request.form.get('action') == "save":
            # save to file
            if user_info.get('orcid'):
                save_file = os.path.join(app.config['SAVE_FOLDER'], user_info['orcid'] + ".json")
            elif user_info.get('id'):
                save_file = os.path.join(app.config['SAVE_FOLDER'], user_info['id'] + ".json")
            else:
                error = "Unable to save dataset."
            if save_file:
                try:
                    with open(save_file, 'w') as file:
                        file.write(json.dumps(session.get('dataset_metadata', dict()), indent=4, sort_keys=True))
                    success = "Saved dataset form"
                except Exception as e:
                    error = "Unable to save dataset."
            return render_template('dataset.html', name=user_info['name'], email="", error=error, success=success, 
                                   dataset_metadata=session.get('dataset_metadata', dict()), nsf_grants=get_nsf_grants(['award', 'name', 'title'], 
                                   only_inhabited=False), projects=get_projects(), persons=get_persons(), locations=get_usap_locations(), edit=edit)

        elif request.form.get('action') == "restore":
            # restore from file
            if user_info.get('orcid'):
                saved_file = os.path.join(app.config['SAVE_FOLDER'], user_info['orcid'] + ".json")
            elif user_info.get('id'):
                saved_file = os.path.join(app.config['SAVE_FOLDER'], user_info['id'] + ".json")
            else:
                error = "Unable to restore dataset"
            if saved_file:
                try:
                    with open(saved_file, 'r') as file:
                        data = json.load(file)
                        del data['dataset_id']
                    session['dataset_metadata'].update(data)
                    success = "Restored dataset form"
                except Exception as e:
                    error = "Unable to restore dataset."
            else:
                error = "Unable to restore dataset."
            return render_template('dataset.html', name=user_info['name'], email="", error=error, success=success, 
                                   dataset_metadata=session.get('dataset_metadata', dict()), nsf_grants=get_nsf_grants(['award', 'name', 'title'], 
                                   only_inhabited=False), projects=get_projects(), persons=get_persons(), locations=get_usap_locations(), edit=edit)

        if edit:
            return redirect('/edit/dataset2/' + dataset_id)
        return redirect('/submit/dataset2')

    else:
        session['dataset_metadata'] = {}
        # EDIT dataset
        # get the dataset ID from the URL
        if edit:
            form_data = dataset_db2form(dataset_id)
            session['dataset_metadata'] = form_data
            email = form_data.get('email')
            name = ""

        # Create new dataset using existing dataset as template
        elif template:
            form_data = dataset_db2form(template_id)
            session['dataset_metadata'] = form_data

            email = ""
            if user_info.get('email'):
                email = user_info.get('email')
            name = ""
            if user_info.get('name'):
                name = user_info.get('name')

            # remove dataset_id when creating new submission
            if form_data.get('dataset_id'):
                del(form_data['dataset_id'])
        else:
            email = ""
            if user_info.get('email'):
                email = user_info.get('email')
            name = ""
            if user_info.get('name'):
                name = user_info.get('name')
                names = name.split(' ')
                if 'dataset_metadata' not in session:
                    session['dataset_metadata'] = dict()
                session['dataset_metadata']['authors'] = [{'first_name': names[0], 'last_name': names[-1]}]
                session['dataset_metadata']['release_date'] = datetime.now().strftime('%Y-%m-%d')
        return render_template('dataset.html', name=name, email=email, error=error, success=success, 
                               dataset_metadata=session.get('dataset_metadata', dict()), 
                               nsf_grants=get_nsf_grants(['award', 'name', 'title'], only_inhabited=False), projects=get_projects(), 
                               persons=get_persons(), locations=get_usap_locations(), edit=edit, template=template)


# get dataset data from DB and convert to json that can be displayed in the Deposit/Edit Dataset page
def dataset_db2form(uid):
    db_data = get_datasets([uid])[0]
    if not db_data: 
        return {}
    form_data = {
        'dataset_id': uid,
        'abstract': db_data.get('abstract'),
        'name': db_data.get('submitter_id'),
        'title': db_data.get('title'),
        'filenames': [],
        'release_date': db_data.get('release_date'),
        'submitter_name': db_data.get('submitter_id'),
        'locations': db_data.get('locations')
    }

    form_data['authors'] = []
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
            form_data['authors'].append({'first_name': first_name, 'last_name': last_name})

    form_data['awards'] = []
    for award in db_data.get('awards'):
        form_data['awards'].append(award.get('award') + ' ' + award.get('name'))

    if db_data.get('spatial_extents'):
        se = db_data.get('spatial_extents')[0]
        form_data['cross_dateline'] = se.get('cross_dateline')
        form_data['geo_e'] = str(se.get('east'))
        form_data['geo_n'] = str(se.get('north'))
        form_data['geo_s'] = str(se.get('south'))
        form_data['geo_w'] = str(se.get('west'))

    if main_author:
        creator = get_person(main_author)
        if creator:
            form_data['email'] = creator.get('email')

    form_data['publications'] = []
    for ref in db_data.get('references'):
        form_data['publications'].append({'doi': ref.get('doi'), 'text': ref.get('ref_text')})

    form_data['project'] = None
    if db_data.get('projects') and len(db_data['projects']) > 0:
        form_data['project'] = db_data['projects'][0].get('id')
    
    if db_data.get('temporal_extents') and len(db_data['temporal_extents']) > 0:
        form_data['start'] = db_data['temporal_extents'][0].get('start_date')
        form_data['stop'] = db_data['temporal_extents'][0].get('stop_date')

    keywords = cf.getDatasetKeywords(uid)
    form_data['user_keywords'] = ''
    for kw in keywords:
        if kw.get('keyword_id')[0:2] == 'uk' and kw.get('keyword_label') not in form_data['locations']:
            if form_data['user_keywords'] != '':
                form_data['user_keywords'] += ', '
            form_data['user_keywords'] += kw.get('keyword_label')

    # read in more fields from the readme file and add to the form_data
    form_data.update(dataset_readme2form(uid))

    # read in remaining fields from previous submisison form and add to the form_data
    form_data.update(dataset_oldform2form(uid))

    # get uploaded files
    url = db_data.get('url')
    form_data['uploaded_files'] = []
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
                form_data['uploaded_files'] = files
                form_data['filenames'].append(f_name)
            files.sort()
        else:
            form_data['uploaded_files'] = [{'url': url, 'name': os.path.basename(os.path.normpath(url))}]

    return form_data


def dataset_readme2form(uid):
    r = requests.get(url_for('readme', dataset_id=uid, _external=True))
    form_data = {}
    # check readme file is found and is plain text (not a pdf)
    if r.url != url_for('not_found', _external=True) and r.headers.get('Content-Type') and r.headers['Content-Type'].find('text/plain') == 0:
        text = r.text
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

    return form_data


def dataset_oldform2form(uid):
    #get Related Field Event IDs and Region Feature Name from previous submission
    submitted_dir = os.path.join(current_app.root_path, app.config['SUBMITTED_FOLDER'])
    # if there is an editted file, use taht one, other wise use original
    if os.path.isfile(os.path.join(submitted_dir, "e" + uid + ".json")):
        submitted_file = os.path.join(submitted_dir, "e" + uid + ".json")
    else:
        submitted_file = os.path.join(submitted_dir, uid + ".json")

    try:
        with open(submitted_file) as infile:
            submitted_data = json.load(infile)
    except:
            submitted_data = {}

    form_data = {'related_fields': submitted_data.get('related_fields'), 'feature_name': submitted_data.get('feature_name')}
    return form_data


@app.route('/submit/help', methods=['GET', 'POST'])
def submit_help():
    return render_template('submission_help.html')


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
    return render_template('error.html', error_message=str(e), back_url=e.redirect, name=session['user_info']['name'])


#@app.errorhandler(OAuthException)
def oauth_error(e):
    return render_template('error.html', error_message=str(e))


#@app.errorhandler(Exception)
def general_error(e):
    return render_template('error.html', error_message=str(e))


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
    if not msg_data.get('edit'):
        validators.append(Validator(func=default_func('filenames'), msg='You must include files in your submission.'))
    
    msg = ""
    for v in validators:
        if not v.func(msg_data):
            msg += "<p>" + v.msg
    if len(msg) > 0:
        raise BadSubmission(msg, '/submit/dataset')


@app.route('/repo_list')
def repo_list():
    return render_template('repo_list.html')


@app.route('/not_found')
def not_found():
    return render_template('not_found.html')


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
        if 'dataset_metadata' not in session:
            session['dataset_metadata'] = dict()

        session['dataset_metadata'].update(request.form.to_dict())
        session['dataset_metadata']['properGeoreferences'] = 'properGeoreferences' in request.form
        session['dataset_metadata']['propertiesExplained'] = 'propertiesExplained' in request.form
        session['dataset_metadata']['comprehensiveLegends'] = 'comprehensiveLegends' in request.form
        session['dataset_metadata']['dataUnits'] = 'dataUnits' in request.form
        # for some reason, the flash command makes sure that the session variable remains in tact when saving or restoring
        flash('test message2')

        if request.form.get('action') == 'Submit':
            msg_data = copy.copy(session['dataset_metadata'])
            msg_data['submitter_name'] = session['user_info'].get('name')
            del msg_data['action']

            cross_dateline = False
            if session['dataset_metadata'].get('cross_dateline') == 'on':
                cross_dateline = True
            msg_data['cross_dateline'] = cross_dateline

            if 'orcid' in session['user_info']:
                msg_data['submitter_orcid'] = session['user_info']['orcid']
            if 'email' in session['user_info']:
                msg_data['submitter_email'] = session['user_info']['email']

            msg_data['filenames'] = []

            if edit:
                # get all the names of any files previously uploaded
                file_keys = [s for s in request.form.keys() if "uploaded_file_" in s]
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

            msg_data['filenames'] += fnames.keys()

            # if files have been added or deleted during an edit, we will create a new dataset
            if edit and (len(files) > 0 or msg_data.get('file_deleted') == 'true'):
                msg_data['related_dataset'] = dataset_id
                msg_data['edit'] = False
                edit = False

            timestamp = format_time()
            msg_data['timestamp'] = timestamp
            check_dataset_submission(msg_data)

            # nsfid = 'NSF' + msg_data['awards'][0].split(' ')[0]
            upload_dir = os.path.join(current_app.root_path, app.config['UPLOAD_FOLDER'], timestamp)
            msg_data['upload_directory'] = upload_dir
            if not os.path.exists(upload_dir):
                os.makedirs(upload_dir)

            for fname, fobj in fnames.items():
                fobj.save(os.path.join(upload_dir, fname))
          
            # save json file in submitted dir
            submitted_dir = os.path.join(current_app.root_path, app.config['SUBMITTED_FOLDER'])
            if edit:
                submitted_file = os.path.join(submitted_dir, "e" + dataset_id + ".json")
            else:
                # get next_id
                next_id = getNextDOIRef()
                updateNextDOIRef()
                submitted_file = os.path.join(submitted_dir, next_id + ".json")
            with open(submitted_file, 'w') as file:
                file.write(json.dumps(msg_data, indent=4, sort_keys=True))
            os.chmod(submitted_file, 0o664)
          
            # email RT queue
            if edit:
                message = "Dataset Edit.\n\nDataset JSON: %scurator?uid=e%s\n" \
                    % (request.url_root, dataset_id)
            else:
                message = "New dataset submission.\n\nDataset JSON: %scurator?uid=%s\n" \
                    % (request.url_root, next_id)
            msg = MIMEText(message)
            if msg_data.get('submitter_email'):
                sender = msg_data.get('submitter_email')
            else:
                sender = msg_data.get('email')
            recipients = ['info@usap-dc.org']

            if edit:
                msg['Subject'] = 'USAP-DC Dataset Edit'
            else: 
                msg['Subject'] = 'USAP-DC Dataset Submission'
            msg['From'] = sender
            msg['To'] = ', '.join(recipients)

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
            # post the form back to dataset since the session['dataset_metadata'] 
            # gets lost if we use a standard GET redirect
            if edit:
                return redirect('/edit/dataset/' + dataset_id, code=307)
            return redirect('/submit/dataset', code=307)
        elif request.form.get('action') == "save":
            # save to file
            if user_info.get('orcid'):
                save_file = os.path.join(app.config['SAVE_FOLDER'], user_info['orcid'] + ".json")
            elif user_info.get('id'):
                save_file = os.path.join(app.config['SAVE_FOLDER'], user_info['id'] + ".json")
            else:
                error = "Unable to save dataset."
            if save_file:
                try:
                    with open(save_file, 'w') as file:
                        file.write(json.dumps(session.get('dataset_metadata', dict()), indent=4, sort_keys=True))
                    success = "Saved dataset form"
                except Exception as e:
                    error = "Unable to save dataset."
            return render_template('dataset2.html', name=user_info['name'], email="", error=error, success=success, dataset_metadata=session.get('dataset_metadata', dict()), edit=edit)

        elif request.form.get('action') == "restore":
            # restore from file
            if user_info.get('orcid'):
                saved_file = os.path.join(app.config['SAVE_FOLDER'], user_info['orcid'] + ".json")
            elif user_info.get('id'):
                saved_file = os.path.join(app.config['SAVE_FOLDER'], user_info['id'] + ".json")
            else:
                error = "Unable to restore dataset"
            if saved_file:
                try:
                    with open(saved_file, 'r') as file:
                        data = json.load(file)
                    session['dataset_metadata'].update(data)
                    success = "Restored dataset form"
                except Exception as e:
                    error = "Unable to restore dataset."
            else:
                error = "Unable to restore dataset."
            return render_template('dataset2.html', name=user_info['name'], email="", error=error, success=success, dataset_metadata=session.get('dataset_metadata', dict()), edit=edit)

    else:
        email = ""
        if user_info.get('email'):
            email = user_info.get('email')
        name = ""
        if user_info.get('name'):
            name = user_info.get('name')
        return render_template('dataset2.html', name=name, email=email, dataset_metadata=session.get('dataset_metadata', dict()), edit=edit)


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
    # make some space in the session cookie by clearing any dataset_metadata
    if session.get('dataset_metadata'):
        del session['dataset_metadata']
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
    # user_info = {'name': 'test name', 'email': 'test@email.com'}
    if user_info is None:
        session['next'] = request.path
        return redirect(url_for('login'))

    # if editing - check user has editing permissions on this dataset
    if edit and not check_user_permission(user_info, project_id, project=True):
            return redirect(url_for('invalid_project_user', project_id=project_id))

    if request.method == 'POST':
        if request.form.get('action') == 'Submit':
            msg_data = process_form_data(request.form.to_dict())

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
            submitted_dir = os.path.join(current_app.root_path, app.config['SUBMITTED_FOLDER'])
            if edit:
                submitted_file = os.path.join(submitted_dir, "e" + project_id + ".json")
            else:
                # get next_id for project
                next_id = getNextProjectRef()
                updateNextProjectRef()
                submitted_file = os.path.join(submitted_dir, next_id + ".json")
            with open(submitted_file, 'w') as file:
                file.write(json.dumps(msg_data, indent=4, sort_keys=True))
            os.chmod(submitted_file, 0o664)

            # email RT queue
            if edit:
                message = "Project Edit.\n\nProject JSON: %scurator?uid=e%s\n" \
                    % (request.url_root, project_id)
            else:
                message = "New project submission.\n\nProject JSON: %scurator?uid=%s\n" \
                    % (request.url_root, next_id)
            msg = MIMEText(message)

            # use submitter's email if available, otherwise the email given in the form
            sender = msg_data.get('submitter_email')
            if not sender: 
                sender = msg_data.get('email')

            recipients = ['info@usap-dc.org']

            if edit:
                msg['Subject'] = 'USAP-DC Project Edit'
            else: 
                msg['Subject'] = 'USAP-DC Project Submission'
            msg['From'] = sender
            msg['To'] = ', '.join(recipients)

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
            
            return redirect('thank_you/project')

        elif request.form.get('action') == "save":

            project_metadata = process_form_data(request.form.to_dict())
            success, error, full_name = save_project(project_metadata)
            
            return render_template('project.html', name=user_info['name'], full_name=full_name, email=project_metadata['email'], programs=get_projects(), persons=get_persons(),
                                   nsf_grants=get_nsf_grants(['award', 'name', 'title'], only_inhabited=False), deployment_types=get_deployment_types(),
                                   locations=get_usap_locations(), parameters=get_parameters(), orgs=get_orgs(), roles=get_roles(), 
                                   project_metadata=project_metadata, edit=edit, error=error, success=success)

        elif request.form.get('action') == "restore":
            # restore from file
            if user_info.get('orcid'):
                saved_file = os.path.join(app.config['SAVE_FOLDER'], user_info['orcid'] + "_p.json")
            elif user_info.get('id'):
                saved_file = os.path.join(app.config['SAVE_FOLDER'], user_info['id'] + "_p.json")
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
                                   locations=get_usap_locations(), parameters=get_parameters(), orgs=get_orgs(), roles=get_roles(), 
                                   project_metadata=project_metadata, edit=edit, error=error, success=success)

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
                               locations=get_usap_locations(), parameters=get_parameters(), orgs=get_orgs(), roles=get_roles(), 
                               project_metadata=session.get('project_metadata'), edit=edit)


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
    elif user_info.get('id'):
        save_file = os.path.join(app.config['SAVE_FOLDER'], user_info['id'] + "_p.json")
    else:
        error = "Unable to save project."
    if save_file:
        try:
            with open(save_file, 'w') as file:
                file.write(json.dumps(project_metadata, indent=4, sort_keys=True))
            success = "Saved project form"
        except Exception as e:
            error = "Unable to project dataset."
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
        if msg_data[key] != "":
            if msg_data[key] == 'Not In This List':
                msg_data[key] = "Not_In_This_List:" + msg_data[user_award_key]
            other_awards.append(msg_data[key])
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

    locations_keys = [s for s in request.form.keys() if "location" in s and "user" not in s]
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

    datasets = []
    idx = 0
    key = 'ds_repo'
    while key in msg_data:
        if msg_data[key] != "":
            repo = {'repository': msg_data[key],
                    'title': msg_data.get(key.replace('repo', 'title')),
                    'url': msg_data.get(key.replace('repo', 'url')),
                    'doi': msg_data.get(key.replace('repo', 'doi'))}
            datasets.append(repo)
        del msg_data[key] 
        if msg_data.get(key.replace('repo', 'title')):
            del msg_data[key.replace('repo', 'title')]
        if msg_data.get(key.replace('repo', 'url')):
            del msg_data[key.replace('repo', 'url')]
        if msg_data.get(key.replace('repo', 'doi')):
            del msg_data[key.replace('repo', 'doi')]
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
        'websites': [],
        'copis': [],
        'deployments': [],
        'locations': [],
        'parameters': [],
        'publications': [],
        'datasets': [],
        'program': None,
        'dmp_file': None
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

    awards = set()
    for award in db_data.get('funding'):
        if award.get('is_main_award'):
            form_data['award'] = award.get('award') + ' ' + award.get('pi_name')
            if award.get('dmp_link'):
                form_data['dmp_file'] = award.get('dmp_link').split('/')[-1]
        else:
            awards.add(award.get('award') + ' ' + award.get('pi_name'))
    form_data['other_awards'] = list(awards)

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

    if db_data.get('feature'):
        features = []
        for f in db_data.get('feature'):
            features.append(f.get('feature_name'))
        form_data['location_free'] = ', '.join(features)

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
            form_data['datasets'].append({'repository': d.get('repository'),
                                          'title': d.get('title'),
                                          'url': d.get('url'),
                                          'doi': d.get('doi')})
 
    return form_data


@app.route('/submit/projectinfo', methods=['GET'])
def projectinfo():
    award_id = request.args.get('award')
    if award_id and award_id != 'Not_In_This_List':       
        (conn, cur) = connect_to_db()
        query_string = "SELECT * FROM award a WHERE a.award = '%s'" % award_id
        cur.execute(query_string)
        return flask.jsonify(cur.fetchall()[0])
    return flask.jsonify({})


@app.route('/login')
def login():
    if session.get('next') is None:
        session['next'] = '/home'
    return render_template('login.html')


@app.route('/login_google')
def login_google():
    callback = url_for('authorized', _external=True)
    return google.authorize(callback=callback)


@app.route('/login_orcid')
def login_orcid():
    callback = url_for('authorized_orcid', _external=True)
    return orcid.authorize(callback=callback)


@app.route('/authorized')
@google.authorized_handler
def authorized(resp):
    access_token = resp['access_token']
    session['google_access_token'] = access_token, ''
    session['googleSignedIn'] = True
    headers = {'Authorization': 'OAuth ' + access_token}
    req = Request('https://www.googleapis.com/oauth2/v1/userinfo',
                  None, headers)
    res = urlopen(req)
    session['user_info'] = json.loads(res.read())
    if session['user_info'].get('name') is None:
        session['user_info']['name'] = ""

    session['user_info']['is_curator'] = cf.isCurator()
    return redirect(session['next'])


@app.route('/authorized_orcid')
@orcid.authorized_handler
def authorized_orcid(resp):
    session['orcid_access_token'] = resp['access_token']

    session['user_info'] = {
        'name': resp.get('name'),
        'orcid': resp.get('orcid')
    }

    res = requests.get('https://pub.orcid.org/v2.1/' + resp['orcid'] + '/email',
                       headers={'accept': 'application/json'}).json()
    try:
        email = res['email'][0]['email']
        session['user_info']['email'] = email
    except:
        email = ''

    session['user_info']['is_curator'] = cf.isCurator()
    return redirect(session['next'])


@google.tokengetter
def get_access_token():
    return session.get('google_access_token')


@app.route('/logout', methods=['GET'])
def logout():
    if 'user_info' in session:
        del session['user_info']
    if 'google_access_token' in session:
        del session['google_access_token']
    if 'orcid_access_token' in session:
        del session['orcid_access_token']
    if 'dataset_metadata' in session:
        del session['dataset_metadata']
    if request.args.get('type') == 'curator':
        return redirect(url_for('curator'))
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
    with open("inc/recent_news.txt") as csvfile:
        reader = csv.reader(csvfile, delimiter="\t")
        for row in reader:
            if row[0] == "#" or len(row) < 2:
                continue
            news_dict.append({"date": row[0], "news": row[1]})
        template_dict['news_dict'] = news_dict
    # read in recent data
    data_dict = []
    with open("inc/recent_data.txt") as csvfile:
        reader = csv.reader(csvfile, delimiter="\t")
        for row in reader:
            if row[0] == "#" or len(row) < 4: 
                continue
            data_dict.append({"date": row[0], "link": row[1], "authors": row[2], "title": row[3]})
        template_dict['data_dict'] = data_dict

    # get all spatial extents
    (conn, cur) = connect_to_db()
    template_dict['spatial_extents'] = get_spatial_extents(conn=conn, cur=cur)
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


#DEPRECATED
@app.route('/search_old', methods=['GET', 'POST'])
def search_old():
    if request.method == 'GET':
        return render_template('search_old.html', search_params=session.get('search_params'), nsf_grants=get_nsf_grants(['award', 'name', 'title']), keywords=get_keywords(),
                               parameters=get_parameters(), locations=get_locations(), platforms=get_platforms(),
                               persons=get_persons(), sensors=get_sensors(), programs=get_programs(), projects=get_projects(), titles=get_titles())
    elif request.method == 'POST':
        params = request.form.to_dict()
        filtered = filter_datasets(**params)

        del params['spatial_bounds_interpolated']
        session['filtered_datasets'] = filtered
        session['search_params'] = params

        return redirect('/search_result')


#DEPRECATED
@app.route('/filter_search_menus', methods=['GET'])
def filter_search_menus():
    keys = ['person', 'parameter', 'program', 'award', 'title', 'project']
    args = request.args.to_dict()
    # if reseting:
    if args == {}:
        session['search_params'] = {}

    person_ids = filter_datasets(**{k: args.get(k) for k in keys if k != 'person'})
    person_dsets = get_datasets(person_ids)
    persons = set([p['id'] for d in person_dsets for p in d['persons']])

    parameter_ids = filter_datasets(**{k: args.get(k) for k in keys if k != 'parameter'})
    parameter_dsets = get_datasets(parameter_ids)
    parameters = set([' > '.join(p['id'].split(' > ')[2:]) for d in parameter_dsets for p in d['parameters']])

    program_ids = filter_datasets(**{k: args.get(k) for k in keys if k != 'program'})
    program_dsets = get_datasets(program_ids)
    programs = set([p['id'] for d in program_dsets for p in d['programs']])

    award_ids = filter_datasets(**{k: args.get(k) for k in keys if k != 'award'})
    award_dsets = get_datasets(award_ids)
    awards = set([(p['name'], p['award']) for d in award_dsets for p in d['awards']])

    project_ids = filter_datasets(**{k: args.get(k) for k in keys if k != 'project'})
    project_dsets = get_datasets(project_ids)
    projects = set([p['id'] for d in project_dsets for p in d['projects']])
#    projects = set([d['id'] for d in project_dsets])

    return flask.jsonify({
        'person': sorted(persons),
        'parameter': sorted(parameters),
        'program': sorted(programs),
        'award': [a[1] + ' ' + a[0] for a in sorted(awards)],
        'project': sorted(projects),
        'sci_program': sorted(projects)
    })

# DEPRECATED
@app.route('/search_result', methods=['GET', 'POST'])
def search_result():
    if 'filtered_datasets' not in session:
        return redirect('/search_old')
    filtered_ids = session['filtered_datasets']
    
    exclude = False
    if request.method == 'POST':
        exclude = request.form.get('exclude') == "on"

    datasets = get_datasets(filtered_ids)

    grp_size = 50
    dataset_grps = []
    cur_grp = []
    total_count = 0
    for d in datasets:
        if exclude and d.get('spatial_extents') and len(d.get('spatial_extents')) > 0 and \
            ((d.get('spatial_extents')[0].get('east') == 180 and d.get('spatial_extents')[0].get('west') == -180) or \
            (d.get('spatial_extents')[0].get('east') == 360 and d.get('spatial_extents')[0].get('west') == 0)):
            continue
        if len(cur_grp) < grp_size:
            cur_grp.append(d)
        else:
            dataset_grps.append(cur_grp)
            cur_grp = []
        total_count += 1
    if len(cur_grp) > 0:
        dataset_grps.append(cur_grp)
    
    return render_template('search_result.html',
                                      total_count=total_count,
                                      dataset_grps=dataset_grps,
                                      exclude=exclude,
                                      search_params=session['search_params'])


@app.route('/contact', methods=['GET', 'POST'])
def contact():
    if request.method == 'GET':
        return render_template('contact.html',secret=app.config['RECAPTCHA_DATA_SITE_KEY'])
    elif request.method == 'POST':
        form = request.form.to_dict()
        g_recaptcha_response = form.get('g-recaptcha-response')
        remoteip = request.remote_addr
        resp = requests.post('https://www.google.com/recaptcha/api/siteverify', data={'response':g_recaptcha_response,'remoteip':remoteip,'secret': app.config['RECAPTCHA_SECRET_KEY']}).json()
        if resp.get('success'):
            sender = form['email']
            recipients = ['info@usap-dc.org']
            msg = MIMEText(form['msg'])
            msg['Subject'] = form['subj']
            msg['From'] = sender
            msg['To'] = ', '.join(recipients)
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
            return redirect('/thank_you/message')
        else:
            msg = "<br/>You failed to pass the captcha<br/>"
            raise CaptchaException(msg, url_for('contact'))


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
    with open("inc/recent_news.txt") as csvfile:
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
    with open("inc/recent_data.txt") as csvfile:
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
            metadata['files'] = files
        files.sort()
    else:
        metadata['files'] = [{'url': url, 'name': os.path.basename(os.path.normpath(url))}]

    if metadata.get('url_extra'):
        metadata['url_extra'] = os.path.basename(metadata['url_extra'])

    metadata['creator_orcids'] = []
    for c in metadata['creator'].split('; '):
        p = get_person(c)
        if p:
            metadata['creator_orcids'].append({'id': p.get('id'), 'orcid': p.get('id_orcid')}) 
        else:
            metadata['creator_orcids'].append({'id': c, 'orcid': None})

    metadata['citation'] = makeCitation(metadata, dataset_id)
    metadata['json_ld'] = makeJsonLD(metadata, dataset_id)

    # get count of how many times this dataset has been downloaded
    metadata['downloads'] = getDownloadsCount(dataset_id)

    return render_template('landing_page.html', data=metadata)


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

    json_ld = {
        "@context": "https://schema.org/",
        "@type": "Dataset",
        "@id": "doi:" + doi,
        "additionalType": ["geolink:Dataset", "vivo:Dataset"],
        "name": data.get('title'),
        "description": data.get('abstract'),
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


def makeCitation(metadata, dataset_id):
    try:
        (conn, cur) = connect_to_db()
        cur.execute("SELECT person_id, first_name, middle_name, last_name FROM dataset_person_map dpm JOIN person ON person.id=dpm.person_id WHERE dataset_id='%s'" % dataset_id)
        creators = cur.fetchall()

        etal = ''
        middle_init = ''

        if len(creators) == 0: 
            creators = metadata['creator'].split(';')
            (last_name, first_name) = creators[0].split(',', 1)
            first_names = first_name.strip().split(' ')
            if len(first_names) > 1:
                middle_init = ' ' + first_names[1][0] + '. '
        else:
            pi = creators[0]
            # try and find the person who matches the first in the dataset.creators list
            creator1 = metadata['creator'].split(' ')[0]
            for creator in creators:
                if creator1 in creator['person_id']:
                    pi = creator

            if pi.get('first_name') and pi.get('last_name'):
                first_name = pi['first_name']
                last_name = pi['last_name']
                
                if pi.get('middle_name'):
                    middle_init = ' ' + pi['middle_name'][0] + '. '
            else:
                (last_name, first_name) = pi['person_id'].split(',', 1)

        if len(creators) > 1: 
            etal = ' et al. ' 
        year = metadata['release_date'].split('-')[0]

        citation = '%s, %s.%s %s(%s) "%s" U.S. Antarctic Program (USAP) Data Center. doi: %s.' % (initcap(last_name), first_name.strip()[0], middle_init, etal, year, metadata['title'], metadata['doi'])
        return citation
    except:
        return None


@app.route('/dataset/<path:filename>')
def file_download(filename):
    dataset_id = request.args.get('dataset_id')
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


@app.route('/mapserver-template.html')
def mapserver_template():
    return render_template('mapserver-template.html')


@app.route('/map')
def map():
    return render_template('data_map.html')


@app.route('/curator', methods=['GET', 'POST'])
def curator():
    template_dict = {}
    template_dict['message'] = []
    template_dict['no_action_status'] = ['Completed', 'Edit completed', 'Rejected', 'No Action Required']
    (conn, cur) = connect_to_db()

    # login
    if (not cf.isCurator()):
        session['next'] = request.url
        template_dict['need_login'] = True
    else:
        template_dict['need_login'] = False
        submitted_dir = os.path.join(current_app.root_path, app.config['SUBMITTED_FOLDER'])

        # if Add User To Dataset / Project button pressed
        if request.args.get('fnc') is not None:
            return render_template('curator.html', type=request.args['fnc'], projects=filter_datasets_projects(dp_type='Project'),
                                   datasets=filter_datasets_projects(dp_type='Dataset'), persons=get_persons(), orgs=get_orgs(), 
                                   roles=get_roles())

        # if Add User submit button pressed
        if request.method == 'POST' and request.form.get('submit') == 'add_user':
            try:
                msg = cf.addUserToDatasetOrProject(request.form)
                if msg:
                    template_dict['error'] = "Error adding user: " + msg
                else:
                    template_dict['message'].append("Successfully added user")
            except Exception as err:
                template_dict['error'] = "Error adding user: " + str(err)
        
        if not request.args.get('uid'):    
            # get list of json files in submission directory, ordered by date
            query = "SELECT * FROM submission ORDER BY submitted_date DESC"
            cur.execute(query)
            res = cur.fetchall()
            if res:
                submissions = []
                for sub in res:
                    uid = sub['uid']
                    landing_page = ''
                    if sub['submission_type'].find('project') == 0 and sub['status'] != 'Pending':
                        landing_page = '/view/project/%s' % uid.replace('e', '') 
                    elif sub['submission_type'].find('dataset') == 0 and sub['status'] != 'Pending':
                        landing_page = '/view/dataset/%s' % uid.replace('e', '')

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

                if edit:
                    template_dict['status_options'] = ['Pending', 'Edit Completed', 'Rejected', 'No Action Required']
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
        
            submission_file = os.path.join(submitted_dir, filename + ".json")
            template_dict['filename'] = submission_file
            template_dict['sql'] = "Will be generated after you click on Create SQL and Readme in JSON tab."
            template_dict['readme'] = "Will be generated after you click on Create SQL and Readme in JSON tab."
            template_dict['dcxml'] = cf.getDataCiteXMLFromFile(uid)
            # for each keyword_type, get all keywords from database 
            template_dict['keywords'] = cf.getKeywordsFromDatabase()

            if request.method == 'POST':
                template_dict.update(request.form.to_dict())

                # read in json and convert to sql
                if request.form.get('submit') == 'make_sql':
                    json_str = request.form.get('json').encode('utf-8')
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
                        try:
                            with open(readme_file) as infile:
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

                        template_dict['message'].append("Successfully imported to database")
                        if edit:
                            template_dict['message'].append("Remember to update DataCite and ISOXML records")
                        data = json.loads(request.form.get('json'))
                        
                        template_dict['email_recipients'] = cf.getCreatorEmails(uid)
                        # add contact and submitter to list of recipients
                        template_dict['email_recipients'] = getEmailsFromJson(data, template_dict['email_recipients'])

                        if edit:
                            template_dict['email_text'] = "This is to confirm that your dataset, '%s', has been successfully updated.\n" \
                                                          % data.get('title') + \
                                                          "Please check the landing page %s and contact us if there are any issues." \
                                                          % url_for('landing_page', dataset_id=uid, _external=True)
                        else:
                            template_dict['email_text'] = "Dear %s,\n" % data.get('submitter_name') \
                                                          + "\nWe have processed your dataset %s, and added the dataset to the USAP-DC repository." % data.get('title') \
                                                          + "\nThe dataset ID is %s." % uid \
                                                          + "\nThe DOI for the dataset is %s." % 'TBD' \
                                                          + "\nPlease check the landing page %s and contact us if there are any issues." % url_for('landing_page', dataset_id=uid, _external=True) \
                                                          + "\n\nBest regards,"  

                        coords = cf.getCoordsFromDatabase(uid)
                        if coords is not None:
                            template_dict['coords'] = coords
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
                            upload_dir = os.path.join(current_app.root_path, app.config['UPLOAD_FOLDER'], timestamp)
                            uid_dir = os.path.join(current_app.root_path, app.config['DATASET_FOLDER'], 'usap-dc', uid)
                            dest_dir = os.path.join(uid_dir, timestamp)
                            try:
                                if os.path.exists(dest_dir):
                                    shutil.rmtree(dest_dir)
                                shutil.copytree(upload_dir, dest_dir)

                                # if this dataset is replacing an existing one
                                # any files from data['filenames'] that were not in
                                # the submission dir should be in the dataset dir
                                # or the replaced dataset
                                if json_data.get('related_dataset') and json_data.get('filenames'):
                                    old_ds = get_datasets([json_data['related_dataset']])[0]
                                    old_dir = old_ds.get('url').replace(app.config['USAP_DOMAIN'], current_app.root_path + '/')
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
                    filename = request.form.get('readme_file')
                    try:
                        with open(filename, 'w') as out_file:
                            out_file.write(readme_str)
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
                        with open(datacite_file, 'w') as out_file:
                            out_file.write(xml_str)
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

                            if edit:
                                template_dict['email_text'] = "This is to confirm that your dataset, '%s', has been successfully updated.\n" \
                                                              % data.get('title') + \
                                                              "Please check the landing page %s and contact us if there are any issues." \
                                                              % url_for('landing_page', dataset_id=uid, _external=True)
                            else:
                                template_dict['email_text'] = "Dear %s,\n" % data.get('submitter_name') \
                                                              + "\nWe have processed your dataset %s, and added the dataset to the USAP-DC repository." % data.get('title') \
                                                              + "\nThe dataset ID is %s." % uid \
                                                              + "\nThe DOI for the dataset is %s." % doi \
                                                              + "\nPlease check the landing page %s and contact us if there are any issues." % url_for('landing_page', dataset_id=uid, _external=True) \
                                                              + "\n\nBest regards," 
                            # Update submission table
                            update_status(uid, 'ISO XML file missing')

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
                        with open(isoxml_file, 'w') as out_file:
                            out_file.write(xml_str)
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

                # Send email to creator and editor - for both datasets and projects
                elif request.form.get('submit') == "send_email":
                    template_dict['tab'] = 'email'
                    try:
                        msg = MIMEText(request.form.get('email_text'))
                        sender = 'info@usap-dc.org'
                        recipients_text = request.form.get('email_recipients').encode('utf-8')
                        recipients = recipients_text.splitlines()

                        msg['Subject'] = request.form.get('email_subject')

                        msg['From'] = sender
                        msg['To'] = ', '.join(recipients)

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
                        template_dict['message'].append("Email sent")
                        s.quit()  
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
                                % (status, comments, datetime.now().strftime('%Y-%m-%d'), filename)
                        cur.execute(query)
                        template_dict['message'].append("Submission status and/or comments updated.")
                    except Exception as err:
                        template_dict['error'] = "Error updating status and/or comments: " + str(err)

                # PROJECT CURATION

                # read in json and convert to sql
                elif request.form.get('submit') == 'make_project_sql':
                    json_str = request.form.get('proj_json').encode('utf-8')
                    json_data = json.loads(json_str)
                    template_dict['json'] = json_str
                    sql = cf.projectJson2sql(json_data, uid)
                    template_dict['sql'] = sql
                    template_dict['tab'] = "project_sql"
   
                # read in sql and submit to the database only
                elif request.form.get('submit') == 'import_project_to_db':
                    sql_str = request.form.get('proj_sql').encode('utf-8')
                    template_dict['sql'] = sql_str
                    template_dict['tab'] = "project_sql"
                    problem = False
                    print("IMPORTING TO DB")
                    try:
                        # run sql to import data into the database
                        cur.execute(sql_str)
                        cf.updateEditFile(uid)
                        template_dict['message'].append("Successfully imported to database")
                        data = json.loads(request.form.get('proj_json'))
                        
                        template_dict['email_recipients'] = cf.getCreatorEmails(uid)
                        # add contact and submitter to list of recipients
                        template_dict['email_recipients'] = getEmailsFromJson(data, template_dict['email_recipients'])

                        if edit:
                            template_dict['email_text'] = "This is to confirm that your project, '%s', has been successfully updated.\n" \
                                                          % data.get('title') + \
                                                          "Please check the landing page %s and contact us if there are any issues." \
                                                          % url_for('project_landing_page', project_id=uid, _external=True)
                        else:
                            template_dict['email_text'] = "Dear %s,\n" % data.get('submitter_name') \
                                                          + "\nThis is to confirm that your project, %s, has been successfully registered at USAP-DC." % data.get('title') \
                                                          + "\nPlease check the landing page %s and contact us (info@usap-dc.org) if there are any issues." % url_for('project_landing_page', project_id=uid, _external=True) \
                                                          + "\n\nWe have also prepared and submitted a catalog entry (DIF) at the Antarctic Master Directory (AMD)." \
                                                          + "\nThe DIF ID will be %s." % 'TBC' \
                                                          + "\nThe direct link to the AMD record will be %s." % 'TBC' \
                                                          + "\n\nIt usually takes AMD staff a few business days to review the submission before it goes live." \
                                                          + "\n\nBest regards,"

                        template_dict['landing_page'] = url_for('project_landing_page', project_id=uid)
                        template_dict['db_imported'] = True
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
                            
                            src = os.path.join(json_data['upload_directory'], json_data['dmp_file'])
                            dst_dir = os.path.join(app.config['AWARDS_FOLDER'], json_data['award'].split(' ')[0])
                            try:
                                if not os.path.exists(dst_dir):
                                    os.mkdir(dst_dir)
                                dst = os.path.join(dst_dir, json_data['dmp_file'])
                                shutil.copyfile(src, dst)
                            except Exception as e:
                                return ("ERROR: unable to copy data management plan to award directory. \n" + str(e))
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
                    award = request.form.get('proj_award')
                    is_main_award = request.form.get('proj_main_award') == 'on'
                    (msg, status) = cf.addAwardToProject(uid, award, is_main_award)
                    if status == 0:
                        template_dict['error'] = msg
                    else:
                        template_dict['message'].append(msg)

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
                        with open(difxml_file, 'w') as out_file:
                            out_file.write(xml_str)
                            template_dict['message'].append("DIF XML file saved to watch directory.")
                        os.chmod(difxml_file, 0o664)
                        # Update submission table
                        update_status(uid, 'Completed')

                    except Exception as err:
                        template_dict['error'] = "Error saving DIF XML file to watch directory: " + str(err)

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

                        if edit:
                            template_dict['email_text'] = "This is to confirm that your project, '%s', has been successfully updated.\n" \
                                                          % data.get('title') + \
                                                          "Please check the landing page %s and contact us if there are any issues." \
                                                          % url_for('project_landing_page', project_id=uid, _external=True)
                        else:
                            template_dict['email_text'] = "Dear %s,\n" % data.get('submitter_name') \
                                                          + "\nThis is to confirm that your project, %s, has been successfully registered at USAP-DC." % data.get('title') \
                                                          + "\nPlease check the landing page %s and contact us (info@usap-dc.org) if there are any issues." % url_for('project_landing_page', project_id=uid, _external=True) \
                                                          + "\n\nWe have also prepared and submitted a catalog entry (DIF) at the Antarctic Master Directory (AMD)." \
                                                          + "\nThe DIF ID will be %s." % cf.getDifID(uid) \
                                                          + "\nThe direct link to the AMD record will be %s." % cf.getDifUrl(uid) \
                                                          + "\n\nIt usually takes AMD staff a few business days to review the submission before it goes live." \
                                                          + "\n\nBest regards,"

            else:
                # display submission json file
                try:
                    with open(submission_file) as infile:
                        data = json.load(infile)
                        submission_data = json.dumps(data, sort_keys=True, indent=4)
                        template_dict['json'] = submission_data
                        # add contact and submitter to list of recipients
                        template_dict['email_recipients'] = getEmailsFromJson(data, template_dict['email_recipients'])
                except:
                    template_dict['error'] = "Can't read submission file: %s" % submission_file

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
    (conn, cur) = connect_to_db()
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


def getFromDifTable(col, all_selected):
    (conn, cur) = connect_to_db()
    query = "SELECT DISTINCT %s FROM dif_test " % col
    if not all_selected:
        query += "WHERE is_usap_dc = true "
    query += "ORDER BY %s;" % col
    cur.execute(query)
    return cur.fetchall()


@app.route('/catalog_browser', methods=['GET', 'POST'])
def catalog_browser():
    all_selected = False
    template_dict = {'pi_name': '', 'title': '', 'award': '', 'dif_id': '', 'all_selected': all_selected}
    (conn, cur) = connect_to_db()

    query = "SELECT DISTINCT dif_test.*, ST_AsText(dsm.bounds_geometry) AS bounds_geometry FROM dif_test LEFT JOIN dif_spatial_map dsm ON dsm.dif_id = dif_test.dif_id WHERE dif_test.dif_id !=''"

    if request.method == 'POST':

        template_dict['pi_name'] = request.form.get('pi_name')
        template_dict['title'] = request.form.get('title')
        template_dict['summary'] = request.form.get('summary')
        template_dict['award'] = request.form.get('award')
        template_dict['dif_id'] = request.form.get('dif_id')
        all_selected = bool(int(request.form.get('all_selected')))
        template_dict['all_selected'] = all_selected

        # print(bool(int(request.form.get('all_selected'))))
        if (request.form.get('pi_name') != ""):
            query += " AND dif_test.pi_name ~* '%s'" % request.form['pi_name']
        if(request.form.get('title') != ""):
            query += " AND dif_test.title ILIKE '%" + request.form['title'] + "%'"
        if(request.form.get('summary') != ""):
            query += " AND dif_test.summary ILIKE '%" + request.form['summary'] + "%'"
        if (request.form.get('award') != "" and request.form.get('award') != "Any award"):
            query += " AND dif_test.award = '%s'" % request.form['award']
        if (request.form.get('dif_id') != "" and request.form.get('dif_id') != "Any DIF ID"):
            query += " AND dif_test.dif_id = '%s'" % request.form['dif_id']

    if not all_selected:
        query += " AND dif_test.is_usap_dc = true"

    query += " ORDER BY dif_test.date_created DESC"

    query_string = cur.mogrify(query)
    cur.execute(query_string)
    rows = cur.fetchall()

    for row in rows:
        authors = row['pi_name']
        if row['co_pi'] != "":
            authors += "; %s" % row['co_pi']
        row['authors'] = authors
        if row['award'] != "":
            row['award'] = int(row['award'])
            row['award_7d'] = "%07d" % row['award']
        ds_query = "SELECT * FROM dif_data_url_map WHERE dif_id = '%s'" % row['dif_id']
        ds_query_string = cur.mogrify(ds_query)
        cur.execute(ds_query_string)
        datasets = cur.fetchall()

        # get the list of repositories
        repos = []
        for ds in datasets:
            repo = ds['repository']
            if repo not in repos:
                repos.append(repo)
        row['repositories'] = repos

        if row['dif_id'] == "NSF-ANT05-37143":
            datasets = [{'title': 'Studies of Antarctic Fungi', 'url': url_for('genBank_datasets')}]

        row['datasets'] = datasets

    template_dict['dif_records'] = rows

    if template_dict['award'] == "":
        template_dict['award'] = "Any award"

    if template_dict['dif_id'] == "":
        template_dict['dif_id'] = "Any DIF ID"

    # get list of available options for drop downs and autocomplete
    template_dict['awards'] = getFromDifTable('award', all_selected)
    template_dict['dif_ids'] = getFromDifTable('dif_id', all_selected)
    template_dict['titles'] = getFromDifTable('title', all_selected)
    template_dict['pi_names'] = getFromDifTable('pi_name', all_selected)

    return render_template('catalog_browser.html', **template_dict)


@app.route('/filter_dif_menus', methods=['GET'])
def filter_dif_menus():
    args = request.args.to_dict()
    # if reseting:
    if args == {}:
        session['search_params'] = {}

    (conn, cur) = connect_to_db()

    query_base = "SELECT award, dif_id FROM dif_test"

    if args.get('dif_id') is not None and args['dif_id'] != 'Any DIF ID' and \
       args['dif_id'] != '':
        query_string = query_base + " WHERE dif_id = '%s'" % args['dif_id']
        if args.get('all_selected') is not None and args['all_selected'] == '0':
            query_string += " AND is_usap_dc = true"
    else:
        if args.get('all_selected') is not None and args['all_selected'] == '0':
            query_string = query_base + " WHERE is_usap_dc = true"
        else:
            query_string = query_base
    cur.execute(query_string)
    dsets = cur.fetchall()
    awards = set([d['award'] for d in dsets])

    if args.get('award') is not None and args['award'] != 'Any award' and \
       args['award'] != '':
        query_string = query_base + " WHERE award = '%s'" % args['award']
        if args.get('all_selected') is not None and args['all_selected'] == '0':
            query_string += " AND is_usap_dc = true"
    else:
        if args.get('all_selected') is not None and args['all_selected'] == '0':
            query_string = query_base + " WHERE is_usap_dc = true"
        else:
            query_string = query_base
    cur.execute(query_string)
    dsets = cur.fetchall()
    dif_ids = set([d['dif_id'] for d in dsets])

    return flask.jsonify({
        'award': sorted(awards),
        'dif_id': sorted(dif_ids)
    })


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
    print('getfeatureinfo')
    if request.args.get('layers') != "":
        url = urllib.unquote('http://api.usap-dc.org:81/wfs?' + urllib.urlencode(request.args))
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

    template_dict['start_date'] = start_date
    template_dict['end_date'] = end_date

    # get download information from the database
    (conn, cur) = connect_to_db()
    query = "SELECT * FROM access_logs_downloads WHERE time >= '%s' AND time <= '%s';" % (start_date, end_date)

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

    download_numfiles_bytes = []
    download_users_downloads = []
    months_list = downloads.keys()
    months_list.sort()
    for month in months_list:
        download_numfiles_bytes.append([month, downloads[month]['num_files'], downloads[month]['bytes']])
        download_users_downloads.append([month, len(downloads[month]['users']), len(downloads[month]['user_files'])])
    template_dict['download_numfiles_bytes'] = download_numfiles_bytes
    template_dict['download_users_downloads'] = download_users_downloads

    # To be used if we start collecting stats on searches
    
    # get search information from the database
    (conn, cur) = connect_to_db()
    query = "SELECT * FROM access_logs_searches WHERE time >= '%s' AND time <= '%s';" % (start_date, end_date)

    cur.execute(query)
    data = cur.fetchall()
    searches = {'repos': {}, 'sci_progs': {}, 'nsf_progs': {}, 'persons': {}, 'awards': {}, 'free_texts': {}, 'titles': {}, 'spatial_bounds': {}}
    params = {'repo': 'repos', 'sci_program': 'sci_progs', 'nsf_program': 'nsf_progs', 'person': 'persons', 'award': 'awards', 
              'free_text': 'free_texts', 'dp_title': 'titles', 'spatial_bounds': 'spatial_bounds'}
    for row in data:
        time = row['time']
        month = "%s-%02d-01" % (time.year, time.month)  
        bytes = row['resource_size']
        user = row['remote_host']
        resource = row['resource_requested']
        search = parseSearch(resource)

        for search_param, searches_param in params.items():
            searches = binSearch(search, searches, search_param, searches_param)

    template_dict['searches'] = searches
    # get submission information from the database

    query = cur.mogrify('''SELECT dsf.*, d.date_created::text, dt.date_created::text AS dif_date FROM dataset_file_info dsf 
            JOIN dataset d ON d.id = dsf.dataset_id
            LEFT JOIN dif_test dt ON d.id_orig = dt.dif_id;''') 
    cur.execute(query)
    data = cur.fetchall()
    submissions = {}
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
        month = "%s-01" % date[:7]
        bytes = row['file_size']
        num_files = row['file_count']
        submission = row['dataset_id']
        if submissions.get(month):
            submissions[month]['bytes'] += bytes
            submissions[month]['num_files'] += num_files
            submissions[month]['submissions'].add(submission)
        else:
            submissions[month] = {'bytes': bytes, 'num_files': num_files, 'submissions': {submission}}

    submission_bytes = []
    submission_num_files = []
    submission_submissions = []
    months_list = submissions.keys()
    months_list.sort()
    for month in months_list:
        submission_bytes.append([month, submissions[month]['bytes']])
        submission_num_files.append([month, submissions[month]['num_files']])
        submission_submissions.append([month, len(submissions[month]['submissions'])])
    template_dict['submission_bytes'] = submission_bytes
    template_dict['submission_num_files'] = submission_num_files
    template_dict['submission_submissions'] = submission_submissions
    template_dict['download_numbers'] = getDownloadsForDatasets(start_date, end_date)

    return render_template('statistics.html', **template_dict)


def binSearch(search, searches, search_param, searches_param):
    if search.get(search_param) and search[search_param] != '':
        search[search_param] = search[search_param].replace('+', ' ').replace('%2C', ',')
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


# To be used if we start collecting stats on searches
def parseSearch(resource):
    resource = resource.split('?')[1]
    filters = resource.split('&')
    search = {}
    for f in filters:
        try:
            filter, value = f.split('=', 1)
            search[filter] = value
        except:
            continue
    return search


@app.route('/view/project/<project_id>')
def project_landing_page(project_id):
    metadata = get_project(project_id)
    if metadata is None:
        return redirect(url_for('not_found'))

    # make a list of all the unique Data Management Plans
    dmps = set()
    if metadata.get('funding'):
        for f in metadata['funding']:
            if f.get('dmp_link') and f['dmp_link'] != 'None':
                dmps.add(f['dmp_link'])
        if len(dmps) == 0:
            dmps = None
    metadata['dmps'] = dmps

    return render_template('project_landing_page.html', data=metadata)


@app.route('/data_management_plan', methods=['POST'])
def data_management_plan():
    if request.method != 'POST':
        return redirect(url_for('not_found'))

    dmp_link = request.form.get('dmp_link')
    if dmp_link.startswith('/'):
        dmp_link = dmp_link[1:]
    try:
        return send_file(os.path.join(current_app.root_path, dmp_link),
                         attachment_filename=os.path.basename(dmp_link))
    except:
        return redirect(url_for('not_found'))


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
                            'is_main_award', pam.is_main_award, 'pi_name', a.name)) funding
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
                            ORDER BY pperm.oid) persons
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
                            SELECT proj_uid, json_agg(keyword_label) locations
                                FROM vw_project_location vdl
                                GROUP BY proj_uid
                        ) locations ON (p.proj_uid = locations.proj_uid)
                        LEFT JOIN (
                            SELECT pglm.proj_uid AS loc_proj_uid, json_agg(gl) gcmd_locations
                            FROM project_gcmd_location_map pglm JOIN gcmd_location gl ON (gl.id=pglm.loc_id)
                            GROUP BY pglm.proj_uid
                        ) gcmd_locations ON (p.proj_uid = gcmd_locations.loc_proj_uid)
                        LEFT JOIN ( 
                            SELECT k_1.proj_uid, string_agg(k_1.keywords, '; '::text) AS keywords
                            FROM (SELECT pskm.proj_uid, reverse(split_part(reverse(pskm.gcmd_key_id), ' >'::text, 1)) AS keywords
                                  FROM project_gcmd_science_key_map pskm
                                  UNION
                                  -- SELECT plm.proj_uid, reverse(split_part(reverse(plm.loc_id), ' >'::text, 1)) AS keywords
                                  -- FROM project_gcmd_location_map plm
                                  -- UNION
                                  SELECT pim.proj_uid, reverse(split_part(reverse(pim.gcmd_iso_id), ' >'::text, 1)) AS keywords
                                  FROM project_gcmd_isotopic_map pim
                                  UNION
                                  SELECT ppm.proj_uid, reverse(split_part(reverse(ppm.platform_id), ' >'::text, 1)) AS keywords
                                  FROM project_gcmd_platform_map ppm
                                  UNION
                                  SELECT pkm.proj_uid, ku.keyword_label AS keywords
                                  FROM project_keyword_map pkm JOIN keyword_usap ku ON (ku.keyword_id=pkm.keyword_id)
                                  UNION
                                  SELECT pkm.proj_uid, ki.keyword_label AS keywords
                                  FROM project_keyword_map pkm JOIN keyword_ieda ki ON (ki.keyword_id=pkm.keyword_id) 
                                  ) k_1
                            GROUP BY k_1.proj_uid
                        ) keywords ON keywords.proj_uid = p.proj_uid
                        WHERE p.proj_uid = '%s' ORDER BY p.title''' % project_id)
        cur.execute(query_string)
        return cur.fetchone()


#DEPRECATED
@app.route('/project_browser', methods=['GET', 'POST'])
def project_browser():

    if request.method == 'POST':
        params = request.form.to_dict()
        filtered = filter_datasets(**params)

        del params['spatial_bounds_interpolated']
        session['filtered_datasets'] = filtered
        session['search_params'] = params

    template_dict = {'pi_name': '', 'title': '', 'award': '', 'summary': ''}
    (conn, cur) = connect_to_db()

    query = "SELECT DISTINCT project.*, per.persons, a.awards, ST_AsText(psm.bounds_geometry) AS bounds_geometry " + \
            "FROM project " + \
            "LEFT JOIN project_spatial_map psm ON psm.proj_uid = project.proj_uid " + \
            "LEFT JOIN (" + \
            "SELECT pperm.proj_uid, string_agg(per.id, '; ') persons " + \
            "FROM project_person_map pperm JOIN person per ON per.id=pperm.person_id " + \
            "WHERE pperm.role ILIKE '%investigator%' " + \
            "GROUP BY pperm.proj_uid) per ON per.proj_uid = project.proj_uid " + \
            "LEFT JOIN ( " + \
            "SELECT pam.proj_uid, string_agg(a.award,'; ') awards " + \
            "FROM project_award_map pam JOIN award a ON a.award=pam.award_id " + \
            "GROUP BY pam.proj_uid) a ON a.proj_uid = project.proj_uid " + \
            "WHERE project.proj_uid !=''"

    if request.method == 'POST':
        template_dict['pi_name'] = request.form.get('pi_name')
        template_dict['title'] = request.form.get('title')
        template_dict['summary'] = request.form.get('summary')
        template_dict['award'] = request.form.get('award')

        if (request.form.get('pi_name') != ""):
            query += " AND per.persons ~* '%s'" % request.form['pi_name']
        if(request.form.get('title') != ""):
            query += " AND project.title ILIKE '%" + request.form['title'] + "%'"
        if(request.form.get('summary') != ""):
            query += " AND project.description ILIKE '%" + request.form['summary'] + "%'"
        if (request.form.get('award') != "" and request.form.get('award') != "Any award"):
            query += " AND a.awards ~* '%s'" % request.form['award']

    query += " ORDER BY project.date_created DESC"

    query_string = cur.mogrify(query)
    cur.execute(query_string)
    rows = cur.fetchall()

    for row in rows:
        authors = row['persons']
        row['authors'] = authors
        if row['awards'] != "":
            awards = row['awards'].split('; ')
            row['awards_7d'] = []
            for award in awards:
                row['awards_7d'].append("%07d" % int(award))
        ds_query = "SELECT * FROM project_dataset pd " + \
                   "LEFT JOIN project_dataset_map pdm ON pdm.dataset_id = pd.dataset_id " + \
                   "WHERE pdm.proj_uid = '%s'" % row['proj_uid']
        ds_query_string = cur.mogrify(ds_query)
        cur.execute(ds_query_string)
        datasets = cur.fetchall()
        row['datasets'] = datasets

    template_dict['proj_records'] = rows

    if template_dict['award'] == "":
        template_dict['award'] = "Any award"

    # get list of available options for drop downs and autocomplete
    query = "SELECT DISTINCT award_id as award FROM project_award_map ORDER BY award_id;"
    cur.execute(query)
    template_dict['awards'] = cur.fetchall()
    query = "SELECT DISTINCT title FROM project ORDER BY title;"
    cur.execute(query)
    template_dict['titles'] = cur.fetchall()
    query = "SELECT DISTINCT person_id as pi_name FROM project_person_map pperm " + \
            "WHERE pperm.role ILIKE '%investigator%' ORDER BY person_id;"
    cur.execute(query)
    template_dict['pi_names'] = cur.fetchall()
    template_dict['dif_ids'] = getFromDifTable('dif_id', True)

    return render_template('project_browser.html', **template_dict)


@app.route('/search', methods=['GET'])
def search():
    template_dict = {}

    # refresh the project view to make sure it is up to date
    (conn, cur) = connect_to_db()
    query = "REFRESH MATERIALIZED VIEW project_view; COMMIT;"
    cur.execute(query)

    params = request.args.to_dict()
    params['dp_type'] = 'Project'
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

    # refresh the project view to make sure it is up to date
    (conn, cur) = connect_to_db()
    query = "REFRESH MATERIALIZED VIEW dataset_view; COMMIT;"
    cur.execute(query)

    params = request.args.to_dict()
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
        titles.add(d['title'])
        if d.get('dataset_titles'):
            ds_titles = d['dataset_titles'].split(';')
            for ds in ds_titles:
                titles.add(ds)
        if d.get('project_titles'):
            p_titles = d['project_titles'].split(';')
            for p in p_titles:
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
        query_string = '''SELECT *,  ST_AsText(bounds_geometry) AS bounds_geometry FROM project_view dpv''' 
        titles = 'dataset_titles'
    elif dp_type == 'Dataset':
        d_or_p = 'projects'
        query_string = '''SELECT *,  bounds_geometry AS bounds_geometry FROM dataset_view dpv'''
        if spatial_bounds_interpolated:
            query_string += ''', text(JSON_ARRAY_ELEMENTS(dpv.bounds_geometry)) AS b'''
        titles = 'project_titles'
    else:
        return

    conds = []
    if uid:
        conds.append(cur.mogrify('dpv.uid = %s', (uid,)))
    if dp_title:
        conds.append("dpv.title ~* '%s' OR dpv.%s ~* '%s'" % (dp_title, titles, dp_title))
    if award:
        conds.append(cur.mogrify('dpv.awards ~* %s', (award,)))
    if person:
        conds.append(cur.mogrify('dpv.persons ~* %s', (person,)))
    if spatial_bounds_interpolated:
        conds.append(cur.mogrify("st_intersects(st_transform(st_geomfromewkt('srid=4326;'||replace(b,'\"','')),3031),st_geomfromewkt('srid=3031;'||%s))", (spatial_bounds_interpolated,)))
        conds.append("b is not null and b!= 'null'")
    if exclude:
        conds.append(cur.mogrify("NOT ((dpv.east=180 AND dpv.west=-180) OR (dpv.east=360 AND dpv.west=0))"))
    if sci_program:
        conds.append(cur.mogrify('dpv.science_programs = %s ', (sci_program,)))
    if nsf_program:
        conds.append(cur.mogrify('dpv.nsf_funding_programs = %s ', (nsf_program,)))
    # if dp_type and dp_type != 'Both':
    #     conds.append(cur.mogrify('dpv.type=%s ', (dp_type,)))
    # if location:
    #     conds.append(cur.mogrify('dpv.locations ~* %s ', (location,)))
    if free_text:
        conds.append(cur.mogrify("title ~* %s OR description ~* %s OR keywords ~* %s OR persons ~* %s OR " + d_or_p + " ~* %s", 
                                 (free_text, free_text, free_text, free_text, free_text)))
    if repo:
        conds.append(cur.mogrify('repositories = %s ', (repo,)))

    conds = ['(' + c + ')' for c in conds]
    if len(conds) > 0:
        query_string += ' WHERE ' + ' AND '.join(conds)

    cur.execute(query_string)
    return cur.fetchall()


def initcap(s):
    parts = re.split('( |_|-|>)+', s)
    return ' '.join([p.lower().capitalize() for p in parts])


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

if __name__ == "__main__":
    SECRET_KEY = 'development key'
    app.secret_key = SECRET_KEY
    app.run(host=app.config['SERVER_NAME'], debug=True, ssl_context=context, threaded=True)
