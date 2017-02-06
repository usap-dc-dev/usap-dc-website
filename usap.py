from __future__ import print_function

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import math
import flask
from flask import Flask, session, render_template, redirect, url_for, request, g, jsonify, flash, send_from_directory, send_file, current_app
from flask_session import Session
import random
from random import randint
from OpenSSL import SSL
import binascii, os
from flask_oauth import OAuth, OAuthException
import json
from urllib2 import Request, urlopen, URLError
import sys
from werkzeug.utils import secure_filename
import smtplib
from email.mime.text import MIMEText
import psycopg2
import psycopg2.extras
import requests
import re
import copy
import datetime
import csv
from collections import namedtuple
import string
import humanize
import urllib


app = Flask(__name__)

############
# Load configuration
############
app.config.update(
    SESSION_TYPE="filesystem",
    SESSION_FILE_DIR="flask_session",
    PERMANENT_SESSION_LIFETIME=1440,
    UPLOAD_FOLDER="upload",
    DATASET_FOLDER="dataset",
    DEBUG=False
)



app.config.update(json.loads(open('config.json','r').read()))


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


config = json.loads(open('config.json', 'r').read())

def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER'],
                            password=info['PASSWORD'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn,cur)

def get_nsf_grants(columns, award=None, only_inhabited=True):
    (conn,cur) = connect_to_db()
    query_string = 'SELECT %s FROM award a WHERE a.award != \'XXXXXXX\'' % ','.join(columns)
    if only_inhabited:
        query_string += ' AND EXISTS (SELECT award_id FROM dataset_award_map dam WHERE dam.award_id=a.award)'
    query_string +=  ' ORDER BY name,award'
    cur.execute(query_string)
    return cur.fetchall()

def filter_datasets(dataset_id=None, award=None, parameter=None, location=None, person=None, platform=None,
                    sensor=None, west=None,east=None,south=None,north=None, spatial_bounds=None, spatial_bounds_interpolated=None,start=None, stop=None, program=None,
                    title=None, limit=None, offset=None):
    (conn,cur) = connect_to_db()
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
                   '''
    conds = []
    if dataset_id:
        conds.append(cur.mogrify('d.id=%s',(dataset_id,)))
    if title:
        conds.append(cur.mogrify('d.title ILIKE %s',('%'+title+'%',)))
    if award:
        [num, name] = award.split(' ',1)
        conds.append(cur.mogrify('a.award=%s',(num,)))
        conds.append(cur.mogrify('a.name=%s',(name,)))
    if parameter:
        conds.append(cur.mogrify('par.id ILIKE %s',('%'+parameter+'%',)))
    if location:
        conds.append(cur.mogrify('l.id=%s',(location,)))
    if person:
        conds.append(cur.mogrify('per.id=%s',(person,)))
    if platform:
        conds.append(cur.mogrify('pl.id=%s',(platform,)))
    if sensor:
        conds.append(cur.mogrify('sen.id=%s',(sensor,)))
    if west:
        conds.append(cur.mogrify('%s <= sp.east', (west,)))
    if east:
        conds.append(cur.mogrify('%s >= sp.west', (east,)))
    if north:
        conds.append(cur.mogrify('%s >= sp.south', (north,)))
    if south:
        conds.append(cur.mogrify('%s <= sp.north', (south,)))
    if spatial_bounds_interpolated:
        bbox = "st_geomfromewkt('srid=4326;POLYGON ((' || sp.west || ' ' || sp.south || ', ' || sp.west || ' ' || sp.north || ', ' || sp.east || ' ' || sp.north || ', ' || sp.east || ' ' || sp.south || ', ' || sp.west || ' ' || sp.south || '))')"
        conds.append(cur.mogrify("st_intersects("+bbox+", st_transform(st_geomfromewkt('srid=3031;' || %s),4326))",(spatial_bounds_interpolated,)))
    if start:
        conds.append(cur.mogrify('%s <= tem.stop_date', (start,)))
    if stop:
        conds.append(cur.mogrify('%s >= tem.start_date', (stop,)))
    if program:
        conds.append(cur.mogrify('prog.id=%s ', (program,)))
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
        (conn,cur) = connect_to_db()
        query_string = \
                   cur.mogrify(
                       '''SELECT d.*,
                             CASE WHEN a.awards IS NULL THEN '[]'::json ELSE a.awards END,
                             CASE WHEN par.parameters IS NULL THEN '[]'::json ELSE par.parameters END,
                             CASE WHEN l.locations IS NULL THEN '[]'::json ELSE l.locations END,
                             CASE WHEN per.persons IS NULL THEN '[]'::json ELSE per.persons END,
                             CASE WHEN pl.platforms IS NULL THEN '[]'::json ELSE pl.platforms END,
                             CASE WHEN sen.sensors IS NULL THEN '[]'::json ELSE sen.sensors END,
                             CASE WHEN ref.references IS NULL THEN '[]'::json ELSE ref.references END,
                             CASE WHEN sp.spatial_extents IS NULL THEN '[]'::json ELSE sp.spatial_extents END,
                             CASE WHEN tem.temporal_extents IS NULL THEN '[]'::json ELSE tem.temporal_extents END,
                             CASE WHEN prog.programs IS NULL THEN '[]'::json ELSE prog.programs END
                       FROM
                        dataset d
                        LEFT JOIN (
                            SELECT dam.dataset_id, json_agg(a) awards
                            FROM dataset_award_map dam JOIN award a ON (a.award=dam.award_id)
                            WHERE a.award != 'XXXXXXX'
                            GROUP BY dam.dataset_id
                        ) a ON (d.id = a.dataset_id)
                        LEFT JOIN (
                            SELECT dkm.dataset_id, json_agg(k) keywords
                            FROM dataset_keyword_map dkm JOIN keyword k ON (k.id=dkm.keyword_id)
                            GROUP BY dkm.dataset_id
                        ) k ON (d.id = k.dataset_id)
                        LEFT JOIN (
                            SELECT dparm.dataset_id, json_agg(par) parameters
                            FROM dataset_parameter_map dparm JOIN parameter par ON (par.id=dparm.parameter_id)
                            GROUP BY dparm.dataset_id
                        ) par ON (d.id = par.dataset_id)
                        LEFT JOIN (
                            SELECT dlm.dataset_id, json_agg(l) locations
                            FROM dataset_location_map dlm JOIN location l ON (l.id=dlm.location_id)
                            GROUP BY dlm.dataset_id
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
                            SELECT ref.dataset_id, json_agg(ref) AS references
                            FROM dataset_reference_map ref
                            GROUP BY ref.dataset_id
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
                        WHERE d.id IN %s ORDER BY d.title''',
                       (tuple(dataset_ids),))
        cur.execute(query_string)
        return cur.fetchall()

# def get_dataset_old(dataset_id):
#     (conn, cur) = connect_to_db()
#     data = dict()
#     cur.execute('SELECT * FROM dataset d WHERE id=%s', (dataset_id,))
#     data.update(cur.fetchall()[0])
#     data['keywords'] = get_keywords(conn=conn, cur=cur, dataset_id=dataset_id)
#     data['parameters'] = get_parameters(conn=conn, cur=cur, dataset_id=dataset_id)
#     data['locations'] = get_locations(conn=conn, cur=cur, dataset_id=dataset_id)
#     data['persons'] = get_persons(conn=conn, cur=cur, dataset_id=dataset_id)
#     data['sensors'] = get_sensors(conn=conn, cur=cur, dataset_id=dataset_id)
#     data['references'] = get_references(conn=conn, cur=cur, dataset_id=dataset_id)
#     data['spatial_extents'] = get_spatial_extents(conn=conn, cur=cur, dataset_id=dataset_id)
#     data['temporal_extents'] = get_temporal_extents(conn=conn, cur=cur, dataset_id=dataset_id)
#     return data
    
def get_parameter_menu(conn=None, cur=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    cur.execute('SELECT * FROM parameter_menu')
    return cur.fetchall()

def get_location_menu(conn=None, cur=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    cur.execute('SELECT * FROM location_menu')
    return cur.fetchall()

def get_parameters(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT DISTINCT parameter_id AS id FROM dataset_parameter_map'
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
    query = 'SELECT DISTINCT location_id AS id FROM dataset_location_map'
    query += cur.mogrify(' ORDER BY id')
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

def get_persons(conn=None, cur=None, dataset_id=None):
    if not (conn and cur):
        (conn, cur) = connect_to_db()
    query = 'SELECT * FROM person'
    if dataset_id:
        query += cur.mogrify(' WHERE id in (SELECT person_id FROM dataset_person_map WHERE dataset_id=%s)', (dataset_id,))
    query += cur.mogrify(' ORDER BY id')
    cur.execute(query)
    return cur.fetchall()

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
    query = 'SELECT * FROM dataset_reference_map'
    if dataset_id:
        query += cur.mogrify(' WHERE dataset_id=%s', (dataset_id,))
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

@app.route('/submit/dataset', methods=['GET','POST'])
def dataset():
    user_info = session.get('user_info')
    if user_info is None:
        session['next'] = '/submit/dataset'
        return redirect(url_for('login'))

    if request.method == 'POST':
        if 'dataset_metadata' not in session:
            session['dataset_metadata'] = dict()
        session['dataset_metadata'].update(request.form.to_dict())
        session['dataset_metadata']['agree'] = 'agree' in request.form
        flash('test message')
        return redirect('/submit/dataset2')

    else:
        return '\n'.join([render_template('header.jnj',cur='dataset'),
                          render_template('dataset.jnj',name=user_info['name'],dataset_metadata=session.get('dataset_metadata',dict()),nsf_grants=get_nsf_grants(['award','name','title'],only_inhabited=False)),
                          render_template('footer.jnj')])

class ExceptionWithRedirect(Exception):
    def __init__(self, message, redirect):
        self.redirect = redirect
        super(ExceptionWithRedirect, self).__init__(message)
    
class BadSubmission(ExceptionWithRedirect):
    pass
        
class CaptchaException(ExceptionWithRedirect):
    pass

class InvalidDatasetException(ExceptionWithRedirect):
    def __init__(self,msg='Invalid Dataset ID',redirect='/'):
        super(InvalidDatasetException, self).__init__(msg,redirect)
    

@app.errorhandler(CaptchaException)
def failed_captcha(e):
    return '\n'.join([render_template('header.jnj',cur='failed_captcha'),
                      render_template('error.jnj',error_message=str(e)),
                      render_template('footer.jnj')])

@app.errorhandler(BadSubmission)
def invalid_dataset(e):
    return '\n'.join([render_template('header.jnj',cur='dataset_error'),
                      render_template('error.jnj',error_message=str(e),back_url=e.redirect,name=session['user_info']['name']),
                      render_template('footer.jnj')])

#@app.errorhandler(OAuthException)
def oauth_error(e):
    return '\n'.join([render_template('header.jnj',cur='dataset_error'),
                      render_template('error.jnj',error_message=str(e)),
                      render_template('footer.jnj')])

#@app.errorhandler(Exception)
def general_error(e):
    return '\n'.join([render_template('header.jnj',cur='_error'),
                      render_template('error.jnj',error_message=str(e)),
                      render_template('footer.jnj')])

@app.route('/thank_you/<submission_type>')
def thank_you(submission_type):
    return '\n'.join([render_template('header.jnj',cur='thank_you'),
                      render_template('thank_you.jnj',name=session['user_info']['name'],submission_type=submission_type),
                      render_template('footer.jnj')])

Validator = namedtuple('Validator', ['func', 'msg'])

def check_dataset_submission(msg_data):
    def default_func(field):
        return lambda data: field in data and bool(data[field])
    def check_spatial_bounds(data):
        try:
            return \
                abs(float(data['geo_w'])) <= 180 and \
                abs(float(data['geo_e'])) <= 180 and \
                abs(float(data['geo_n'])) <= 90 and abs(float(data['geo_s'])) <= 90
        except:
            return False
    
    validators = [
        Validator(func=default_func('agree'),msg='You must agree to have your files posted with a DOI.'),
        Validator(func=default_func('filenames'),msg='You must include files in your submission.'),
        Validator(func=default_func('award'),msg='You must select an NSF grant for the submission'),
        Validator(func=check_spatial_bounds, msg="Spatial bounds are invalid")
    ]
    for v in validators:
        if not v.func(msg_data):
            raise BadSubmission(v.msg,'/submit/dataset')

@app.route('/repo_list')
def repo_list():
    return '\n'.join([render_template('header.jnj',cur='repo_list'),
                      render_template('repo_list.jnj'),
                      render_template('footer.jnj')])
    
        
def check_project_registration(msg_data):
    def default_func(field):
        return lambda data: field in data and bool(data[field])
    
    validators = [
        Validator(func=default_func('award'),msg="You need to select an award #"),
        Validator(func=default_func("title"),msg="You need to provide a title for the project"),
        Validator(func=default_func("name"),msg="You need to provide the PI's name for the project"),
        Validator(func=lambda data: 'repos' in data and (len(data['repos'])>0 or data['repos'] == 'nodata'),msg="You need to provide info about the repository where you submitted the dataset"),
        Validator(func=lambda data: 'locations' in data and len(data['locations'])>0,msg="You need to provide at least one location term"),
        Validator(func=lambda data: 'parameters' in data and len(data['parameters'])>0,msg="You need to provide at least one keyword term")
    ]
    for v in validators:
        if not v.func(msg_data):
            raise BadSubmission(v.msg,'/submit/project')
    
    
@app.route('/submit/dataset2',methods=['GET','POST'])
def dataset2():
    user_info = session.get('user_info')
    if user_info is None:
        session['next'] = '/submit/dataset2'

    if request.method == 'POST':
        if 'dataset_metadata' not in session:
            session['dataset_metadata'] = dict()
        session['dataset_metadata'].update(request.form.to_dict())
        session['dataset_metadata']['properGeoreferences'] = 'properGeoreferences' in request.form
        session['dataset_metadata']['propertiesExplained'] = 'propertiesExplained' in request.form
        session['dataset_metadata']['comprehensiveLegends'] = 'comprehensiveLegends' in request.form
        if request.form.get('action') == 'Submit':                    
            msg_data = copy.copy(session['dataset_metadata'])
            msg_data['name'] = session['user_info']['name']
            del msg_data['action']
            if 'orcid' in session['user_info']:
                msg_data['orcid'] = session['user_info']['orcid']
            if 'email' in session['user_info']:
                msg_data['email'] = session['user_info']['email']

            files = request.files.getlist('file[]')
            fnames = dict()
            for f in files:
                fname = secure_filename(f.filename)
                if len(fname) > 0:
                    fnames[fname] = f

            msg_data['filenames'] = fnames.keys()
            timestamp = datetime.datetime.now().isoformat()
            msg_data['timestamp'] = timestamp
            check_dataset_submission(msg_data)
            
            nsfid = 'NSF' + msg_data['award'].split(' ')[0]
            upload_dir = os.path.join(current_app.root_path, app.config['UPLOAD_FOLDER'], timestamp)
            msg_data['upload_directory'] = upload_dir
            if not os.path.exists(upload_dir):
                os.makedirs(upload_dir)
                      
            for fname,fobj in fnames.items():
                fobj.save(os.path.join(upload_dir, fname))

            msg = MIMEText(json.dumps(msg_data, indent=4, sort_keys=True))
            sender = 'web@usap-dc.org'
            recipients = ['web@usap-dc.org','lagrange@ldeo.columbia.edu']
            msg['Subject'] = 'Dataset Submission'
            msg['From'] = sender
            msg['To'] = ', '.join(recipients)

            s = smtplib.SMTP('***REMOVED***')
            s.login(sender, '***REMOVED***')
            s.sendmail(sender, recipients, msg.as_string())
            s.quit()
                
            return redirect('/thank_you/dataset')
        elif request.form.get('action') == 'Previous Page':
            return redirect('/submit/dataset')
    else:
        return '\n'.join([render_template('header.jnj',cur='dataset'),
                          render_template('dataset2.jnj',name=user_info['name'],dataset_metadata=session.get('dataset_metadata', dict())),
                          render_template('footer.jnj')])

@app.route('/submit/project',methods=['GET','POST'])
def project():
    user_info = session.get('user_info')
    if user_info is None:
        session['next'] = '/submit/project'
        return redirect(url_for('login'))

    if request.method == 'POST':
        msg_data = dict()
        msg_data['name'] = session['user_info']['name']
        if 'orcid' in session['user_info']:
            msg_data['orcid'] = session['user_info']['orcid']
        if 'email' in session['user_info']:
            msg_data['email'] = session['user_info']['email']
        msg_data.update(request.form.to_dict())
        
        parameters = []
        idx = 1
        key = 'parameter1'
        while key in msg_data:
            parameters.append(msg_data[key])
            del msg_data[key]
            idx += 1
            key = 'parameter'+str(idx)
        msg_data['parameters'] = parameters
            
        locations = []
        idx = 1
        key = 'location1'
        while key in msg_data:
            locations.append(msg_data[key])
            del msg_data[key]
            idx += 1
            key = 'location'+str(idx)
        msg_data['locations'] = locations

        if 'nodata' in msg_data:
            repos = 'nodata'
            del msg_data['nodata']
        else:
            repos = []
            idx = 1
            key = 'repo1'
            while key in msg_data:
                if msg_data[key] == 'USAP-DC':
                    repos.append({'name':'USAP-DC'})
                else:
                    d = dict()
                    if 'repo_name_other'+str(idx) in msg_data:
                        d['name'] = msg_data['repo_name_other'+str(idx)]
                    if 'repo_id_other'+str(idx) in msg_data:
                        d['id'] = msg_data['repo_id_other'+str(idx)]
                    repos.append(d)
                idx+=1
                key = 'repo'+str(idx)
        msg_data = {k:v for k,v in msg_data.iteritems() if k[:4] != 'repo'}   
        msg_data['repos'] = repos
        
        msg_data['timestamp'] = datetime.datetime.now().isoformat()
        msg = MIMEText(json.dumps(msg_data, indent=4, sort_keys=True))
        check_project_registration(msg_data)
        from_addr = 'web@usap-dc.org'
        #to_addr = 'web@usap-dc.org'
        to_addr = 'lagrange@ldeo.columbia.edu'
        msg['Subject'] = 'Dataset Registration'
        msg['From'] = from_addr
        msg['To'] = to_addr

        s = smtplib.SMTP('***REMOVED***')
        s.login(from_addr, '***REMOVED***')
        s.sendmail(from_addr, [to_addr], msg.as_string())
        s.quit()
        
        return redirect('thank_you/project')
    else:
        return '\n'.join([render_template('header.jnj',cur='project'),
                          render_template('project.jnj',name=user_info['name'],nsf_grants=get_nsf_grants(['award','name'],only_inhabited=False), locations=get_location_menu(), parameters=get_parameter_menu()),
                          render_template('footer.jnj')])

#@app.route('/submit/projectinfo',methods=['GET'])
#def projectinfo():
#    grant = get_nsf_grants(['*'], award=request.args.get('award'))
#    return flask.jsonify(grant[0])

@app.route('/login')
def login():
    return '\n'.join([render_template('header.jnj',cur='login'),
                      render_template('login.jnj'),
                      render_template('footer.jnj')])

@app.route('/login_google')
def login_google():
    callback=url_for('authorized', _external=True)
    return google.authorize(callback=callback)

@app.route('/login_orcid')
def login_orcid():
    return redirect('https://orcid.org/oauth/authorize?client_id=APP-699JD0NNLULN5MIL&response_type=code&scope=/authenticate&redirect_uri=http://www-dev.usap-dc.org/authorized_orcid')

@app.route('/authorized')
@google.authorized_handler
def authorized(resp):
    access_token = resp['access_token']
    session['google_access_token'] = access_token, ''
    headers = {'Authorization': 'OAuth '+access_token}
    req = Request('https://www.googleapis.com/oauth2/v1/userinfo',
                  None, headers)
    res = urlopen(req)
    session['user_info'] = json.loads(res.read())
    return redirect(session['next'])

@app.route('/authorized_orcid')
def authorized_orcid():
    code = request.args['code']
    p = requests.post('https://orcid.org/oauth/token', data={'client_id':'APP-699JD0NNLULN5MIL', 'client_secret':'bd82034f-dada-4b5e-b216-c374a7e3b342','grant_type':'authorization_code','code':code,'redirect_uri':'http://www-dev.usap-dc.org/authorized_orcid'}, headers={'accept': 'application/json'}).json()
    access_token = p['access_token']
    session['orcid_access_token'] = access_token
    r = requests.get('https://pub.orcid.org/v1.2/search/orcid-bio/?q=orcid:'+p['orcid'], headers={'accept': 'application/json'}).json()
    bio = r['orcid-search-results']['orcid-search-result'][0]['orcid-profile']['orcid-bio']
    resp = flask.Response(json.dumps(bio,indent=4))
    resp.headers['Content-Type'] = 'application/json'
    session['user_info'] = {
        'name': bio['personal-details']['given-names']['value'] + ' ' + bio['personal-details']['family-name']['value'],
        'orcid': p['orcid']
    }
    return redirect(session['next'])

    
@google.tokengetter
def get_access_token():
    return session.get('google_access_token')


@app.route('/logout')
def logout():
    if 'user_info' in session:
        del session['user_info']
    if 'google_access_token' in session:
        del session['google_access_token']
    if 'orcid_access_token' in session:
        del session['orcid_access_token']
    if 'dataset_metadata' in session:
        del session['dataset_metadata']
    return redirect(url_for('submit'))


@app.route("/index")
@app.route("/")
def home():
    return '\n'.join([render_template('header.jnj',cur='home'),
                      render_template('home.jnj'),
                      render_template('footer.jnj')])

@app.route("/home2")
def home2():
    return '\n'.join([render_template('header2.jnj',cur='home'),
                      render_template('home.jnj'),
                      render_template('footer.jnj')])

@app.route('/overview')
def overview():
    return '\n'.join([render_template('header.jnj',cur='overview'),
                      render_template('overview.jnj'),
                      render_template('footer.jnj')])

@app.route('/links')
def links():
    return '\n'.join([render_template('header.jnj',cur='links'),
                      render_template('links.jnj'),
                      render_template('footer.jnj')])

@app.route('/search', methods=['GET', 'POST'])
def search():
    if request.method == 'GET':
        return '\n'.join([render_template('header.jnj',cur='search'),
                          render_template('search.jnj', search_params=session.get('search_params'), nsf_grants=get_nsf_grants(['award','name','title']), keywords=get_keywords(),
                                          parameters=get_parameters(), locations=get_locations(), platforms=get_platforms(),
                                          persons=get_persons(), sensors=get_sensors(), programs=get_programs(), titles=get_titles()),
                          render_template('footer.jnj')])
    elif request.method == 'POST':
        params = request.form.to_dict()

        filtered = filter_datasets(**params)

        del params['spatial_bounds_interpolated']
        session['filtered_datasets'] = filtered
        session['search_params'] = params
        
        return redirect('/search_result')

@app.route('/filter_search_menus', methods=['GET'])
def filter_search_menus():
    keys = ['person', 'parameter', 'program', 'award', 'title']
    args = request.args.to_dict()

    person_ids = filter_datasets(**{ k: args.get(k) for k in keys if k != 'person'})
    person_dsets = get_datasets(person_ids)
    persons = set([p['id'] for d in person_dsets for p in d['persons']])

    parameter_ids = filter_datasets(**{ k: args.get(k) for k in keys if k != 'parameter'})
    parameter_dsets = get_datasets(parameter_ids)
    parameters = set([' > '.join(p['id'].split(' > ')[2:]) for d in parameter_dsets for p in d['parameters']])

    program_ids = filter_datasets(**{ k: args.get(k) for k in keys if k != 'program'})
    program_dsets = get_datasets(program_ids)
    programs = set([p['id'] for d in program_dsets for p in d['programs']])
    
    award_ids = filter_datasets(**{ k: args.get(k) for k in keys if k != 'award'})
    award_dsets = get_datasets(award_ids)
    awards = set([(p['name'],p['award']) for d in award_dsets for p in d['awards']])

    return flask.jsonify({
        'person': sorted(persons),
        'parameter': sorted(parameters),
        'program': sorted(programs),
        'award': [a[1] + ' ' + a[0] for a in sorted(awards)],
    })
    
@app.route('/search_result')
def search_result():
    if 'filtered_datasets' not in session:
        return redirect('/search')
    filtered_ids = session['filtered_datasets']
    
    datasets = get_datasets(filtered_ids)
    grp_size = 50
    dataset_grps = []
    cur_grp = []
    for d in datasets:
        if len(cur_grp) < grp_size:
            cur_grp.append(d)
        else:
            dataset_grps.append(cur_grp)
            cur_grp = []
    if len(cur_grp) > 0:
        dataset_grps.append(cur_grp)
    
    
    return '\n'.join([render_template('header.jnj',cur='search_result'),
                      render_template('search_result.jnj',
                                      total_count=len(filtered_ids),
                                      dataset_grps=dataset_grps,
                                      search_params=session['search_params']),
                      render_template('footer.jnj')])
        

@app.route('/contact', methods=['GET', 'POST'])
def contact():
    if request.method == 'GET':
        return '\n'.join([render_template('header.jnj',cur='contact'),
                          render_template('contact.jnj'),
                          render_template('footer.jnj')])
    elif request.method == 'POST':
        form = request.form.to_dict()
        g_recaptcha_response = form.get('g-recaptcha-response')
        remoteip = request.remote_addr
        resp = requests.post('https://www.google.com/recaptcha/api/siteverify',
                             data={'response':g_recaptcha_response,
                                   'remoteip':remoteip,
                                   'secret': app.config['RECAPTCHA_SECRET_KEY']}).json()
        if resp.get('success'):
            sender = form['email']
            recipients = ['web@usap-dc.org','lagrange@ldeo.columbia.edu']
            msg = MIMEText(form['msg'])
            msg['Subject'] = form['subj']
            msg['From'] = sender
            msg['To'] = recipients
            s = smtplib.SMTP('***REMOVED***')
            s.login('web@usap-dc.org', '***REMOVED***')
            s.sendmail(sender, recipients, msg.as_string())
            s.quit()
            return redirect('/thank_you/email')
        else:
            msg = "<br/>You failed to pass the captcha<br/>"
            raise CaptchaException(msg,url_for('contact'))

@app.route('/submit')
def submit():
    return '\n'.join([render_template('header.jnj',cur='submit'),
                      render_template('submit.jnj'),
                      render_template('footer.jnj')])

@app.route('/data_repo')
def data_repo():
    return '\n'.join([render_template('header.jnj',cur='data_repo'),
                      render_template('data_repo.jnj'),
                      render_template('footer.jnj')])

@app.route('/devices')
def devices():
    user_info = session.get('user_info')
    if user_info is None:
        session['next'] = '/devices'
        return redirect(url_for('login'))
    else:
        return '\n'.join([render_template('header.jnj', cur='dataset'),
                          render_template('device_examples.jnj', name=user_info['name']),
                          render_template('footer.jnj')])

@app.route('/procedures')
def procedures():
    user_info = session.get('user_info')
    if user_info is None:
        session['next'] = '/devices'
        return redirect(url_for('login'))
    else:
        return '\n'.join([render_template('header.jnj', cur='dataset'),
                          render_template('procedure_examples.jnj', name=user_info['name']),
                          render_template('footer.jnj')])

@app.route('/content')
def content():
    user_info = session.get('user_info')
    if user_info is None:
        session['next'] = '/devices'
        return redirect(url_for('login'))
    else:
        return '\n'.join([render_template('header.jnj', cur='dataset'),
                          render_template('content_examples.jnj', name=user_info['name']),
                          render_template('footer.jnj')])

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    return str(obj)

@app.route('/view/dataset/<dataset_id>')
def landing_page(dataset_id):
    datasets = get_datasets([dataset_id])
    if len(datasets) == 0:
        raise InvalidDatasetException()
    metadata = datasets[0]
    url = metadata['url']
    if not url:
        raise InvalidDatasetException()

    usap_domain = 'http://www-dev.usap-dc.org/'
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
    else:
        metadata['files'] = [{'url': url, 'name': os.path.basename(os.path.normpath(url))}]

    if metadata.get('url_extra'):
        metadata['url_extra'] = os.path.basename(metadata['url_extra'])
        
    creator_orcid = None
    
    for p in metadata['persons'] or []:
        if p['id'] == metadata['creator'] and p['orcid'] is not None:
            creator_orcid = p['orcid']
            break
    return '\n'.join([render_template('header.jnj',cur='landing_page'),
                      render_template('landing_page.jnj',data=metadata,creator_orcid=creator_orcid),
                      render_template('footer.jnj')])

@app.route('/dataset/<path:filename>')
def file_download(filename):
    directory = os.path.join(current_app.root_path, app.config['DATASET_FOLDER'])
    return send_from_directory(directory, filename, as_attachment=True)

@app.route('/supplement/<dataset_id>')
def supplement(dataset_id):
    (conn, cur) = connect_to_db()
    cur.execute('''SELECT url_extra FROM dataset WHERE id=%s''', (dataset_id,))
    url_extra = cur.fetchall()[0]['url_extra'][1:]
    if url_extra.startswith('/'):
        url_extra = url_extra[1:]
    return send_file(os.path.join(current_app.root_path, url_extra),
                     as_attachment=True,
                     attachment_filename=os.path.basename(url_extra))

@app.route('/mapserver-template.html')
def mapserver_template():
    return render_template('mapserver-template.html')

@app.route('/getfeatureinfo')
def getfeatureinfo():
    url = urllib.unquote('http://api.usap-dc.org:81/wfs?' + urllib.urlencode(request.args))
    return requests.get(url).text

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
           WHERE st_within(st_transform(dspm.geometry,3031),st_geomfromewkt('srid=3031;'||%s))) foo GROUP BY program''',(wkt,))
    cur.execute(query)
    return flask.jsonify(cur.fetchall())

@app.route('/titles')
def titles():
    term = request.args.get('term')
    (conn,cur) = connect_to_db()
    query_string = cur.mogrify('SELECT title FROM dataset WHERE title ILIKE %s', ('%' + term + '%',));
    cur.execute(query_string)
    rows = cur.fetchall()
    titles = []
    for r in rows:
        titles.append(r['title'])
    return flask.jsonify(titles)

@app.route('/geometries')
def geometries():
    (conn,cur) = connect_to_db()
    query = "SELECT st_asgeojson(st_transform(st_geomfromewkt('srid=4326;' || 'POLYGON((' || west || ' ' || south || ', ' || west || ' ' || north || ', ' || east || ' ' || north || ', ' || east || ' ' || south || ', ' || west || ' ' || south || '))'),3031)) as geom FROM dataset_spatial_map;"
    cur.execute(query)
    return flask.jsonify([row['geom'] for row in cur.fetchall()])

@app.route('/parameter_search')
def parameter_search():
    (conn,cur) = connect_to_db()
    expr = '%'+request.args.get('term')+'%'
    query = cur.mogrify("SELECT id FROM parameter WHERE category ILIKE %s OR topic ILIKE %s OR term ILIKE %s OR varlev1 ILIKE %s OR varlev2 ILIKE %s OR varlev3 ILIKE %s", (expr,expr,expr,expr,expr,expr))
    cur.execute(query)
    return flask.jsonify([row['id'] for row in cur.fetchall()])

@app.route('/test_autocomplete')
def test_autocomplete():
    return '\n'.join([render_template('header.jnj'),
                      render_template('test_autocomplete.jnj'),
                      render_template('footer.jnj')])

@app.route('/dataset_json/<dataset_id>')
def dataset_json(dataset_id):
    return flask.jsonify(get_datasets([dataset_id]))

def initcap(s):
    parts = re.split('( |_|-|>)+',s)
    return ' '.join([p.lower().capitalize() for p in parts])


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
    app.secret_key = '702c9c7895652021199d367965a074f36b0f6959992af982'
    app.run(host='www-dev.usap-dc.org', debug=True, ssl_context=context, threaded=True)
