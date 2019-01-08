import json
import xml.dom.minidom as minidom
import os
import psycopg2
import psycopg2.extras
import sys
import requests
from flask import session, url_for
from subprocess import Popen, PIPE
from json2sql import makeBoundsGeom
import base64

UPLOAD_FOLDER = "upload"
DATASET_FOLDER = "dataset"
SUBMITTED_FOLDER = "submitted"
DCXML_FOLDER = "watch/dcxml"
ISOXML_FOLDER = "watch/isoxml"
DOCS_FOLDER = "doc"
AWARDS_FOLDER = "awards"
DOI_REF_FILE = "inc/doi_ref"
CURATORS_LIST = "inc/curators.txt"
DATACITE_CONFIG = "inc/datacite.json"
DATACITE_TO_ISO_XSLT = "static/DataciteToISO19139v3.2.xslt"
ISOXML_SCRIPT = "bin/makeISOXMLFile.py"
PYTHON = "/opt/rh/python27/root/usr/bin/python"
LD_LIBRARY_PATH = "/opt/rh/python27/root/usr/lib64"
PROJECT_DATASET_ID_FILE = "inc/proj_ds_ref"

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


def submitToDataCite(uid):
    datacite_file = getDCXMLFileName(uid)
    # Read in Datacite connection details
    try:
        with open(DATACITE_CONFIG) as dc_file:
            dc_details = json.load(dc_file)
    except Exception as e:
        return("Error opening Datacite config file: %s" % str(e))

    try:
        doi = dc_details['SHOULDER'] + uid
        landing_page = url_for("landing_page", dataset_id=uid, _external=True)
    except Exception as e:
        return("Error generating DOI: %s" % str(e))
 
    # read in datacite xml file
    try:
        with open(datacite_file, "r") as f:
            xml = f.read()
    except Exception as e:
        return("Error reading DataCite XML file: %s" % str(e))

    # apply base64 encoding
    try:
        xml_b64 = base64.b64encode(xml)
    except Exception as e:
        return("Error encoding DataCite XML: %s" % str(e))

    # create the DOI json object
    try:
        doi_json = {"data": {
                    "type": "dois",
                    "attributes": {
                        "doi": doi,
                        "event": "publish",
                        "url": landing_page,
                        "xml": xml_b64
                    },
                    "relationships": {
                        "client": {
                            "data": {
                                "type": "clients",
                                "id": dc_details['USER']
                            }
                        }
                    }
                    }
                    }
    except Exception as e:
        return("Error generating DOI json object: %s" % str(e))

    # Send DOI Create request to DataCite
    try:
        headers = {'Content-Type': 'application/vnd.api+json'}
        response = requests.post(dc_details['SERVER'], headers=headers, data=json.dumps(doi_json), auth=(dc_details['USER'], dc_details['PASSWORD']))

        if response.status_code != 201:
            return("Error with request to create DOI at DataCite.  Status code: %s\n Error: %s" % (response.status_code, response.json()))

        return("Successfully registered dataset at DataCite, DOI: %s" % doi)

    except Exception as e:
        return("Error generating DOI: %s" % str(e))


def getDataCiteXML(uid):
    print('in getDataCiteXML')
    status = 1
    (conn, cur) = connect_to_db()
    if type(conn) is str:
        out_text = conn
    else:
        # query the database to get the XML for the submission ID
        try:
            sql_cmd = '''SELECT datacitexml FROM generate_datacite_xml WHERE id='%s';''' % uid
            cur.execute(sql_cmd)
            res = cur.fetchone()
            xml = minidom.parseString(res['datacitexml'])
            out_text = xml.toprettyxml().encode('utf-8').strip()
        except:
            out_text = "Error running database query. \n%s" % sys.exc_info()[1][0]
            print(out_text)
            status = 0

    # write the xml to a temporary file
    xml_file = getDCXMLFileName(uid)
    with open(xml_file, "w") as myfile:
        myfile.write(out_text)
    os.chmod(xml_file, 0o664)
    return(xml_file, status)


def getDataCiteXMLFromFile(uid):
    dcxml_file = getDCXMLFileName(uid)
    # check if datacite xml file already exists
    if os.path.exists(dcxml_file):
        try:
            with open(dcxml_file) as infile:
                dcxml = infile.read()
            return dcxml
        except:
            return "Error reading DataCite XML file."
    return "Will be generated after Database import"


def getDCXMLFileName(uid):
    return os.path.join(DCXML_FOLDER, uid)


def getISOXMLFromFile(uid):
    isoxml_file = getISOXMLFileName(uid)
    # check if datacite xml file already exists
    if not os.path.exists(isoxml_file):
        msg = doISOXML(uid)
        if msg.find("Error") >= 0:
            return msg
    try:
        with open(isoxml_file) as infile:
            isoxml = infile.read()
        return isoxml
    except:
        return "Error reading ISO XML file."
    return "Will be generated after Database import"


def getISOXMLFileName(uid):
    return os.path.join(ISOXML_FOLDER, "%siso.xml" % uid)


def isRegisteredWithDataCite(uid):
    with open(DATACITE_CONFIG) as dc_file:
        dc_details = json.load(dc_file)
    id = dc_details['SHOULDER'] + uid
    dc_url = dc_details['SERVER'] + '/' + id
    r = requests.get(dc_url)
    return r.status_code == 200


def doISOXML(uid):
    # get datacite XML
    xml_filename = getDCXMLFileName(uid)
    if not os.path.exists(xml_filename):
        xml_filename, status = getDataCiteXML(uid)
        if status == 0:
            return "Error obtaining DataCite XML file"

    try:
        # convert to ISO XML by running through xslt
        xsl_filename = DATACITE_TO_ISO_XSLT
        isoxml_filename = getISOXMLFileName(uid)

        os.environ['LD_LIBRARY_PATH'] = LD_LIBRARY_PATH
        # need to run external script as lxml module doesn't seem to work when running with apache
        process = Popen([PYTHON, ISOXML_SCRIPT, xml_filename, xsl_filename, isoxml_filename], stdout=PIPE)
        (output, err) = process.communicate()
        if err:
            return "Error making ISO XML file.  %s" % err
        return output

    except Exception as e:
        return "Error making ISO XML file.  %s" % str(e)


def ISOXMLExists(uid):
    return os.path.exists(getISOXMLFileName(uid))


# def copyISOXMLFile(isoxml_file):
#         ISO = json.loads(open(ISO_WATCHDIR_CONFIG_FILE, 'r').read())
#         new_file_name = os.path.join(ISO['ISO_WATCH_DIR'], isoxml_file.split('/')[-1])
#         cmd = "scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o GlobalKnownHostsFile=/dev/null %s %s@%s:%s" %\
#             (isoxml_file, ISO['REMOTE_USER'], ISO['REMOTE_HOST'], new_file_name)
#         print(cmd)
#         return os.system(cmd.encode('utf-8'))
#         os.remove(isoxml_file)


def isCurator():
    if session.get('user_info') is None:
        return False
    userid = session['user_info'].get('id')
    if userid is None:
        userid = session['user_info'].get('orcid')
    curator_file = open(CURATORS_LIST, 'r')
    curators = curator_file.read().split('\n')
    return userid in curators


def addKeywordsToDatabase(uid, keywords):
    (conn, cur) = connect_to_db()
    status = 1
    if type(conn) is str:
        out_text = conn
        status = 0
    else:
        # query the database to get the XML for the submission ID
        try:
            for kw in keywords:
                # check for new keywords to be added. These will have an id that starts with 'nk'
                print(kw)
                if kw[0:2] == 'nk':
                    # kw will contiain all the information needed to create the keyword entry
                    # in the keyword_usap table, using ::: as a separator
                    (kw_id, label, description, type_id, source) = kw.split(':::')
                    # first work out the last keyword_id used
                    query = "SELECT keyword_id FROM keyword_usap ORDER BY keyword_id DESC"
                    cur.execute(query)
                    res = cur.fetchone()
                    last_id = res['keyword_id'].replace('uk-', '')
                    next_id = int(last_id) + 1
                    # now insert new record into db
                    sql_cmd = "INSERT INTO keyword_usap (keyword_id, keyword_label, keyword_description, keyword_type_id, source) VALUES ('uk-%s', '%s', '%s', '%s', '%s');" % \
                        (next_id, label, description, type_id, source)
                    print(sql_cmd)
                    cur.execute(sql_cmd)
                    kw = 'uk-' + str(next_id)

                # update dataset_keyword_map 
                sql_cmd = "INSERT INTO dataset_keyword_map(dataset_id,  keyword_id) VALUES ('%s','%s');" % (uid, kw)
                print(sql_cmd)
                cur.execute(sql_cmd)
            cur.execute('COMMIT;')
            out_text = "Keywords successfully assigned in database"    
        except:
            out_text = "Error running database query. \n%s" % sys.exc_info()[1][0]
            print(out_text)
            status = 0

    return (out_text, status)


def isDatabaseImported(uid):
    (conn, cur) = connect_to_db()
    query = "SELECT COUNT(*) from dataset WHERE id = '%s'" % uid
    cur.execute(query)
    res = cur.fetchone()
    return res['count'] > 0


def updateSpatialMap(uid, data):
    (conn, cur) = connect_to_db()
    status = 1
    if type(conn) is str:
        out_text = conn
        status = 0
    else:
        # query the database to get the XML for the submission ID
        try:
            if (data["geo_w"] != '' and data["geo_e"] != '' and data["geo_s"] != '' and data["geo_n"] != '' and data["cross_dateline"] != ''):
                west = float(data["geo_w"])
                east = float(data["geo_e"])
                south = float(data["geo_s"])
                north = float(data["geo_n"])
                mid_point_lat = (south - north) / 2 + north
                mid_point_long = (east - west) / 2 + west

                geometry = "ST_GeomFromText('POINT(%s %s)', 4326)" % (mid_point_long, mid_point_lat)
                bounds_geometry = "ST_GeomFromText('%s', 4326)" % makeBoundsGeom(north, south, east, west, data["cross_dateline"])

                # update record if already in the database
                query = "SELECT COUNT(*) FROM dataset_spatial_map WHERE dataset_id = '%s';" % uid
                cur.execute(query)
                count = cur.fetchone()
                if count['count'] > 0:
                    sql_cmd = "UPDATE dataset_spatial_map SET (west,east,south,north,cross_dateline, geometry, bounds_geometry) = (%s, %s, %s, %s, %s, %s, %s) WHERE dataset_id = '%s';"\
                        % (west, east, south, north, data['cross_dateline'], geometry, bounds_geometry, uid)
                    out_text = "Spatial bounds successfully updated"
                # otherwise insert record
                else:
                    sql_cmd = "INSERT INTO dataset_spatial_map (dataset_id, west,east,south,north,cross_dateline, geometry, bounds_geometry) VALUES ('%s', %s, %s, %s, %s, %s, %s, %s);"\
                        % (uid, west, east, south, north, data['cross_dateline'], geometry, bounds_geometry)
                    out_text = "Spatial bounds successfully inserted"
                print(sql_cmd)
                cur.execute(sql_cmd)
                cur.execute('COMMIT;')
                
            else:
                out_text = "Error updating spatial bounds. Not all coordinate information present"
                status = 0
        except:
            out_text = "Error updating database. \n%s" % sys.exc_info()[1][0]
            print(out_text)
            status = 0

    return (out_text, status)


def getCoordsFromDatabase(uid):
    (conn, cur) = connect_to_db()
    query = "SELECT north as geo_n, east as geo_e, south as geo_s, west as geo_w, cross_dateline FROM dataset_spatial_map WHERE dataset_id = '%s';" % uid
    cur.execute(query)
    return cur.fetchone()


def getKeywordsFromDatabase():
    # for each keyword_type, get all keywords from database  
    (conn, cur) = connect_to_db()
    query = "SELECT REPLACE(keyword_type_id, '-', '_') AS id, * FROM keyword_type;"
    cur.execute(query)
    keyword_types = cur.fetchall()
    for kw_type in keyword_types:
        query = "SELECT keyword_id, keyword_label, keyword_description FROM keyword_ieda " + \
            "WHERE keyword_type_id = '%s' " % (kw_type['keyword_type_id']) + \
            "UNION " + \
            "SELECT keyword_id, keyword_label, keyword_description FROM keyword_usap " + \
            "WHERE keyword_type_id = '%s' " % (kw_type['keyword_type_id'])
        cur.execute(query)
        keywords = cur.fetchall()
        kw_type['keywords'] = sorted(keywords, key=lambda k: k['keyword_label'].upper())

    return sorted(keyword_types, key=lambda k: k['keyword_type_label'].upper())


def getDatasetKeywords(uid):
    (conn, cur) = connect_to_db()
    query = "SELECT dkm.keyword_id, ku.keyword_label FROM dataset_keyword_map dkm " + \
            "JOIN keyword_usap ku ON ku.keyword_id = dkm.keyword_id " + \
            "WHERE dkm.dataset_id = '%s' " % uid + \
            "UNION " +\
            "SELECT dkm.keyword_id, ki.keyword_label FROM dataset_keyword_map dkm " + \
            "JOIN keyword_ieda ki ON ki.keyword_id = dkm.keyword_id " + \
            "WHERE dkm.dataset_id = '%s' " % uid + \
            "ORDER BY keyword_label;"
    cur.execute(query)
    return cur.fetchall() 


def projectJson2sql(data, uid):

    conn, cur = connect_to_db()

    sql_out = ""
    sql_out += "START TRANSACTION;\n\n"

    # project table
    sql_out += "--NOTE: populate project table\n"
    sql_out += "INSERT INTO project (proj_uid, title, short_name, description, start_date, end_date, date_created, date_modified) " \
               "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');\n\n" % (uid, data['title'], data['short_title'], data['sum'].replace("'", "''"),  
                                                                                 data['start'], data['end'], data['timestamp'][0:10], data['timestamp'][0:10])

    # Add PI to person table, if necessary, and project_person_map
    pi_id = data['pi_name_last'] + ', ' + data['pi_name_first'] 
    pi_id = pi_id.replace(',  ', ', ')
    query = "SELECT * FROM person WHERE id = '%s'" % pi_id
    cur.execute(query)
    res = cur.fetchall()
    if len(res) == 0:
        sql_out += "--NOTE: adding PI to person table\n"
        sql_out += "INSERT INTO person (id, first_name, last_name, email, organization) " \
                   "VALUES ('%s', '%s', '%s', '%s', '%s');\n\n" % \
                   (pi_id, data.get('pi_name_first'), data.get('pi_name_last'), data.get('email'), data.get('org'))
    else:
        if data.get('email') is not None and res[0]['email'] != data['email']:
            sql_out += "--NOTE: updating email for PI %s to %s\n" % (pi_id, data['email'])
            sql_out += "UPDATE person SET email='%s' WHERE id='%s';\n\n" % (data['email'], pi_id)
        if data.get('org') is not None and res[0]['organization'] != data['org']:
            sql_out += "--NOTE: updating organization for PI %s to %s\n" % (pi_id, data['org'])
            sql_out += "UPDATE person SET organization='%s' WHERE id='%s';\n\n" % (data['org'], pi_id)

    sql_out += "--NOTE: adding PI %s to project_person_map\n" % pi_id
    sql_out += "INSERT INTO project_person_map (proj_uid, person_id, role) VALUES ('%s', '%s', 'Investigator and contact');\n\n" % \
               (uid, pi_id)
    
    # add org to Organizations table if necessary
    if data.get('org') is not None:
        query = "SELECT COUNT(*) FROM organizations WHERE name = '%s'" % data['org']
        cur.execute(query)
        res = cur.fetchall()
        if res[0]['count'] == 0:
            sql_out += "--NOTE: adding %s to organizations table\n" % data['org']
            sql_out += "INSERT INTO organizations (name) VALUES ('%s');\n\n" % data['org']

    # Add other personnel to the person table, if necessary, and project_person_map
    for co_pi in data['copis']:
        co_pi_id = co_pi['name_last'] + ', ' + co_pi['name_first']
        co_pi_id = co_pi_id.replace(',  ', ', ')
        query = "SELECT * FROM person WHERE id = '%s'" % co_pi_id
        cur.execute(query)
        res = cur.fetchall()
        if len(res) == 0:
            sql_out += "--NOTE: adding %s to person table\n" % co_pi_id
            sql_out += "INSERT INTO person (id, first_name, last_name, organization) " \
                       "VALUES ('%s', '%s', '%s' ,'%s');\n\n" % \
                       (co_pi_id, co_pi.get('name_first'), co_pi.get('name_last'), co_pi.get('org'))
        elif co_pi.get('org') is not None and res[0]['organization'] != co_pi['org']:
            sql_out += "--NOTE: updating organization for %s to %s\n" % (co_pi_id, co_pi['org'])
            sql_out += "UPDATE person SET organization='%s' WHERE id='%s';\n\n" % (co_pi['org'], co_pi_id)
        
        sql_out += "--NOTE: adding %s to project_person_map\n" % co_pi_id
        sql_out += "INSERT INTO project_person_map (proj_uid, person_id, role) VALUES ('%s', '%s', '%s');\n\n" % \
                   (uid, co_pi_id, co_pi.get('role'))

        # add org to Organizations table if necessary
        if co_pi.get('org') is not None:
            query = "SELECT COUNT(*) FROM organizations WHERE name = '%s'" % co_pi['org']
            cur.execute(query)
            res = cur.fetchall()
            if res[0]['count'] == 0:
                sql_out += "--NOTE: adding %s to orgainzations table\n" % co_pi['org']
                sql_out += "INSERT INTO organizations (name) VALUES('%s');\n\n" % co_pi['org']

    # Update data management plan link in award table
    if data.get('dmp_file') is not None and data['dmp_file'] != '' and data.get('upload_directory') is not None \
       and data.get('award') is not None:
        dst = os.path.join(AWARDS_FOLDER, data['award'], data['dmp_file'])
        sql_out += "--NOTE: updating dmp_link for award %s\n" % data['award']
        sql_out += "UPDATE award SET dmp_link = '%s' WHERE award = '%s';\n\n" % (dst, data['award'])

    # Add awards to project_award_map
    sql_out += "--NOTE: adding awards to project_award_map\n"
    sql_out += "INSERT INTO project_award_map (proj_uid, award_id, is_main_award) VALUES ('%s', '%s', 'True');\n" % \
        (uid, data['award'])
    for award in data['other_awards']:
        sql_out += "INSERT INTO project_award_map (proj_uid, award_id, is_main_award) VALUES ('%s', '%s', 'False');\n" % \
            (uid, award)
    sql_out += "\n"
    
    # Add initiatives to project_initiative_map
    if data.get('program') is not None and data['program'] != "":
        initiative = json.loads(data["program"].replace("\'", "\""))['id']
        sql_out += "\n--NOTE: adding initiative to project_initiative_map\n"
        sql_out += "INSERT INTO project_initiative_map (proj_uid, initiative_id) VALUES ('%s', '%s');\n\n" % \
            (uid, initiative)

    # Add references
    if data.get('publications') is not None and len(data['publications']) > 0:
        sql_out += "--NOTE: adding references\n"

        #first find the highest ref_uid already in the table
        query = "SELECT MAX(ref_uid) FROM reference;"
        cur.execute(query)
        res = cur.fetchall()
        old_uid = int(res[0]['max'].replace('ref_', ''))

        for pub in data['publications']:
            # see if they are already in the references table
            query = "SELECT * FROM reference WHERE doi='%s' AND ref_text = '%s';" % (pub.get('doi'), pub.get('name'))
            cur.execute(query)
            res = cur.fetchall()
            if len(res) == 0:
                # sql_out += "--NOTE: adding %s to reference table\n" % pub['name']
                old_uid += 1
                ref_uid = 'ref_%0*d' % (7, old_uid)
                sql_out += "INSERT INTO reference (ref_uid, ref_text, doi) VALUES ('%s', '%s', '%s');\n" % \
                    (ref_uid, pub.get('name'), pub.get('doi'))
            else:
                ref_uid = res[0]['ref_uid']
            # sql_out += "--NOTE: adding reference %s to project_ref_map\n" % ref_uid
            sql_out += "INSERT INTO project_ref_map (proj_uid, ref_uid) VALUES ('%s', '%s');\n" % \
                (uid, ref_uid)

        sql_out += "\n"

    # Add datasets
    if data.get('datasets') is not None and len(data['datasets']) > 0:
        sql_out += "--NOTE: adding project_datasets\n"
        for ds in data['datasets']:
            # see if they are already in the project_dataset table
            query = "SELECT * FROM project_dataset WHERE repository = '%s' AND title = '%s'" % (ds['repository'], ds['title'])
            if ds.get('doi') is not None and ds['doi'] != '':
                query += " AND doi = '%s'"
            cur.execute(query)
            res = cur.fetchall()
            if len(res) == 0:
                # sql_out += "--NOTE: adding dataset %s to project_dataset table\n" % ds['title']
                if ds.get('doi') is not None and ds['doi'] != '':
                    # first try and get the dataset id from the dataset table, using the DOI
                    query = "SELECT id FROM dataset WHERE doi = '%s';" % ds['doi']
                    cur.execute(query)
                    res2 = cur.fetchall()
                    if len(res2) == 1:
                        ds_id = res2[0]['id']
                    else:
                        ds_id = getNextProjectDatasetID()
                else:
                    ds_id = getNextProjectDatasetID()
                sql_out += "INSERT INTO project_dataset (dataset_id, repository, title, url, status, doi) VALUES ('%s', '%s', '%s', '%s', 'exists', '%s');\n" % \
                    (ds_id, ds.get('repository'), ds.get('title'), ds.get('url'), ds.get('doi'))
            else:
                ds_id = res[0]['dataset_id']
            # sql_out += "--NOTE: adding dataset %s to project_dataset_map\n" % ds_id
            sql_out += "INSERT INTO project_dataset_map (proj_uid, dataset_id) VALUES ('%s', '%s');\n" % (uid, ds_id)
        sql_out += "\n"

    # Add websites
    if data.get('websites') is not None and len(data['websites']) > 0:
        sql_out += "--NOTE: adding project_websites\n"
        for ws in data['websites']:
            sql_out += "INSERT INTO project_website (proj_uid, title, url) VALUES ('%s', '%s', '%s');\n" % \
                (uid, ws.get('title'), ws.get('url'))
        sql_out += "\n"

    # Add deployments
    if data.get('deployments') is not None and len(data['deployments']) > 0:
        sql_out += "--NOTE: adding project_deployments\n"
        for dep in data['deployments']:
            sql_out += "INSERT INTO project_deployment (proj_uid, deployment_id, deployment_type, url) VALUES ('%s', '%s', '%s', '%s');\n" % \
                (uid, dep.get('name'), dep.get('type'), dep.get('url'))
        sql_out += "\n"

    # Add features/locations
    if data.get('locations') is not None and len(data['locations']) > 0:
        sql_out += "--NOTE: adding gcmd_locations\n"
        for loc in data['locations']:
            sql_out += "INSERT INTO project_gcmd_location_map (proj_uid, loc_id) VALUES ('%s', '%s');\n" % (uid, loc)
        sql_out += "\n"

    if data.get('location_free') is not None and data['location_free'] != '':
        free_locs = data['location_free'].split(',')
        if len(free_locs) > 0:
            sql_out += "--NOTE: adding free text locations to project_features\n"
            for loc in free_locs:
                sql_out += "INSERT INTO project_feature (proj_uid, feature_name) VALUES ('%s', '%s');\n" % (uid, loc)
        sql_out += "\n"

    # Add GCMD keywords
    if data.get('parameters') is not None and len(data['parameters']) > 0:
        sql_out += "--NOTE: adding gcmd_science_keys\n"
        for param in data['parameters']:
            sql_out += "INSERT INTO project_gcmd_science_key_map (proj_uid, gcmd_key_id) VALUES ('%s', '%s');\n" % (uid, param)
        sql_out += "\n"

    # Add spatial bounds
    sql_out += "--NOTE: adding spatial bounds\n"
    sql_out += "INSERT INTO project_spatial_map(proj_uid,west,east,south,north,cross_dateline) VALUES ('%s','%s','%s','%s','%s','%s');\n" % \
        (uid, data["geo_w"], data["geo_e"], data["geo_s"], data["geo_n"], data["cross_dateline"])
    if (data["geo_w"] != '' and data["geo_e"] != '' and data["geo_s"] != '' and data["geo_n"] != '' and data["cross_dateline"] != ''):
        west = float(data["geo_w"])
        east = float(data["geo_e"])
        south = float(data["geo_s"])
        north = float(data["geo_n"])
        mid_point_lat = (south - north) / 2 + north
        mid_point_long = (east - west) / 2 + west

        geometry = "ST_GeomFromText('POINT(%s %s)', 4326)" % (mid_point_long, mid_point_lat)
        bounds_geometry = "ST_GeomFromText('%s', 4326)" % makeBoundsGeom(north, south, east, west, data["cross_dateline"])

        sql_out += "UPDATE project_spatial_map SET (geometry, bounds_geometry) = (%s, %s) WHERE proj_uid = '%s';\n\n" % \
            (geometry, bounds_geometry, uid)

    sql_out += '\nCOMMIT;\n'
    
    return sql_out


# Read the next project dataset reference number from the file (for non-USAP-DC datasets)
def getNextProjectDatasetID():
    ds_id = open(PROJECT_DATASET_ID_FILE, 'r').readline().strip()
    new_ds_id = int(ds_id) + 1
    with open(PROJECT_DATASET_ID_FILE, 'w') as refFile:
        refFile.write(str(new_ds_id))
    return ds_id 

