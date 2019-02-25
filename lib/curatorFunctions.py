import json
import xml.dom.minidom as minidom
import xml.etree.ElementTree as ET
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
DIFXML_FOLDER = "watch/difxml"
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


def prettify(elem):
    """Return a pretty-printed XML string for the Element.
    """
    rough_string = ET.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="    ")


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


def isProjectImported(uid):
    (conn, cur) = connect_to_db()
    query = "SELECT COUNT(*) from project WHERE proj_uid = '%s'" % uid
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


def updateProjectSpatialMap(uid, data):
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
                query = "SELECT COUNT(*) FROM project_spatial_map WHERE proj_uid = '%s';" % uid
                cur.execute(query)
                count = cur.fetchone()
                if count['count'] > 0:
                    sql_cmd = "UPDATE project_spatial_map SET (west,east,south,north,cross_dateline, geometry, bounds_geometry) = (%s, %s, %s, %s, %s, %s, %s) WHERE proj_uid = '%s';"\
                        % (west, east, south, north, data['cross_dateline'], geometry, bounds_geometry, uid)
                    out_text = "Spatial bounds successfully updated"
                # otherwise insert record
                else:
                    sql_cmd = "INSERT INTO project_spatial_map (proj_uid, west,east,south,north,cross_dateline, geometry, bounds_geometry) VALUES ('%s', %s, %s, %s, %s, %s, %s, %s);"\
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


def getProjectCoordsFromDatabase(uid):
    (conn, cur) = connect_to_db()
    query = "SELECT north as geo_n, east as geo_e, south as geo_s, west as geo_w, cross_dateline FROM project_spatial_map WHERE proj_uid = '%s';" % uid
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

        #first find the highest non-USAP-DC dataset_id already in the table
        query = "SELECT MAX(dataset_id) FROM project_dataset WHERE dataset_id::integer < 600000;"
        cur.execute(query)
        res = cur.fetchall()
        dataset_id = int(res[0]['max'])

        for ds in data['datasets']:
            # see if they are already in the project_dataset table
            query = "SELECT * FROM project_dataset WHERE repository = '%s' AND title = '%s'" % (ds['repository'], ds['title'])
            if ds.get('doi') is not None and ds['doi'] != '':
                query += " AND doi = '%s'" % ds['doi']
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
                        dataset_id += 1
                        ds_id = dataset_id
                else:
                    dataset_id += 1
                    ds_id = dataset_id
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

    if (data["geo_w"] != '' and data["geo_e"] != '' and data["geo_s"] != '' and data["geo_n"] != '' and data["cross_dateline"] != ''):

        west = float(data["geo_w"])
        east = float(data["geo_e"])
        south = float(data["geo_s"])
        north = float(data["geo_n"])
        mid_point_lat = (south - north) / 2 + north
        mid_point_long = (east - west) / 2 + west

        geometry = "ST_GeomFromText('POINT(%s %s)', 4326)" % (mid_point_long, mid_point_lat)
        bounds_geometry = "ST_GeomFromText('%s', 4326)" % makeBoundsGeom(north, south, east, west, data["cross_dateline"])

        sql_out += "--NOTE: adding spatial bounds\n"
        sql_out += "INSERT INTO project_spatial_map(proj_uid,west,east,south,north,cross_dateline,geometry,bounds_geometry) VALUES ('%s','%s','%s','%s','%s','%s',%s,%s);\n" % \
            (uid, west, east, south, north, data["cross_dateline"], geometry, bounds_geometry)

    sql_out += '\nCOMMIT;\n'
    
    return sql_out


def getDifID(uid):
    conn, cur = connect_to_db()
    query = "SELECT award_id FROM project_award_map WHERE is_main_award = 'True' AND proj_uid = '%s';" % uid
    cur.execute(query)
    res = cur.fetchone()
    return "USAP-%s_1" % res['award_id']  


def getDifXMLFileName(uid):
    return os.path.join(DIFXML_FOLDER, "%s.xml" % getDifID(uid))


def getDifXML(data, uid):
    root = ET.Element("DIF")
    root.set("xmlns", "http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/")
    root.set("xmlns:dif", "http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/")
    root.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
    root.set("xsi:schemaLocation", "http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/ https://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/dif_v10.2.xsd")

    # --- entry and title
    xml_entry = ET.SubElement(root, "Entry_ID")
    short_id = ET.SubElement(xml_entry, "Short_Name")
    short_id.text = getDifID(uid).split('_')[0]
    version = ET.SubElement(xml_entry, "Version")
    version.text = "1"
    xml_entry = ET.SubElement(root, "Entry_Title")
    xml_entry.text = data.get('title')

    # ---- personel
    if data.get('persons'):
        for person in data['persons']:
            name_last, name_first = person.get('id').split(',')
            xml_pi = ET.SubElement(root, "Personnel")
            xml_pi_role = ET.SubElement(xml_pi, "Role")
            xml_pi_role.text = "INVESTIGATOR"
            xml_pi_contact = ET.SubElement(xml_pi, "Contact_Person")
            xml_pi_contact_fname = ET.SubElement(xml_pi_contact, "First_Name")
            xml_pi_contact_fname.text = name_first.strip()
            xml_pi_contact_lname = ET.SubElement(xml_pi_contact, "Last_Name")
            xml_pi_contact_lname.text = name_last.strip()
            xml_pi_contact_address = ET.SubElement(xml_pi_contact, "Address")
            xml_pi_contact_address1 = ET.SubElement(xml_pi_contact_address, "Street_Address")
            xml_pi_contact_address1.text = person.get('org')
            xml_pi_contact_email = ET.SubElement(xml_pi_contact, "Email")
            xml_pi_contact_email.text = person.get('email')

    # --- science keywords
    if data.get('parameters'):
        for keyword in data['parameters']:
            keys = keyword['id'].split('>')
            xml_key = ET.SubElement(root, "Science_Keywords")
            xml_key_cat = ET.SubElement(xml_key, "Category")
            xml_key_cat.text = keys[0].strip()
            xml_key_topic = ET.SubElement(xml_key, "Topic")
            xml_key_topic.text = keys[1].strip()
            xml_key_term = ET.SubElement(xml_key, "Term")
            xml_key_term.text = keys[2].strip()
            if len(keys) > 3:
                xml_key_level1 = ET.SubElement(xml_key, "Variable_Level_1")
                xml_key_level1.text = keys[3].strip()

    # --- iso topic category --> delete the ones that don't fit, should be form
    xml_iso = ET.SubElement(root, "ISO_Topic_Category")
    xml_iso.text = "GEOSCIENTIFIC INFORMATION"
    xml_iso = ET.SubElement(root, "ISO_Topic_Category")
    xml_iso.text = "CLIMATOLOGY/METEOROLOGY/ATMOSPHERE"
    xml_iso = ET.SubElement(root, "ISO_Topic_Category")
    xml_iso.text = "OCEANS"
    xml_iso = ET.SubElement(root, "ISO_Topic_Category")
    xml_iso.text = "BIOTA"

    # --- Ancillary_Keyword
    xml_aux_key = ET.SubElement(root, "Ancillary_Keyword")
    xml_aux_key.text = "USAP-DC"

    # --- temporal coverage
    xml_time = ET.SubElement(root, "Temporal_Coverage")
    xml_time_range = ET.SubElement(xml_time, "Range_DateTime")
    xml_time_begin = ET.SubElement(xml_time_range, "Beginning_Date_Time")
    xml_time_begin.text = str(data.get('start_date'))
    xml_time_end = ET.SubElement(xml_time_range, "Ending_Date_Time")
    xml_time_end.text = str(data.get('end_date'))

    # --- Spatial coverage
    if data.get('spatial_bounds'):
        for sb in data['spatial_bounds']:
            xml_space = ET.SubElement(root, "Spatial_Coverage")
            xml_space_type = ET.SubElement(xml_space, "Spatial_Coverage_Type")
            xml_space_type.text = "Horizontal"
            xml_space_represent = ET.SubElement(xml_space, "Granule_Spatial_Representation")
            xml_space_represent.text = "CARTESIAN"
            xml_space_geom = ET.SubElement(xml_space, "Geometry")
            xml_space_geom_coord = ET.SubElement(xml_space_geom, "Coordinate_System")
            xml_space_geom_coord.text = "CARTESIAN"
            xml_space_geom_bound = ET.SubElement(xml_space_geom, "Bounding_Rectangle")
            xml_space_geom_south = ET.SubElement(xml_space_geom_bound, "Southernmost_Latitude")
            xml_space_geom_south.text = str(sb['south'])
            xml_space_geom_south = ET.SubElement(xml_space_geom_bound, "Northernmost_Latitude")
            xml_space_geom_south.text = str(sb['north'])
            xml_space_geom_south = ET.SubElement(xml_space_geom_bound, "Westernmost_Longitude")
            xml_space_geom_south.text = str(sb['west'])
            xml_space_geom_south = ET.SubElement(xml_space_geom_bound, "Easternmost_Longitude")
            xml_space_geom_south.text = str(sb['east'])

    # --- location
    if data.get('locations'):
        for location in data['locations']:
            loc_val = location['id'].split('>')
            xml_loc = ET.SubElement(root, "Location")
            xml_loc_cat = ET.SubElement(xml_loc, "Location_Category")
            xml_loc_cat.text = loc_val[0].strip()
            if len(loc_val) > 1:
                xml_loc_type = ET.SubElement(xml_loc, "Location_Type")
                xml_loc_type.text = loc_val[1].strip()
            if len(loc_val) > 2:
                xml_loc_sub1 = ET.SubElement(xml_loc, "Location_Subregion1")
                xml_loc_sub1.text = loc_val[2].strip()

    # # --- project
    xml_proj = ET.SubElement(root, "Project")
    xml_proj_sname = ET.SubElement(xml_proj, "Short_Name")
    xml_proj_sname.text = "NSF/OPP"
    xml_proj_lname = ET.SubElement(xml_proj, "Long_Name")
    xml_proj_lname.text = "Office of Polar Programs, National Science Foundation"

    # --- language
    xml_lang = ET.SubElement(root, "Dataset_Language")
    xml_lang.text = "English"

    # --- progress
    xml_progress = ET.SubElement(root, "Dataset_Progress")
    xml_progress.text = "COMPLETE"

    # --- platform
    xml_platform = ET.SubElement(root, "Platform")
    xml_platform_type = ET.SubElement(xml_platform, "Type")
    xml_platform_type.text = "Not applicable"
    xml_platform_sname = ET.SubElement(xml_platform, "Short_Name")
    xml_platform_sname.text = "Not applicable"
    xml_instrument = ET.SubElement(xml_platform, "Instrument")
    xml_instrument_sname = ET.SubElement(xml_instrument, "Short_Name")
    xml_instrument_sname.text = "Not applicable"

    # --- organization
    xml_org = ET.SubElement(root, "Organization")
    xml_org_type = ET.SubElement(xml_org, "Organization_Type")
    xml_org_type.text = "ARCHIVER"
    xml_org_type = ET.SubElement(xml_org, "Organization_Type")
    xml_org_type.text = "DISTRIBUTOR"
    xml_org_name = ET.SubElement(xml_org, "Organization_Name")
    xml_org_sname = ET.SubElement(xml_org_name, "Short_Name")
    xml_org_sname.text = "USAP-DC"
    xml_org_sname = ET.SubElement(xml_org_name, "Long_Name")
    xml_org_sname.text = "United States Polar Program - Data Center"
    xml_org_url = ET.SubElement(xml_org, "Organization_URL")
    xml_org_url.text = "http://www.usap-dc.org/"

    xml_org_pers = ET.SubElement(xml_org, "Personnel")
    xml_org_pers_role = ET.SubElement(xml_org_pers, "Role")
    xml_org_pers_role.text = "DATA CENTER CONTACT"
    xml_org_pers_contact = ET.SubElement(xml_org_pers, "Contact_Person")
    xml_org_pers_contact_fname = ET.SubElement(xml_org_pers_contact, "First_Name")
    xml_org_pers_contact_fname.text = "Data"
    xml_org_pers_contact_lname = ET.SubElement(xml_org_pers_contact, "Last_Name")
    xml_org_pers_contact_lname.text = "Manager"
    xml_org_pers_contact_address = ET.SubElement(xml_org_pers_contact, "Address")
    xml_org_pers_contact_street = ET.SubElement(xml_org_pers_contact_address, "Street_Address")
    xml_org_pers_contact_street.text = "Lamont-Doherty Earth Observatory"
    xml_org_pers_contact_street = ET.SubElement(xml_org_pers_contact_address, "Street_Address")
    xml_org_pers_contact_street.text = "61 Route 9W"
    xml_org_pers_contact_street = ET.SubElement(xml_org_pers_contact_address, "City")
    xml_org_pers_contact_street.text = "Palisades"
    xml_org_pers_contact_street = ET.SubElement(xml_org_pers_contact_address, "State_Province")
    xml_org_pers_contact_street.text = "NY"
    xml_org_pers_contact_street = ET.SubElement(xml_org_pers_contact_address, "Postal_Code")
    xml_org_pers_contact_street.text = "10964"
    xml_org_pers_contact_street = ET.SubElement(xml_org_pers_contact_address, "Country")
    xml_org_pers_contact_street.text = "USA"
    xml_org_pers_contact_email = ET.SubElement(xml_org_pers_contact, "Email")
    xml_org_pers_contact_email.text = "info@usap-dc.org"

    # --- summary
    xml_sum = ET.SubElement(root, "Summary")
    xml_abstract = ET.SubElement(xml_sum, "Abstract")
    text = data['description'].replace('<br/>', '\n\n')
    xml_abstract.text = text

    # --- related URL for awards
    if data.get('funding'):
        for award in set([a['award'] for a in data['funding']]):
            xml_url = ET.SubElement(root, "Related_URL")
            xml_url_ctype = ET.SubElement(xml_url, "URL_Content_Type")
            xml_url_type = ET.SubElement(xml_url_ctype, "Type")
            xml_url_type.text = "VIEW RELATED INFORMATION"
            xml_url_url = ET.SubElement(xml_url, "URL")
            xml_url_url.text = "http://www.nsf.gov/awardsearch/showAward.do?AwardNumber=%s" % award
            xml_url_desc = ET.SubElement(xml_url, "Description")
            xml_url_desc.text = "NSF Award Abstract"

    # --- related URL for awards
    if data.get('datasets'):
        for ds in data['datasets']:
            xml_url = ET.SubElement(root, "Related_URL")
            xml_url_ctype = ET.SubElement(xml_url, "URL_Content_Type")
            xml_url_type = ET.SubElement(xml_url_ctype, "Type")
            xml_url_type.text = "GET DATA"
            xml_url_url = ET.SubElement(xml_url, "URL")
            xml_url_url.text = ds.get('url')
            xml_url_desc = ET.SubElement(xml_url, "Description")
            xml_url_desc.text = ds.get('title')

    # --- IDN nodes
    xml_idn = ET.SubElement(root, "IDN_Node")
    xml_idn_name = ET.SubElement(xml_idn, "Short_Name")
    xml_idn_name.text = "AMD"
    xml_idn = ET.SubElement(root, "IDN_Node")
    xml_idn_name = ET.SubElement(xml_idn, "Short_Name")
    xml_idn_name.text = "AMD/US"
    xml_idn = ET.SubElement(root, "IDN_Node")
    xml_idn_name = ET.SubElement(xml_idn, "Short_Name")
    xml_idn_name.text = "CEOS"
    xml_idn = ET.SubElement(root, "IDN_Node")
    xml_idn_name = ET.SubElement(xml_idn, "Short_Name")
    xml_idn_name.text = "USA/NSF"

    # --- metadata info
    xml_meta_node = ET.SubElement(root, "Originating_Metadata_Node")
    xml_meta_node.text = "GCMD"
    xml_meta_name = ET.SubElement(root, "Metadata_Name")
    xml_meta_name.text = "CEOS IDN DIF"
    xml_meta_version = ET.SubElement(root, "Metadata_Version")
    xml_meta_version.text = "VERSION 10.2"
    xml_meta_date = ET.SubElement(root, "Metadata_Dates")
    xml_meta_date_create = ET.SubElement(xml_meta_date, "Metadata_Creation")
    xml_meta_date_create.text = str(data['date_created'])
    xml_meta_date_mod = ET.SubElement(xml_meta_date, "Metadata_Last_Revision")
    xml_meta_date_mod.text = str(data['date_modified'])
    xml_data_date_create = ET.SubElement(xml_meta_date, "Data_Creation")
    xml_data_date_create.text = str(data['date_created'])
    xml_data_date_mod = ET.SubElement(xml_meta_date, "Data_Last_Revision")
    xml_data_date_mod.text = str(data['date_created'])

    # write XML to file
    file_name = getDifXMLFileName(uid)
    with open(file_name, 'w') as out_file:
        out_file.write(prettify(root))
    os.chmod(file_name, 0o664)

    return prettify(root)


def addDifToDB(uid):
    (conn, cur) = connect_to_db()
    status = 1
    if type(conn) is str:
        out_text = conn
        status = 0
    else:
        try:
            sql_cmd = ""
            dif_id = getDifID(uid)
            # Add to dif table if not already there
            query = "SELECT * FROM dif WHERE dif_id = '%s';" % dif_id
            cur.execute(query)
            res = cur.fetchall()
            if len(res) == 0:
                sql_cmd += "INSERT INTO dif (dif_id) VALUES ('%s');" % dif_id

            # add to project_dif_map
            query = "SELECT * FROM project_dif_map WHERE proj_uid = '%s' AND dif_id = '%s';" % (uid, dif_id)
            cur.execute(query)
            res = cur.fetchall()
            if len(res) == 0:
                sql_cmd += "INSERT INTO project_dif_map (proj_uid, dif_id) VALUES ('%s', '%s');" % (uid, dif_id)

            sql_cmd += "COMMIT;"

            cur.execute(sql_cmd)

            out_text = "Succesfully added DIF record to database."
        except Exception as e:
            out_text = "Error adding DIF record to database. \n%s" % str(e)
            status = 0

    return (out_text, status)
