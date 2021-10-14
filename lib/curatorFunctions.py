import json
import xml.dom.minidom as minidom
import xml.etree.ElementTree as ET
import os
import sys
import requests
from flask import session, url_for, current_app, request
from subprocess import Popen, PIPE, check_output
import base64
from datetime import datetime
from dateutil.parser import parse
import usap
import tarfile
import gzip
import zipfile
import mimetypes
import shutil
import py7zlib

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
RECENT_DATA_FILE = "inc/recent_data.txt"
REF_UID_FILE = "inc/ref_uid"


config = json.loads(open('config.json', 'r').read())


def prettify(elem):
    """Return a pretty-printed XML string for the Element.
    """
    rough_string = ET.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="    ")


def submitToDataCite(uid, edit=False):
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
        if edit:
            doi_json = {"data": {"attributes": {"xml": xml_b64}}}
        else:
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

    headers = {'Content-Type': 'application/vnd.api+json'}
    response = requests.put(dc_details['SERVER'], headers=headers, data=json.dumps(doi_json), auth=(dc_details['USER'], dc_details['PASSWORD']))

    # Send DOI Create request to DataCite
    try:
        if edit:
            response = requests.put(dc_details['SERVER'] + '/' + doi, headers=headers, data=json.dumps(doi_json), auth=(dc_details['USER'], dc_details['PASSWORD']))
            msg = "Successfully updated dataset at DataCite, DOI: %s"
        else:
            response = requests.post(dc_details['SERVER'], headers=headers, data=json.dumps(doi_json), auth=(dc_details['USER'], dc_details['PASSWORD']))
            msg = "Successfully registered dataset at DataCite, DOI: %s"

        if response.status_code not in [201, 200]:
            return("Error with request to create DOI at DataCite.  Status code: %s\n Error: %s" % (response.status_code, response.json()))
        elif not edit:
            #update doi in database
            (conn, cur) = usap.connect_to_db(curator=True)
            query = "UPDATE dataset SET doi='%s' WHERE id='%s';" % (doi, uid)
            query += "UPDATE project_dataset SET doi = '%s' WHERE dataset_id = '%s';" % (doi, uid)
            query += "COMMIT;" 
            cur.execute(query)

        return(msg % doi)

    except Exception as e:
        return("Error generating DOI: %s" % str(e))


def getDataCiteXML(uid):
    print("in getDataCiteXML")
    status = 1
    (conn, cur) = usap.connect_to_db()
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


def getISOXMLFromFile(uid, update=False):
    isoxml_file = getISOXMLFileName(uid)
    # check if datacite xml file already exists
    # if not, or if updating, run doISOXML to generate 
    if not os.path.exists(isoxml_file) or update:
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
    (conn, cur) = usap.connect_to_db()
    query = "SELECT doi from dataset WHERE id = '%s'" % uid
    cur.execute(query)
    res = cur.fetchone()
    return (res['doi'] and len(res['doi']) > 0)
    

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
    (conn, cur) = usap.connect_to_db(curator=True)
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
    (conn, cur) = usap.connect_to_db()
    query = "SELECT COUNT(*) from dataset WHERE id = '%s'" % uid
    cur.execute(query)
    res = cur.fetchone()
    return res['count'] > 0


def isProjectImported(uid):
    (conn, cur) = usap.connect_to_db()
    query = "SELECT COUNT(*) from project WHERE proj_uid = '%s'" % uid
    cur.execute(query)
    res = cur.fetchone()
    return res['count'] > 0


def updateSpatialMap(uid, data, run_update=True):
    (conn, cur) = usap.connect_to_db(curator=True)
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

                geometry = "ST_GeomFromText('%s', 4326)" % makeCentroidGeom(north, south, east, west, data["cross_dateline"])
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
                if not run_update:
                    return sql_cmd
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


def updateProjectSpatialMap(uid, data, run_update=True):
    (conn, cur) = usap.connect_to_db(curator=True)
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

                geometry = "ST_GeomFromText('%s', 4326)" % makeCentroidGeom(north, south, east, west, data["cross_dateline"])
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
                if not run_update:
                    return sql_cmd
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
    (conn, cur) = usap.connect_to_db()
    query = "SELECT north as geo_n, east as geo_e, south as geo_s, west as geo_w, cross_dateline FROM dataset_spatial_map WHERE dataset_id = '%s';" % uid
    cur.execute(query)
    return cur.fetchone()


def getProjectCoordsFromDatabase(uid):
    (conn, cur) = usap.connect_to_db()
    query = "SELECT north as geo_n, east as geo_e, south as geo_s, west as geo_w, cross_dateline FROM project_spatial_map WHERE proj_uid = '%s';" % uid
    cur.execute(query)
    return cur.fetchone()


def getProjectAwardsFromDatabase(uid):
    (conn, cur) = usap.connect_to_db()
    query = "SELECT * FROM project_award_map WHERE proj_uid = '%s' ORDER by award_id DESC;" % uid
    cur.execute(query)
    return cur.fetchall()


def addAwardToDataset(uid, award):
    (conn, cur) = usap.connect_to_db(curator=True)
    status = 1
    if type(conn) is str:
        out_text = conn
        status = 0
    else:
        try:
            sql_cmd = ""
            # check if this award is already in the award table
            query = "SELECT COUNT(*) FROM  award WHERE award = '%s'" % award
            cur.execute(query)
            res = cur.fetchone()
            if res['count'] == 0:
                # Add award to award table
                sql_cmd += "--NOTE: Adding award %s to award table\n" % award
                sql_cmd += "INSERT INTO award(award, dir, div, title, name) VALUES ('%s', 'GEO', 'OPP', 'TBD', 'TBD');\n" % award
            else:
                query = "SELECT COUNT(*) FROM dif WHERE dif_id = 'USAP-%s'" % award
                cur.execute(query)
                res = cur.fetchone()
                if res['count'] == 0:
                    sql_cmd += "INSERT INTO dif(dif_id) VALUES ('USAP-%s');\n" % (award)

                sql_cmd += "INSERT INTO dataset_dif_map(dataset_id,dif_id) VALUES ('%s','USAP-%s');\n" % (uid, award)

            sql_cmd += "INSERT INTO dataset_award_map(dataset_id,award_id) VALUES ('%s','%s');\n" % (uid, award)

            # look up award to see if already mapped to a program
            query = "SELECT program_id FROM award_program_map WHERE award_id = '%s';" % award
            cur.execute(query)
            res = cur.fetchone()
      
            if res is None:
                sql_cmd += "INSERT INTO dataset_program_map(dataset_id,program_id) VALUES ('{}','Antarctic Ocean and Atmospheric Sciences');\n\n".format(uid)
            else:
                sql_cmd += "INSERT INTO dataset_program_map(dataset_id,program_id) VALUES ('{}','{}');\n\n".format(uid, res['program_id'])

            print(sql_cmd)
            cur.execute(sql_cmd)
            cur.execute('COMMIT;')
            out_text = "Award successfully added to dataset %s" % uid
        except Exception as e:
            out_text = "Error adding award to database. \n%s" % str(e)
            status = 0
    return (out_text, status)


def addAwardToProject(uid, award, is_main_award, is_previous_award):
    (conn, cur) = usap.connect_to_db(curator=True)
    status = 1
    if type(conn) is str:
        out_text = conn
        status = 0
    else:
        try:
            sql_cmd = ""
            # if new award is to be the main award, need to set existing main award to non-main award
            if is_main_award:
                sql_cmd += "UPDATE project_award_map SET is_main_award = 'False' WHERE proj_uid = '%s';\n" % uid
            # check if this award is already in the award table
            query = "SELECT COUNT(*) FROM  award WHERE award = '%s'" % award
            cur.execute(query)
            res = cur.fetchone()
            if res['count'] == 0:
                # Add award to award table
                sql_cmd += "INSERT INTO award(award, dir, div, title, name) VALUES ('%s', 'GEO', 'OPP', 'TBD', 'TBD');\n" % award
            sql_cmd += "INSERT INTO project_award_map (proj_uid, award_id, is_main_award, is_previous_award) VALUES ('%s', '%s', '%s' ,'%s');\n" %( 
                        uid, award, is_main_award, is_previous_award)

            print(sql_cmd)
            cur.execute(sql_cmd)
            cur.execute('COMMIT;')
            out_text = "Award successfully added to project %s" % uid
        except Exception as e:
            out_text = "Error adding award to database. \n%s" % str(e)
            status = 0
    return (out_text, status)


def updateProjectAwards(uid, update_awards, new_award):
    print(update_awards)
    print(new_award)
    print(uid)
    (conn, cur) = usap.connect_to_db(curator=True)
    status = 1
    if type(conn) is str:
        out_text = conn
        status = 0
    else:
        try:
            sql_cmd = ""
            # update existing project awards
            for award in update_awards:
                if award['remove']:
                    sql_cmd += "DELETE FROM project_award_map WHERE proj_uid='%s' AND award_id='%s';\n" % (uid, award['award_id'])
                else:
                    sql_cmd += """UPDATE project_award_map SET (is_main_award, is_previous_award) = (%s, %s) 
                                  WHERE proj_uid='%s' AND award_id='%s';\n""" % (
                                      award['is_main_award'], award['is_previous_award'], uid, award['award_id'])

            # add new award
            if new_award:
            # check if this award is already in the award table
                query = "SELECT COUNT(*) FROM  award WHERE award = '%s'" % new_award['award_id']
                cur.execute(query)
                res = cur.fetchone()
                if res['count'] == 0:
                    # Add award to award table
                    sql_cmd += "INSERT INTO award(award, dir, div, title, name) VALUES ('%s', 'GEO', 'OPP', 'TBD', 'TBD');\n" % new_award['award_id']
                sql_cmd += "INSERT INTO project_award_map (proj_uid, award_id, is_main_award, is_previous_award) VALUES ('%s', '%s', '%s' ,'%s');\n" %( 
                            uid, new_award['award_id'], new_award['is_main_award'], new_award['is_previous_award'])

            # print(sql_cmd)
            cur.execute(sql_cmd)
            cur.execute('COMMIT;')
            out_text = "Award(s) successfully updated for project %s" % uid
        except Exception as e:
            out_text = "Error updating awards in database. \n%s" % str(e)
            status = 0
    return (out_text, status)    


def getKeywordsFromDatabase():
    # for each keyword_type, get all keywords from database  
    (conn, cur) = usap.connect_to_db()
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
    (conn, cur) = usap.connect_to_db()
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


def checkAltIds(person_id, first_name, last_name, field, id_orcid=None, email=None): 
    conn, cur = usap.connect_to_db(curator=True)
    query = "SELECT * FROM person WHERE id ~ '%s' AND id ~'%s'" % (usap.escapeQuotes(first_name.split(' ')[0]), usap.escapeQuotes(last_name.split(' ')[0]))
    if id_orcid: 
        query += " OR id_orcid = '%s'" % id_orcid
    if email:
        query += " OR email = '%s'" % email

    cur.execute(query)
    res = cur.fetchall()
    if len(res) > 0:
        alt_users = "; ".join(u['id'] for u in res)
        return "--NOTE: ONE OR MORE POSSIBLE DATABASE ENTRIES FOUND FOR %s:\n--%s.\n--UPDATE %s IN JSON TAB TO CHANGE USER ID.\n" % (person_id, alt_users, field)
    return ""


def projectJson2sql(data, uid):

    # check if we are editing an existing project
    if data.get('edit') and data['edit'] == 'True':
        return editProjectJson2sql(data, uid)

    conn, cur = usap.connect_to_db(curator=True)

    # --- fix award fields (throw out PI name)
    if data["award"] not in ["None", "Not In This List"] and len(data["award"].split(" ", 1)) > 1:
            data["award"] = data["award"].split(" ")[0]
    for i in range(len(data["other_awards"])):
        if data["other_awards"][i]['id'] not in ["None", "Not In This List"] and len(data["other_awards"][i]['id'].split(" ", 1)) > 1:
            (data["other_awards"][i]['id']) = data["other_awards"][i]['id'].split(" ")[0]  # throw away the rest of the award string

    sql_out = ""
    sql_out += "START TRANSACTION;\n\n"

    # project table
    sql_out += "--NOTE: populate project table\n"
    sql_out += "INSERT INTO project (proj_uid, title, short_name, description, start_date, end_date, date_created, date_modified) " \
               "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');\n\n" % (uid, data['title'], data['short_title'], usap.escapeQuotes(data['sum']),  
                                                                                 data['start'], data['end'], data['timestamp'][0:10], data['timestamp'][0:10])

    # generate the submitter's person id if we have their name
    subm_id = None
    if data.get('submitter_name'):
        names = data['submitter_name'].rsplit(' ',1)
        if len(names) > 1:
            subm_id = names[-1] + ', ' + names[0]

    # Add PI to person table, if necessary, and project_person_map
    data['pi_name_last'] = data['pi_name_last']
    data['pi_name_first'] = data['pi_name_first']
    pi_id = data['pi_name_last'] + ', ' + data['pi_name_first'] 
    pi_id = pi_id.replace(',  ', ', ')
    query = "SELECT * FROM person WHERE id = '%s'" % usap.escapeQuotes(pi_id)
    cur.execute(query)
    res = cur.fetchall()
    if len(res) == 0:
        sql_out += "--NOTE: adding PI to person table\n"
        if subm_id and subm_id == pi_id and data.get('submitter_orcid') and data['submitter_orcid'] != '':
            # look for other possible person IDs that could belong to this person (maybe with/without middle initial, or orcid or email match)
            sql_out += checkAltIds(usap.escapeQuotes(pi_id), usap.escapeQuotes(data.get('pi_name_first')), usap.escapeQuotes(data.get('pi_name_last')), 'PI_NAME_FIRST AND PI_NAME_LAST', data['submitter_orcid'], data.get('email'))

            sql_out += "INSERT INTO person (id, first_name, last_name, email, organization, id_orcid) " \
                       "VALUES ('%s', '%s', '%s', '%s', '%s', '%s');\n\n" % \
                (usap.escapeQuotes(pi_id), usap.escapeQuotes(data.get('pi_name_first')), usap.escapeQuotes(data.get('pi_name_last')), data.get('email'), data.get('org'), data['submitter_orcid'])
        else:
            # look for other possible person IDs that could belong to this person (maybe with/without middle initial, or email match)
            sql_out += checkAltIds(pi_id, data.get('pi_name_first'), data.get('pi_name_last'), 'PI_NAME_FIRST AND PI_NAME_LAST', email=data.get('email'))

            sql_out += "INSERT INTO person (id, first_name, last_name, email, organization) " \
                       "VALUES ('%s', '%s', '%s', '%s', '%s');\n\n" % \
                (usap.escapeQuotes(pi_id), usap.escapeQuotes(data.get('pi_name_first')), usap.escapeQuotes(data.get('pi_name_last')), data.get('email'), data.get('org'))

    else:
        if data.get('email') is not None and res[0]['email'] != data['email']:
            sql_out += "--NOTE: updating email for PI %s to %s\n" % (pi_id, data['email'])
            sql_out += "UPDATE person SET email='%s' WHERE id='%s';\n\n" % (data['email'], usap.escapeQuotes(pi_id))
        if data.get('org') is not None and res[0]['organization'] != data['org']:
            sql_out += "--NOTE: updating organization for PI %s to %s\n" % (pi_id, data['org'])
            sql_out += "UPDATE person SET organization='%s' WHERE id='%s';\n\n" % (data['org'], usap.escapeQuotes(pi_id))
        if subm_id and subm_id == pi_id and data.get('submitter_orcid') is not None and res[0]['id_orcid'] != data['submitter_orcid']:
            sql_out += "--NOTE: updating orcid for PI %s to %s\n" % (pi_id, data['submitter_orcid'])
            sql_out += "UPDATE person SET id_orcid='%s' WHERE id='%s';\n\n" % (data['submitter_orcid'], usap.escapeQuotes(pi_id))

    sql_out += "--NOTE: adding PI %s to project_person_map\n" % pi_id
    sql_out += "INSERT INTO project_person_map (proj_uid, person_id, role) VALUES ('%s', '%s', 'Investigator and contact');\n\n" % \
               (uid, usap.escapeQuotes(pi_id))
    
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
            if subm_id and subm_id == co_pi_id and data.get('submitter_orcid') and data['submitter_orcid'] != '':
                # look for other possible person IDs that could belong to this person (maybe with/without middle initial, or orcid match)
                sql_out += checkAltIds(co_pi_id, co_pi.get('name_first'), co_pi.get('name_last'), 'COPIS', data['submitter_orcid'])

                sql_out += "INSERT INTO person (id, first_name, last_name, organization, id_orcid) " \
                           "VALUES ('%s', '%s', '%s' ,'%s', '%s');\n\n" % \
                           (usap.escapeQuotes(co_pi_id), usap.escapeQuotes(co_pi.get('name_first')), usap.escapeQuotes(co_pi.get('name_last')), co_pi.get('org'), data['submitter_orcid'])
            else:
                # look for other possible person IDs that could belong to this person (maybe with/without middle initial, or orcid match)
                sql_out += checkAltIds(co_pi_id, co_pi.get('name_first'), co_pi.get('name_last'), 'COPIS')
                sql_out += "INSERT INTO person (id, first_name, last_name, organization) " \
                           "VALUES ('%s', '%s', '%s' ,'%s');\n\n" % \
                           (usap.escapeQuotes(co_pi_id), usap.escapeQuotes(co_pi.get('name_first')), usap.escapeQuotes(co_pi.get('name_last')), co_pi.get('org'))
        else:
            if co_pi.get('org') is not None and res[0]['organization'] != co_pi['org']:
                sql_out += "--NOTE: updating organization for %s to %s\n" % (co_pi_id, co_pi['org'])
                sql_out += "UPDATE person SET organization='%s' WHERE id='%s';\n\n" % (co_pi['org'], usap.escapeQuotes(co_pi_id))
            if subm_id and subm_id == co_pi_id and data.get('submitter_orcid') is not None and res[0]['id_orcid'] != data['submitter_orcid']:
                sql_out += "--NOTE: updating orcid for %s to %s\n" % (co_pi_id, data['submitter_orcid'])
                sql_out += "UPDATE person SET id_orcid='%s' WHERE id='%s';\n\n" % (data['submitter_orcid'], usap.escapeQuotes(co_pi_id))
        
        sql_out += "--NOTE: adding %s to project_person_map\n" % co_pi_id
        sql_out += "INSERT INTO project_person_map (proj_uid, person_id, role) VALUES ('%s', '%s', '%s');\n\n" % \
                   (uid, usap.escapeQuotes(co_pi_id), co_pi.get('role'))

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
       and data.get('award') is not None and data['award'] != '':
        dst = os.path.join(AWARDS_FOLDER, data['award'], data['dmp_file'])
        sql_out += "--NOTE: updating dmp_link for award %s\n" % data['award']
        sql_out += "UPDATE award SET dmp_link = '%s' WHERE award = '%s';\n\n" % (dst, data['award'])

    # Add awards to project_award_map
    sql_out += "--NOTE: adding main award to project_award_map\n"
    if data['award'] == '':
        sql_out += "-- NOTE: NO AWARD SUBMITTED\n"
    else:
        award = data['award'] 
        if 'Not_In_This_List' in data['award']:
            sql_out += "--NOTE: AWARD NOT IN PROVIDED LIST\n"
            (dummy, award) = data['award'].split(':', 1)
        # check if this award is already in the award table
        query = "SELECT * FROM  award WHERE award = '%s'" % award
        cur.execute(query)
        res = cur.fetchall()
        if len(res) == 0:
            title = 'TBD'
            # Add award to award table
            sql_out += "--NOTE: Adding award %s to award table. Curator should update with any known fields.\n" % award
            sql_out += "INSERT INTO award(award, dir, div, title, name) VALUES ('%s', 'GEO', 'DIV', 'TBD', 'TBD');\n" % award
            sql_out += "--UPDATE award SET iscr='f', isipy='f', copi='', start='', expiry='', sum='', email='', orgcity='', orgzip='', dmp_link='' WHERE award='%s';\n" % award
        else:
            title = res[0]['title']

        sql_out += "INSERT INTO project_award_map (proj_uid, award_id, is_main_award) VALUES ('%s', '%s', 'True');\n" % (uid, award)

        # Add DIF to project_dif_map
        dif_id = "USAP-%s_1" % award
        # Add to dif table if not already there
        query = "SELECT * FROM dif WHERE dif_id = '%s';" % dif_id
        cur.execute(query)
        res = cur.fetchall()
        if len(res) == 0:
            sql_out += "\n--NOTE: Adding %s to dif table.\n" % dif_id
            sql_out += "INSERT INTO dif (dif_id, date_created, date_modified, title, is_usap_dc, is_nsf) VALUES ('%s', '%s', '%s', '%s', %s, %s);" % \
                (dif_id, datetime.now().date(), datetime.now().date(), title, True, True)
            # add to project_dif_map
        sql_out += "\n--NOTE: adding dif to project_dif_map\n"
        sql_out += "INSERT INTO project_dif_map (proj_uid, dif_id) VALUES ('%s', '%s');\n" % (uid, dif_id)

    for other_award in data['other_awards']:
        if 'Not_In_This_List' in other_award['id']:
                sql_out += "--NOTE: AWARD NOT IN PROVIDED LIST\n"
                (dummy, award) = other_award['id'].split(':', 1)
                # check if this award is already in the award table
                query = "SELECT COUNT(*) FROM  award WHERE award = '%s'" % award
                cur.execute(query)
                res = cur.fetchone()
                if res['count'] == 0:
                    # Add award to award table
                    sql_out += "--NOTE: Adding award %s to award table. Curator should update with any know fields.\n" % award
                    sql_out += "INSERT INTO award(award, dir, div, title, name) VALUES ('%s', 'GEO', 'DIV', 'TBD', 'TBD');\n" % award
                    sql_out += "--UPDATE award SET iscr='f', isopy='f', copi='', start='', expiry='', sum='', email='', orgcity='', orgzip='', dmp_link='' WHERE award='%s';\n" % award

        else:
            award = other_award['id']
        sql_out += "\n--NOTE: adding other awards to project_award_map\n"
        sql_out += "INSERT INTO project_award_map (proj_uid, award_id, is_main_award, is_previous_award) VALUES ('%s', '%s', 'False', '%s');\n" % (uid, award, other_award.get('is_previous_award'))
    sql_out += '\n'

    # Add initiatives to project_initiative_map
    if data.get('program') is not None and data['program'] != "":
        try:
            initiative = data['program'].get('id')
        except AttributeError:
            # if program is a string, as will be the case in older submissions
            program = json.loads(data['program'].replace("\'", "\""))
            initiative = program.get('id')
        except:
            initiative = None
        sql_out += "\n--NOTE: adding initiative to project_initiative_map\n"
        sql_out += "INSERT INTO project_initiative_map (proj_uid, initiative_id) VALUES ('%s', '%s');\n\n" % \
            (uid, initiative)

    # Add references
    if data.get('publications') is not None and len(data['publications']) > 0:
        sql_out += "--NOTE: adding references\n"

        for pub in data['publications']:
            # see if they are already in the references table
            res = []
            if pub.get('doi'):
                query = "SELECT * FROM reference WHERE doi='%s';" % (pub.get('doi'))
                cur.execute(query)
                res = cur.fetchall()
            elif (not pub.get('doi') or pub['doi'] == '') and pub.get('name'):
                query = "SELECT * FROM reference WHERE (doi IS NULL OR doi='') AND ref_text='%s';" % (usap.escapeQuotes(pub.get('name')))
                print(query)
                cur.execute(query)
                res = cur.fetchall()

            if len(res) == 0:
                # sql_out += "--NOTE: adding %s to reference table\n" % pub['name']
                ref_uid = generate_ref_uid()
                sql_out += "INSERT INTO reference (ref_uid, ref_text, doi) VALUES ('%s', '%s', '%s');\n" % \
                    (ref_uid, usap.escapeQuotes(pub.get('name')), pub.get('doi'))
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

    # Add locations
    if data.get('locations') and len(data['locations']) > 0:
        sql_out += "--NOTE: adding locations\n"

        next_id = False
        gcmd_locations = []
        for loc in data['locations']:
            if 'Not_In_This_List' in loc:
                sql_out += "--NOTE: LOCATION NOT IN PROVIDED LIST\n"
                (dummy, loc) = loc.split(':', 1)

            query = "SELECT * FROM keyword_usap WHERE keyword_label = '%s';" % loc
            cur.execute(query)
            res = cur.fetchone()
            if not res:
                sql_out += "--ADDING LOCATION %s TO keyword_usap TABLE\n" % loc

                # first work out the last keyword_id used
                query = "SELECT keyword_id FROM keyword_usap ORDER BY keyword_id DESC"
                cur.execute(query)
                res = cur.fetchone()
                if not next_id:
                    last_id = res['keyword_id'].replace('uk-', '')
                    next_id = int(last_id) + 1
                else:
                    next_id += 1
                # now insert new record into db
                sql_out += "INSERT INTO keyword_usap (keyword_id, keyword_label, keyword_description, keyword_type_id, source) VALUES ('uk-%s', '%s', '', 'kt-0006', 'submission Placename');\n" % \
                    (next_id, loc)
                sql_out += "INSERT INTO project_keyword_map(proj_uid,  keyword_id) VALUES ('%s','uk-%s'); -- %s\n" % (uid, next_id, loc)

            else:
                sql_out += "INSERT INTO project_keyword_map(proj_uid,  keyword_id) VALUES ('%s','%s'); -- %s\n" % (uid, res['keyword_id'], loc)
                # if GCMD_location add to project_gcmd_location_map
                query = "SELECT * FROM gcmd_location WHERE keyword_id = '%s';" % res['keyword_id']
                cur.execute(query)
                res = cur.fetchone()
                if res:
                    sql_out += "INSERT INTO project_gcmd_location_map(proj_uid,  loc_id) VALUES ('%s','%s');\n" % (uid, res['id'])
                    gcmd_locations.append(res['id'])
        # If not included, add Continent->Antarctica and OCEAN -> Southern Ocean to the project_gcmd_location_map.  
        # These can be deleted by the curator is desired.
        if 'CONTINENT > ANTARCTICA' not in gcmd_locations:
            sql_out += "--NOTE: Suggested gcmd_location. Remove if not appropriate for this project:\n"
            sql_out += "INSERT INTO project_gcmd_location_map(proj_uid,  loc_id) VALUES ('%s','CONTINENT > ANTARCTICA');\n" % uid
        if 'OCEAN > SOUTHERN OCEAN' not in gcmd_locations:
            sql_out += "--NOTE: Suggested gcmd_location. Remove if not appropriate for this project:\n"
            sql_out += "INSERT INTO project_gcmd_location_map(proj_uid,  loc_id) VALUES ('%s','OCEAN > SOUTHERN OCEAN');\n" % uid

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

        geometry = "ST_GeomFromText('%s', 4326)" % makeCentroidGeom(north, south, east, west, data["cross_dateline"])
        bounds_geometry = "ST_GeomFromText('%s', 4326)" % makeBoundsGeom(north, south, east, west, data["cross_dateline"])

        sql_out += "--NOTE: adding spatial bounds\n"
        sql_out += "INSERT INTO project_spatial_map(proj_uid,west,east,south,north,cross_dateline,geometry,bounds_geometry) VALUES ('%s','%s','%s','%s','%s','%s',%s,%s);\n" % \
            (uid, west, east, south, north, data["cross_dateline"], geometry, bounds_geometry)

    sql_out += '\nCOMMIT;\n'
    
    return sql_out


def editProjectJson2sql(data, uid):
    conn, cur = usap.connect_to_db(curator=True)

    # generate the submitter's person id if we have their name
    subm_id = None
    if data.get('submitter_name'):
        names = data['submitter_name'].rsplit(' ',1)
        if len(names) > 1:
            subm_id = names[-1] + ', ' + names[0]

    pi_id = data['pi_name_last'] + ', ' + data['pi_name_first'] 
    pi_id = pi_id.replace(',  ', ', ')

    # get existing values from the database and compare them with the JSON file
    orig = usap.project_db2form(uid)

    # compare original with edited json
    updates = set()
    for k in orig.keys():
        if orig[k] != data.get(k) and not (orig[k] in ['None', None, ''] and data.get(k) in ['None', None, '']):
            print(k)
            print("orig:", orig.get(k))
            print("new:", data.get(k))
            if k in ['geo_e', 'geo_n', 'geo_s', 'geo_w', 'cross_dateline']:
                updates.add('spatial_extents')
            elif k in ['pi_name_last', 'pi_name_first']:
                updates.add('pi_name')
            else:
                updates.add(k)

    # check for orcid update
    if subm_id:
        query = "SELECT id_orcid FROM person WHERE id = '%s'" % usap.escapeQuotes(subm_id)
        cur.execute(query)
        res = cur.fetchone()
        if res and res['id_orcid'] != data.get('submitter_orcid'):
            updates.add('orcid')

    # --- fix award fields (throw out PI name)
    data["award_num"] = data["award"]
    if data["award"] not in ["None", "Not_In_This_List"] and len(data["award"].split(" ", 1)) > 1:
            data["award_num"] = data["award"].split(" ")[0]
    data["other_awards_num"] = data['other_awards'][:]
    for i in range(len(data["other_awards"])):
        if data["other_awards"][i]['id'] not in ["None", "Not In This List"] and len(data["other_awards"][i]['id'].split(" ", 1)) > 1:
            data["other_awards"][i]['id'] = data["other_awards"][i]['id'].split(" ")[0]  # throw away the rest of the award string

    # update database with edited values
    sql_out = ""
    sql_out += "START TRANSACTION;\n"
    for k in updates:
        if k == 'award':
            sql_out += "\n--NOTE: UPDATING AWARD\n"

            if data['award_num'] == '' or data['award_num'] == 'None':
                sql_out += "\n--NOTE: No award submitted - remove existing award\n"
                sql_out += "DELETE FROM project_award_map WHERE proj_uid = '%s';\n" % uid
            elif 'Not_In_This_List' in data['award_num']:
                sql_out += "\n--NOTE: Award not in provided list\n"
                (dummy, award) = data['award_num'].split(':', 1)
                # check if this award is already in the award table
                query = "SELECT COUNT(*) FROM  award WHERE award = '%s'" % award
                cur.execute(query)
                res = cur.fetchone()
                if res['count'] == 0:
                    # Add award to award table
                    sql_out += "\n--NOTE: Adding award %s to award table. Curator should update with any know fields.\n" % award
                    sql_out += "INSERT INTO award(award, dir, div, title, name) VALUES ('%s', 'GEO', 'DIV', 'TBD', 'TBD');\n" % award
                    sql_out += "\n--UPDATE award SET iscr='f', isipy='f', copi='', start='', expiry='', sum='', email='', orgcity='', orgzip='', dmp_link='' WHERE award='%s';\n" % award

                sql_out += "UPDATE project_award_map SET award_id = '%s' WHERE proj_uid = '%s' AND is_main_award = 'True';\n" % (award, uid)
            else:
                # check if old award has been moved to additional awards list
                if data.get('other_awards'):
                    orig_award_num = orig['award'].split(' ')[0]
                    if orig_award_num in [x['id'] for x in data['other_awards']]:
                        sql_out += "INSERT INTO project_award_map (proj_uid, award_id, is_main_award) VALUES ('%s', '%s', 'True');\n" % (uid, data['award_num'])
                    else:
                        sql_out += "UPDATE project_award_map SET award_id = '%s' WHERE proj_uid = '%s' AND is_main_award = 'True';\n" % (data['award_num'], uid)

                else:
                    sql_out += "UPDATE project_award_map SET award_id = '%s' WHERE proj_uid = '%s' AND is_main_award = 'True';\n" % (data['award_num'], uid)
        
        elif k == 'copis':
            sql_out += "\n--NOTE: UPDATING CO-PIS\n"

            # remove existing co-pis from project_person_map
            sql_out += "\n--NOTE: First remove all existing co-pis from project_person_map\n"
            sql_out += "DELETE FROM project_person_map WHERE proj_uid = '%s' and person_id != '%s';\n" % (uid, usap.escapeQuotes(pi_id))

            # Update personnel to the person table, if necessary, and project_person_map
            for co_pi in data['copis']:
                co_pi_id = co_pi['name_last'] + ', ' + co_pi['name_first']
                co_pi_id = co_pi_id.replace(',  ', ', ')
                query = "SELECT * FROM person WHERE id = '%s'" % usap.escapeQuotes(co_pi_id)
                cur.execute(query)
                res = cur.fetchall()

                if len(res) == 0:
                    sql_out += "--NOTE: adding %s to person table\n" % co_pi_id

                    if subm_id and subm_id == co_pi_id and data.get('submitter_orcid') and data['submitter_orcid'] != '':
                        # look for other possible person IDs that could belong to this person (maybe with/without middle initial, or orcid match)
                        sql_out += checkAltIds(co_pi_id, co_pi.get('name_first'), co_pi.get('name_last'), 'COPIS', data['submitter_orcid'])

                        sql_out += "INSERT INTO person (id, usap.escapeQuotes(first_name), usap.escapeQuotes(last_name), organization, id_orcid) " \
                                   "VALUES ('%s', '%s', '%s' ,'%s', '%s');\n\n" % \
                                   (usap.escapeQuotes(co_pi_id), usap.escapeQuotes(co_pi.get('name_first')), usap.escapeQuotes(co_pi.get('name_last')), co_pi.get('org'), data['submitter_orcid'])
                    else:
                        # look for other possible person IDs that could belong to this person (maybe with/without middle initial, or orcid match)
                        sql_out += checkAltIds(co_pi_id, co_pi.get('name_first'), co_pi.get('name_last'), 'COPIS')
                        sql_out += "INSERT INTO person (id, first_name, last_name, organization) " \
                                   "VALUES ('%s', '%s', '%s' ,'%s');\n\n" % \
                                   (usap.escapeQuotes(co_pi_id), usap.escapeQuotes(co_pi.get('name_first')), usap.escapeQuotes(co_pi.get('name_last')), co_pi.get('org'))
                else:
                    if co_pi.get('org') is not None and res[0]['organization'] != co_pi['org']:
                        sql_out += "--NOTE: updating organization for %s to %s\n" % (co_pi_id, co_pi['org'])
                        sql_out += "UPDATE person SET organization='%s' WHERE id='%s';\n\n" % (co_pi['org'], co_pi_id)
                    if subm_id and subm_id == co_pi_id and data.get('submitter_orcid') is not None and res[0]['id_orcid'] != data['submitter_orcid']:
                        sql_out += "--NOTE: updating orcid for %s to %s\n" % (co_pi_id, data['submitter_orcid'])
                        sql_out += "UPDATE person SET id_orcid='%s' WHERE id='%s';\n\n" % (data['submitter_orcid'], usap.escapeQuotes(co_pi_id))
                
                sql_out += "\n--NOTE: adding %s to project_person_map\n" % co_pi_id
                sql_out += "INSERT INTO project_person_map (proj_uid, person_id, role) VALUES ('%s', '%s', '%s');\n\n" % \
                           (uid, usap.escapeQuotes(co_pi_id), co_pi.get('role'))

                # add org to Organizations table if necessary
                if co_pi.get('org') is not None:
                    query = "SELECT COUNT(*) FROM organizations WHERE name = '%s'" % co_pi['org']
                    cur.execute(query)
                    res = cur.fetchall()
                    if res[0]['count'] == 0:
                        sql_out += "\n--NOTE: adding %s to orgainzations table\n" % co_pi['org']
                        sql_out += "INSERT INTO organizations (name) VALUES('%s');\n\n" % co_pi['org']

        elif k == 'datasets':
            sql_out += "\n--NOTE: UPDATING DATASETS\n"

            # remove existing datasets from project_dataset_map
            sql_out += "\n--NOTE: First remove all existing datasets from project_dataset_map\n"
            sql_out += "DELETE FROM project_dataset_map WHERE proj_uid = '%s';\n" % uid

            if data.get('datasets') and len(data['datasets']) > 0:
                sql_out += "\n--NOTE: adding project_datasets\n"

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
                        sql_out += "\n--NOTE: adding dataset %s to project_dataset table\n" % ds['title']
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
                        # update url if changed
                        if ds['url'] != res[0]['url']:
                            sql_out += "\n--NOTE: updating dataset\n"
                            sql_out += "UPDATE project_dataset SET url = '%s' WHERE dataset_id = '%s';\n" % (ds['url'], ds_id)
                    sql_out += "\n--NOTE: adding dataset %s to project_dataset_map\n" % ds_id
                    sql_out += "INSERT INTO project_dataset_map (proj_uid, dataset_id) VALUES ('%s', '%s');\n" % (uid, ds_id)
                sql_out += "\n"

        elif k == 'deployments':
            sql_out += "\n--NOTE: UPDATING DEPLOYMENTS\n"

            # remove existing datasets from project_dataset_map
            sql_out += "\n--NOTE: First remove all existing deployments from project_deployment\n"
            sql_out += "DELETE FROM project_deployment WHERE proj_uid = '%s';\n" % uid

            # add deployments from form
            sql_out += "\n--NOTE: Add deployments from submitted form\n"
            for dep in data['deployments']:
                sql_out += "INSERT INTO project_deployment (proj_uid, deployment_id, deployment_type, url) VALUES ('%s', '%s', '%s', '%s');\n" % \
                    (uid, dep.get('name'), dep.get('type'), dep.get('url'))
            sql_out += "\n"

        elif k == 'dmp_file':
            if data.get('dmp_file') is not None and data['dmp_file'] != '' and data.get('upload_directory') is not None \
               and data.get('award') is not None and data['award'] != '':
                dst = os.path.join(AWARDS_FOLDER, data['award'], data['dmp_file'])
                sql_out += "\n--NOTE: UPDATING DMP_LINK FOR AWARD %s\n" % data['award']
                sql_out += "UPDATE award SET dmp_link = '%s' WHERE award = '%s';\n" % (dst, data['award_num'])

        elif k == 'email':
            # first check if pi is already in person DB table - if not, email will get added when pi is created later in the code
            query = "SELECT COUNT(*) FROM person WHERE id = '%s'" % usap.escapeQuotes(pi_id)
            cur.execute(query)
            res = cur.fetchone()
            if res['count'] > 0:
                sql_out += "\n--NOTE: UPDATING EMAIL ADDRESS\n"
                sql_out += "UPDATE person SET email = '%s' WHERE id='%s';\n" % (data['email'], usap.escapeQuotes(pi_id))
 
        elif k == 'end':
            sql_out += "\n--NOTE: UPDATING END DATE\n"
            sql_out += "UPDATE project SET end_date = '%s' WHERE proj_uid = '%s';\n" % (data['end'], uid)

        elif k == 'locations':
            sql_out += "\n--NOTE: UPDATING LOCATIONS\n"

            # remove existing locations from project_gcmd_location_map
            sql_out += "\n--NOTE: First remove all existing locations from project_keyword_map and project_gcmd_location_map\n"
            sql_out += """DELETE FROM project_keyword_map pkm WHERE proj_uid = '%s' AND keyword_id IN (SELECT keyword_id FROM vw_location);\n""" % uid
            sql_out += "DELETE FROM project_gcmd_location_map WHERE proj_uid = '%s';\n" % uid

            sql_out += "\n--NOTE: adding locations\n"
            next_id = False
            gcmd_locations = []
            for loc in data['locations']:
                if 'Not_In_This_List' in loc:
                    sql_out += "--NOTE: LOCATION NOT IN PROVIDED LIST\n"
                    (dummy, loc) = loc.split(':', 1)

                query = "SELECT * FROM keyword_usap WHERE keyword_label = '%s';" % loc
                cur.execute(query)
                res = cur.fetchone()
                if not res:
                    sql_out += "--ADDING LOCATION %s TO keyword_usap TABLE\n" % loc

                    # first work out the last keyword_id used
                    query = "SELECT keyword_id FROM keyword_usap ORDER BY keyword_id DESC"
                    cur.execute(query)
                    res = cur.fetchone()
                    if not next_id:
                        last_id = res['keyword_id'].replace('uk-', '')
                        next_id = int(last_id) + 1
                    else:
                        next_id += 1
                    # now insert new record into db
                    sql_out += "INSERT INTO keyword_usap (keyword_id, keyword_label, keyword_description, keyword_type_id, source) VALUES ('uk-%s', '%s', '', 'kt-0006', 'submission Placename');\n" % \
                        (next_id, loc)
                    sql_out += "INSERT INTO project_keyword_map(proj_uid,  keyword_id) VALUES ('%s','uk-%s'); -- %s\n" % (uid, next_id, loc)

                else:
                    sql_out += "INSERT INTO project_keyword_map(proj_uid,  keyword_id) VALUES ('%s','%s'); -- %s\n" % (uid, res['keyword_id'], loc)
                    # if GCMD_location add to project_gcmd_location_map
                    query = "SELECT * FROM gcmd_location WHERE keyword_id = '%s';" % res['keyword_id']
                    cur.execute(query)
                    res = cur.fetchone()
                    if res:
                        sql_out += "INSERT INTO project_gcmd_location_map(proj_uid,  loc_id) VALUES ('%s','%s');\n" % (uid, res['id'])
                        gcmd_locations.append(res['id'])
            # If not included, add Continent->Antarctica and OCEAN -> Southern Ocean to the project_gcmd_location_map.  
            # These can be deleted by the curator is desired.
            if 'CONTINENT > ANTARCTICA' not in gcmd_locations:
                sql_out += "--NOTE: Suggested gcmd_location. Remove if not appropriate for this project:\n"
                sql_out += "INSERT INTO project_gcmd_location_map(proj_uid,  loc_id) VALUES ('%s','CONTINENT > ANTARCTICA');\n" % uid
            if 'OCEAN > SOUTHERN OCEAN' not in gcmd_locations:
                sql_out += "--NOTE: Suggested gcmd_location. Remove if not appropriate for this project:\n"
                sql_out += "INSERT INTO project_gcmd_location_map(proj_uid,  loc_id) VALUES ('%s','OCEAN > SOUTHERN OCEAN');\n" % uid   
            elif k == 'orcid':
                sql_out += "\n--NOTE: UPDATING ORCID\n"
                sql_out += "UPDATE person SET id_orcid = '%s' WHERE id='%s';\n" % (data['submitter_orcid'], usap.escapeQuotes(subm_id))

        elif k == 'org':
            sql_out += "\n--NOTE: UPDATING ORGANIZATION\n"
            sql_out += "UPDATE person SET organization = '%s' WHERE id='%s';\n" % (data['org'], usap.escapeQuotes(pi_id))

            # add org to Organizations table if necessary
            if data.get('org') is not None:
                query = "SELECT COUNT(*) FROM organizations WHERE name = '%s'" % data['org']
                cur.execute(query)
                res = cur.fetchall()
                if res[0]['count'] == 0:
                    sql_out += "\n--NOTE: adding %s to orgainzations table\n" % data['org']
                    sql_out += "INSERT INTO organizations (name) VALUES('%s');\n\n" % data['org']
        
        elif k == 'other_awards':
            sql_out += "\n--NOTE: UPDATING OTHER AWARDS\n"

            # remove existing additional awards from project_award_map
            sql_out += "\n--NOTE: First remove existing additional awards from project_award_map\n"
            sql_out += "DELETE FROM project_award_map WHERE proj_uid = '%s' AND is_main_award = 'False';\n" % (uid)

            # if other_awards now includes the old main award, need to remove that from the project_award_map too
            if orig.get('award'):
                orig_award_num = orig['award'].split(' ')[0]
                if orig_award_num in [x['id'] for x in data['other_awards']]:
                    sql_out += "--NOTE: main award has changed, so remove old one from project_award_map\n"
                    sql_out += "DELETE FROM project_award_map WHERE proj_uid = '%s' AND award_id = '%s' AND is_main_award = 'True';\n" % (uid, orig_award_num)   

            for other_award in data['other_awards']:
                if 'Not_In_This_List' in other_award:
                    sql_out += "\n--NOTE: AWARD NOT IN PROVIDED LIST\n"
                    (dummy, award) = other_award['id'].split(':', 1)
                    # check if this award is already in the award table
                    query = "SELECT COUNT(*) FROM  award WHERE award = '%s'" % award
                    cur.execute(query)
                    res = cur.fetchone()
                    if res['count'] == 0:
                        # Add award to award table
                        sql_out += "\n--NOTE: Adding award %s to award table. Curator should update with any know fields.\n" % award
                        sql_out += "INSERT INTO award(award, dir, div, title, name) VALUES ('%s', 'GEO', 'DIV', 'TBD', 'TBD');\n" % award
                        sql_out += "--UPDATE award SET iscr='f', isopy='f', copi='', start='', expiry='', sum='', email='', orgcity='', orgzip='', dmp_link='' WHERE award='%s';\n" % award

                else:
                    award = other_award['id']
                sql_out += "INSERT INTO project_award_map (proj_uid, award_id, is_main_award, is_previous_award) VALUES ('%s', '%s', 'False', %s);\n" % (uid, award, other_award['is_previous_award'])
            sql_out += "\n"

        elif k == 'parameters':
            sql_out += "\n--NOTE: UPDATING GCMD SCIENCE KEYWORDS\n"

            # remove existing locations from project_gcmd_location_map
            sql_out += "\n--NOTE: First remove all existing keywords from project_gcmd_science_key_map\n"
            sql_out += "DELETE FROM project_gcmd_science_key_map WHERE proj_uid = '%s';\n" % uid

            sql_out += "\n--NOTE: adding gcmd_science_keys\n"
            for param in data['parameters']:
                sql_out += "INSERT INTO project_gcmd_science_key_map (proj_uid, gcmd_key_id) VALUES ('%s', '%s');\n" % (uid, param)
            sql_out += "\n"

        elif k == 'pi_name':
            sql_out += "\n--NOTE: UPDATING PI\n"

            # Remove old PI from project_person_map
            sql_out += "\n--NOTE: First remove all existing PI from project_person_map\n"
            sql_out += "DELETE FROM project_person_map WHERE proj_uid = '%s' and role = 'Investigator and contact';\n" % uid

            # Check if new pi_name already in person table, add if not
            query = "SELECT * FROM person WHERE id = '%s'" % usap.escapeQuotes(pi_id)
            cur.execute(query)
            res = cur.fetchall()
            if len(res) == 0:
                sql_out += "\n--NOTE: adding PI to person table\n"

                if subm_id and subm_id == pi_id and data.get('submitter_orcid') and data['submitter_orcid'] != '':
                    # look for other possible person IDs that could belong to this person (maybe with/without middle initial, or orcid or email match)
                    sql_out += checkAltIds(pi_id, data.get('pi_name_first'), data.get('pi_name_last'), 'PI_NAME_FIRST AND PI_NAME_LAST', data['submitter_orcid'], data.get('email'))

                    sql_out += "INSERT INTO person (id, first_name, last_name, email, organization, id_orcid) " \
                               "VALUES ('%s', '%s', '%s', '%s', '%s', '%s');\n\n" % \
                        (usap.escapeQuotes(pi_id), usap.escapeQuotes(data.get('pi_name_first')), usap.escapeQuotes(data.get('pi_name_last')), data.get('email'), data.get('org'), data['submitter_orcid'])
                else:
                    # look for other possible person IDs that could belong to this person (maybe with/without middle initial, or email match)
                    sql_out += checkAltIds(pi_id, data.get('pi_name_first'), data.get('pi_name_last'), 'PI_NAME_FIRST AND PI_NAME_LAST', email=data.get('email'))

                    sql_out += "INSERT INTO person (id, first_name, last_name, email, organization) " \
                               "VALUES ('%s', '%s', '%s', '%s', '%s');\n\n" % \
                        (usap.escapeQuotes(pi_id), usap.escapeQuotes(data.get('pi_name_first')), usap.escapeQuotes(data.get('pi_name_last')), data.get('email'), data.get('org'))

            else:
                if data.get('email') is not None and res[0]['email'] != data['email']:
                    sql_out += "\n--NOTE: updating email for PI %s to %s\n" % (pi_id, data['email'])
                    sql_out += "UPDATE person SET email='%s' WHERE id='%s';\n\n" % (data['email'], usap.escapeQuotes(pi_id))
                if data.get('org') is not None and res[0]['organization'] != data['org']:
                    sql_out += "\n--NOTE: updating organization for PI %s to %s\n" % (pi_id, data['org'])
                    sql_out += "UPDATE person SET organization='%s' WHERE id='%s';\n\n" % (data['org'], usap.escapeQuotes(pi_id))
                # if subm_id and subm_id == pi_id and data.get('submitter_orcid') is not None and res[0]['id_orcid'] != data['submitter_orcid']:
                #     sql_out += "\n--NOTE: updating orcid for PI %s to %s\n" % (pi_id, data['submitter_orcid'])
                #     sql_out += "UPDATE person SET id_orcid='%s' WHERE id='%s';\n\n" % (data['submitter_orcid'], pi_id)

            # Insert new PI into project_person_map
            sql_out += "\n--NOTE: adding PI %s to project_person_map\n" % pi_id
            sql_out += "INSERT INTO project_person_map (proj_uid, person_id, role) VALUES ('%s', '%s', 'Investigator and contact');\n" % \
                       (uid, usap.escapeQuotes(pi_id))

        elif k == 'program':
            sql_out += "\n--NOTE: UPDATING INITIATIVE\n"

            # remove existing initiative from project_award_map
            sql_out += "\n--NOTE: First remove initiatives from project_initiative_map\n"
            sql_out += "DELETE FROM project_initiative_map WHERE proj_uid = '%s';\n" % (uid)

            if data.get('program') is not None and data['program'] != "":
                initiative = data['program']['id']
                sql_out += "\n--NOTE: adding initiative to project_initiative_map\n"
                sql_out += "INSERT INTO project_initiative_map (proj_uid, initiative_id) VALUES ('%s', '%s');\n" % \
                    (uid, initiative)

        elif k == 'publications':
            sql_out += "\n--NOTE: UPDATING PUBLICATIONS\n"

            # remove existing publications from project_ref_map
            sql_out += "\n--NOTE: First remove all existing publications from project_ref_map\n"
            sql_out += "DELETE FROM project_ref_map WHERE proj_uid = '%s';\n" % uid

            # add references
            if data.get('publications') is not None and len(data['publications']) > 0:
                sql_out += "\n--NOTE: adding references\n"

                for pub in data['publications']:
                    # see if they are already in the references table
                    res = []
                    if pub.get('doi'):
                        query = "SELECT * FROM reference WHERE doi='%s';" % (pub.get('doi'))
                        cur.execute(query)
                        res = cur.fetchall()
                    elif (not pub.get('doi') or pub['doi'] == '') and pub.get('name'):
                        query = "SELECT * FROM reference WHERE (doi IS NULL OR doi='') AND ref_text='%s';" % (usap.escapeQuotes(pub.get('name')))
                        print(query)
                        cur.execute(query)
                        res = cur.fetchall()

                    if len(res) == 0:
                        sql_out += "\n--NOTE: adding %s to reference table\n" % pub['name']
                        ref_uid = generate_ref_uid()
                        sql_out += "INSERT INTO reference (ref_uid, ref_text, doi) VALUES ('%s', '%s', '%s');\n" % \
                            (ref_uid, usap.escapeQuotes(pub.get('name')), pub.get('doi'))
                    else:
                        ref_uid = res[0]['ref_uid']
                    sql_out += "\n--NOTE: adding reference %s to project_ref_map\n" % ref_uid
                    sql_out += "INSERT INTO project_ref_map (proj_uid, ref_uid) VALUES ('%s', '%s');\n" % \
                        (uid, ref_uid)

                sql_out += "\n"

        elif k == 'short_title':
            sql_out += "\n--NOTE: UPDATING SHORT TITLE\n"
            sql_out += "UPDATE project SET short_name = '%s' WHERE proj_uid = '%s';\n" % (data['short_title'], uid)
    
        elif k == 'spatial_extents':
            sql_out += "\n--NOTE: UPDATING PROJECT_SPATIAL_MAP\n"
            sql_out += updateProjectSpatialMap(uid, data, False)

        elif k == 'start':
            sql_out += "\n--NOTE: UPDATING START DATE\n"
            sql_out += "UPDATE project SET start_date = '%s' WHERE proj_uid = '%s';\n" % (data['start'], uid)

        elif k == 'sum':
            sql_out += "\n--NOTE: UPDATING ABSTRACT\n"
            sql_out += "UPDATE project SET description = '%s' WHERE proj_uid = '%s';\n" % (usap.escapeQuotes(data['sum']), uid)

        elif k == 'title':
            sql_out += "\n--NOTE: UPDATING TITLE\n"
            sql_out += "UPDATE project SET title = '%s' WHERE proj_uid = '%s';\n" % (data['title'], uid)

        elif k == 'websites': 
            sql_out += "\n--NOTE: UPDATING WEBSITES\n"

            # remove existing websites from project_website
            sql_out += "\n--NOTE: First remove all existing websites from project_website\n"
            sql_out += "DELETE FROM project_website WHERE proj_uid = '%s';\n" % uid

            # add websites
            sql_out += "\n--NOTE: adding project_websites\n"
            for ws in data['websites']:
                sql_out += "INSERT INTO project_website (proj_uid, title, url) VALUES ('%s', '%s', '%s');\n" % \
                    (uid, ws.get('title'), ws.get('url'))
            sql_out += "\n"
    
    sql_out += "\nUPDATE project SET date_modified = '%s' WHERE proj_uid= '%s';\n" % (data['timestamp'][0:10], uid)
    sql_out += '\nCOMMIT;\n'

    return sql_out


# update json file to mark edit as complete
def updateEditFile(uid):
    submitted_dir = os.path.join(current_app.root_path, usap.app.config['SUBMITTED_FOLDER'])
    submission_file = os.path.join(submitted_dir, "e" + uid + ".json")
    print(submission_file)
    if not os.path.exists(submission_file):
        return False
    with open(submission_file, 'r') as file:
        data = json.load(file)
    data['edit_complete'] = True
    with open(submission_file, 'w') as file:
        file.write(json.dumps(data, indent=4, sort_keys=True))
    os.chmod(submission_file, 0o664)


def isEditComplete(uid):
    submitted_dir = os.path.join(current_app.root_path, usap.app.config['SUBMITTED_FOLDER'])
    submission_file = os.path.join(submitted_dir, uid + ".json")
    if not os.path.exists(submission_file):
        return False
    with open(submission_file) as infile:
        data = json.load(infile)
    return data.get('edit_complete')


def getDifID(uid):
    conn, cur = usap.connect_to_db()
    query = "SELECT award_id FROM project_award_map WHERE is_main_award = 'True' AND proj_uid = '%s';" % uid
    cur.execute(query)
    res = cur.fetchone()
    return "USAP-%s_1" % res['award_id']  


def getDifIDAndTitle(uid):
    conn, cur = usap.connect_to_db()
    query = "SELECT award_id, title FROM project_award_map pam " \
            "JOIN award a ON pam.award_id = a.award " \
            "WHERE pam.is_main_award = 'True' AND pam.proj_uid = '%s';" % uid
    cur.execute(query)

    res = cur.fetchone()
    return "USAP-%s_1" % res['award_id'], res['title']  


def getDifUrl(uid):
    return 'https://gcmd.nasa.gov/search/Metadata.do?entry=%s' % getDifID(uid)


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
            name_last, name_first = person.get('id').split(',', 1)
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
    if data.get('gcmd_locations'):
        for location in data['gcmd_locations']:
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
    xml_org_url.text = request.url_root

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
    (conn, cur) = usap.connect_to_db(curator=True)
    status = 1
    if type(conn) is str:
        out_text = conn
        status = 0
    else:
        try:
            sql_cmd = ""
            dif_id, title = getDifIDAndTitle(uid)
            # Add to dif table if not already there
            query = "SELECT * FROM dif WHERE dif_id = '%s';" % dif_id
            cur.execute(query)
            res = cur.fetchall()
            if len(res) == 0:
                sql_cmd += "INSERT INTO dif (dif_id, date_created, date_modified, title, is_usap_dc, is_nsf) VALUES ('%s', '%s', '%s', '%s', %s, %s);\n" % \
                    (dif_id, datetime.now().date(), datetime.now().date(), title, True, True)

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


def addUserToDatasetOrProject(data):
    try:
        (conn, cur) = usap.connect_to_db(curator=True)
        sql_out = ""

        person_id = data['name_last'] + ', ' + data['name_first'] 
        person_id = person_id.replace(',  ', ', ')

        # first check if the user is already in person table
        query = "SELECT * FROM person WHERE id = '%s'" % usap.escapeQuotes(person_id)
        cur.execute(query)
        res = cur.fetchall()
        if len(res) == 0:

            if data.get('orcid') and data['orcid'] != '':
                sql_out += "INSERT INTO person (id, first_name, last_name, email, organization, id_orcid) " \
                           "VALUES ('%s', '%s', '%s', '%s', '%s', '%s');\n\n" % \
                    (usap.escapeQuotes(person_id), usap.escapeQuotes(data.get('name_first')), usap.escapeQuotes(data.get('name_last')), data.get('email'), data.get('org'), data['orcid'])
            else:
                sql_out += "INSERT INTO person (id, first_name, last_name, email, organization) " \
                           "VALUES ('%s', '%s', '%s', '%s', '%s');\n\n" % \
                    (usap.escapeQuotes(person_id), usap.escapeQuotes(data.get('name_first')), usap.escapeQuotes(data.get('name_last')), data.get('email'), data.get('org'))

        else:
            if data.get('email') and data['email'] != '' and res[0]['email'] != data['email']:
                sql_out += "UPDATE person SET email='%s' WHERE id='%s';\n\n" % (data['email'], usap.escapeQuotes(person_id))
            if data.get('org') and data['org'] != '' and res[0]['organization'] != data['org']:
                sql_out += "UPDATE person SET organization='%s' WHERE id='%s';\n\n" % (data['org'], usap.escapeQuotes(person_id))
            if data.get('orcid') and data['orcid'] != '' and res[0]['id_orcid'] != data['orcid']:
                sql_out += "UPDATE person SET id_orcid='%s' WHERE id='%s';\n\n" % (data['orcid'], usap.escapeQuotes(person_id))

        uid, title = data.get('dataset_or_project').split(': ', 1)
        if uid[0] == 'p':
            # PROJECT
            sql_out += "INSERT INTO project_person_map (proj_uid, person_id, role) VALUES ('%s', '%s', '%s');\n\n" % \
                (uid, usap.escapeQuotes(person_id), data.get('role'))
        else:
            # DATASET
            sql_out += "INSERT INTO dataset_person_map (dataset_id, person_id, role_id) VALUES ('%s', '%s', '%s');\n\n" % \
                (uid, usap.escapeQuotes(person_id), data.get('role'))

        sql_out += "COMMIT;"
        cur.execute(sql_out)

        return None
    except Exception as e:
        return str(e)


def makeCentroidGeom(north, south, east, west, cross_dateline):
    mid_point_lat = (south - north) / 2 + north

    if cross_dateline:
        mid_point_long = west - (west - east + 360)/2
    else:
        mid_point_long = (east - west) / 2 + west
        if (west > east):
            mid_point_long += 180

    if mid_point_long < -180:
        mid_point_long += 360
    elif mid_point_long > 180:
        mid_point_long -= 360

    # if geometric bound describe a circle or a ring, set the centroid at the south pole
    if (north != south) and ((round(west, 2) == -180 and round(east, 2) == 180) or (east == west)):
        mid_point_lat = -89.999
        mid_point_long = 0

    geom = "POINT(%s %s)" % (mid_point_long, mid_point_lat)
    return geom


def makeBoundsGeom(north, south, east, west, cross_dateline):
    # point
    if (west == east and north == south):
        geom = "POINT(%s %s)" % (west, north)

    # polygon
    else:
        geom = "POLYGON(("
        n = 10
        if (cross_dateline):
            dlon = (-180 - west) / n
            dlat = (north - south) / n
            for i in range(n):
                geom += "%s %s," % (-180 - dlon * i, north)

            for i in range(n):
                geom += "%s %s," % (west, north - dlat * i)

            for i in range(n):
                geom += "%s %s," % (west + dlon * i, south)

            dlon = (180 - east) / n
            for i in range(n):
                geom += "%s %s," % (180 - dlon * i, south)

            for i in range(n):
                geom += "%s %s," % (east, south + dlat * i)

            for i in range(n):
                geom += "%s %s," % (east + dlon * i, north)
            # close the ring ???
            geom += "%s %s," % (-180, north)

        elif east > west:
            dlon = (west - east) / n
            dlat = (north - south) / n
            for i in range(n):
                geom += "%s %s," % (west - dlon * i, north)

            for i in range(n):
                geom += "%s %s," % (east, north - dlat * i)

            for i in range(n):
                geom += "%s %s," % (east + dlon * i, south)

            for i in range(n):
                geom += "%s %s," % (west, south + dlat * i)
            # close the ring
            geom += "%s %s," % (west, north)

        else:
            dlon = (-180 - east) / n
            dlat = (north - south) / n
            for i in range(n):
                geom += "%s %s," % (-180 - dlon * i, north)

            for i in range(n):
                geom += "%s %s," % (east, north - dlat * i)

            for i in range(n):
                geom += "%s %s," % (east + dlon * i, south)

            dlon = (180 - west) / n
            for i in range(n):
                geom += "%s %s," % (180 - dlon * i, south)

            for i in range(n):
                geom += "%s %s," % (west, south + dlat * i)

            for i in range(n):
                geom += "%s %s," % (west + dlon * i, north)
            # close the ring ???
            geom += "%s %s," % (-180, north)

        geom = geom[:-1] + "))"
    return geom


def getCreatorEmails(uid):
    conn, cur = usap.connect_to_db()
    if uid[0] == 'p':
        # get project creators
        query = "SELECT id, email FROM person JOIN project_person_map ppm ON person.id = ppm.person_id " + \
                "WHERE proj_uid = '%s' AND email <> '' AND role IN ('Investigator', 'Investigator and contact')" % uid
        cur.execute(query)
        res = cur.fetchall()
    else:
        # get dataset creators
        query = "SELECT creator FROM dataset WHERE id = '%s';" % uid
        cur.execute(query)
        res = cur.fetchone()
        if res:
            creators = res['creator'].split('; ')
            query = "SELECT id, email FROM person WHERE id IN (%s) AND email <> ''" % (', '.join("'" + usap.escapeQuotes(item) + "'" for item in creators))
            cur.execute(query)
            res = cur.fetchall()
        else:
            res = []
    emails_string = ""
    emails_string = '\n'.join(['"%s" <%s>' % (r.get('id'), r.get('email')) for r in res])

    return emails_string


def getReplacedDataset(uid):
    # determine if this dataset replaces an older one, and if so, return the replaced dataset id
    conn, cur = usap.connect_to_db()
    query = "SELECT replaces FROM dataset WHERE id = '%s';" % uid
    cur.execute(query)
    res = cur.fetchone()
    if res:
        return res['replaces']
    return None


def updateRecentData(uid):
    # update the recent_data.txt file
    conn, cur = usap.connect_to_db()
    query = "SELECT id, title, creator, date_created, string_agg(award_id,',') AS awards " + \
            "FROM dataset ds LEFT JOIN dataset_award_map dam on dam.dataset_id = ds.id " + \
            "WHERE ds.id = '%s'" % uid + \
            "GROUP BY ds.id;"
    cur.execute(query)
    res = cur.fetchone()
    entries = {}
    if res:
        newline = "%s\t<a href=/view/dataset/%s>%s</a>\t%s\t%s\n" % (res.get('date_created'), uid, res.get('awards', 'Award Not Known'), 
                                                                     res.get('creator'), res.get('title'))
        try:
            # read in file
            with open(RECENT_DATA_FILE, 'r') as rd_file:
                lines = rd_file.readlines()
            for line in lines[1:]:
                entries[line] = line[0:10]
            if newline not in lines:
                entries[newline] = newline[0:10]
                # re-write file
                with open(RECENT_DATA_FILE, 'w') as rd_file:
                    rd_file.write(lines[0])
                    for entry in sorted(entries.items(), key=lambda x: datetime.strptime(x[1], '%Y-%m-%d'), reverse=True):
                        rd_file.write(entry[0])
            return None
        except Exception as e:
            return("Error updating Recent Data file: %s" % str(e))
    return("Error updating Recent Data file: can't find database record for %s." % uid)


def getLandingPage(uid):
    landing_page = ''
    conn, cur = usap.connect_to_db()
    if (uid.find('p') > -1):
        query = "SELECT COUNT(*) FROM project WHERE proj_uid = '%s';" % uid.replace('e', '')
        cur.execute(query)
        res = cur.fetchone()
        if res['count'] > 0:
            landing_page = '/view/project/%s' % uid.replace('e', '')
    else:
        query = "SELECT COUNT(*) FROM dataset WHERE id = '%s';" % uid.replace('e', '')
        cur.execute(query)
        res = cur.fetchone()
        if res['count'] > 0:
            landing_page = '/view/dataset/%s' % uid.replace('e', '')

    return landing_page


def generate_ref_uid():
    old_uid = int(open(REF_UID_FILE, 'r').readline().strip())
    new_uid = old_uid + 1
    ref_uid = 'ref_%0*d' % (7, new_uid)

    with open(REF_UID_FILE, 'w') as refFile:
        refFile.write(str(new_uid))

    return ref_uid


def getWeeklyReportOptions(uid):
    conn, cur = usap.connect_to_db(curator=True)
    query = "SELECT * FROM dataset_weekly_report WHERE dataset_id = '%s';" % uid.replace('e', '')
    cur.execute(query)
    res = cur.fetchone()
    return res


def checkThreadInDb(id, opened_date):
    conn, cur = usap.connect_to_db(curator=True)
    query = "SELECT * FROM email_threads WHERE id = '%s';" %id
    cur.execute(query)
    res = cur.fetchone()
    if res:
        return res['state'], res['closed_date']
    else:
        query = "INSERT INTO email_threads(id, state, opened_date) VALUES ('%s','open', '%s');" %(id, parse(opened_date))
        cur.execute(query)
        cur.execute("COMMIT;")
        return "open", None


def updateThreadState(id, state):
    conn, cur = usap.connect_to_db(curator=True)
    if state == 'closed':
        query = "UPDATE email_threads SET (state, closed_date)=('%s', '%s') WHERE id='%s';" %(state, datetime.now(), id)
    else: 
        query = "UPDATE email_threads SET (state, closed_date) =('%s', null) WHERE id='%s';" %(state, id)
    cur.execute(query)
    cur.execute("COMMIT;")    


def getThreadNumbers(start_date='2020-09-01', end_date=datetime.now().date()):    
    conn, cur = usap.connect_to_db(curator=True)
    query = """SELECT COUNT(id), state FROM email_threads
                WHERE opened_date >= '%s' AND opened_date <= '%s'
                GROUP BY state""" % (start_date, end_date)
    cur.execute(query)
    res = cur.fetchall()
    counts={'open':0, 'closed':0, 'all':0}
    for row in res:
        if row['state'] == 'open':
            counts['open'] = row['count']
        elif row['state'] == 'closed':
            counts['closed'] = row['count']
        counts['all'] += row['count']
    return counts


def getArchiveStatus(uid):
    conn, cur = usap.connect_to_db(curator=True)
    query = "SELECT * FROM dataset_archive WHERE dataset_id = '%s';" %uid
    cur.execute(query)
    res = cur.fetchone()
    return res


def markReadyToArchive(uid):
    try:
        status = getArchiveStatus(uid)
        if status:
            query = "UPDATE dataset_archive SET status = 'Ready To Be Archived' WHERE dataset_id = '%s';" %uid
        else:
            query = "INSERT INTO dataset_archive (dataset_id, status) VALUES (%s, 'Ready To Be Archived');" %uid
        conn, cur = usap.connect_to_db(curator=True)
        cur.execute(query)
        cur.execute('COMMIT;')
        return ('Dataset marked Ready To Be Archived', None)
    except:
        out_text = "Error Achiving database. \n%s" % sys.exc_info()[1][0]
        return (None, out_text)


def updateMatViews():
    conn, cur = usap.connect_to_db(curator=True)
    query = """REFRESH MATERIALIZED VIEW project_view; 
               REFRESH MATERIALIZED VIEW dataset_view;
               COMMIT;"""
    cur.execute(query)


def getCMRText(data, uid):
    
    conn, cur = usap.connect_to_db()
    text = ""
    try:
        text += "<b>Short name / version:</b><br>"
        # query = """SELECT dif_id FROM dif_award_map dam 
        #            JOIN  project_award_map pam ON pam.award_id = dam.award
        #            WHERE pam.is_main_award is true AND pam.proj_uid = '%s';""" % uid
        # cur.execute(query)
        # res = cur.fetchone()
        if data['dif_records']:
            values = data['dif_records'][0]['dif_id'].rsplit('_', 1)
            text += 'short name: %s<br> ' % values[0]
            if len(values) > 1:
                text += 'version   : %s<br>' % values[1]

        text +="<br><b>Version Description:</b><br>"
        text += "leave empty<br>"

        text += "<br><b>Entry Title:</b><br>"
        text += data['title']

        text += "<br><br><b>DOI:</b><br>"
        text += "Select DOI Not Available<br>"
        text += "Missing reason: Not Applicable<br>"

        # if datasets exists, print dataset DOIs
        text += "<br><b>Associated DOIs:</b><br>"
        text += "Could use this for dataset DOIs<br>"
        text += " (Was just recently added by GCMD)<br>"

        text += "<br><b>Abstract:</b><br>"
        text += data['description']

        text += "<br><br><b>Purpose:</b><br>"
        text += "leave empty<br>"

        text += "<br><b>Data Language:</b><br>"
        text += "English<br>"

        text += "<br><b>Data Dates:</b><br>"
        text += "Creation: %s<br>" % data['date_created']
        
        text += "<br><b>Collection Data Type:</b><br>"
        text += "Science Quality (or other, if appropriate)<br>"

        text += "<br><b>Processing Level -  description:</b><br>"
        text += "0 - raw data<br>"
        text += "1 - processed data<br>"
        text += "2 - model or derived data<br>"
        text += "or select: Not provided<br>"

        text += "<br><b>Collection Process:</b><br>"
        text += "Active or Complete or Not Provided<br>"

        text += "<br><b>Quality:</b><br>"
        text += "(leave empty)<br>"

        text += "<br><b>Use Constraints:</b><br>"
        text += "leave empty or add CC-BY with other fields empty<br>"

        text += "<br><b>Licence text:</b><br>" # need to come up with something here
        text += "leave empty otherwise provides error<br>"

        text += "<br><b>Access Constraints:</b><br>"
        text += "Value: 0<br>"
        text += "Description: Public<br>"

        text += "<br><b>Metadata Association:</b><br>"
        text += "leave empty<br>"

        text += "<br><b>Publications:</b><br>"
        if data['reference_list']:
            for entry in data['reference_list']:
                text += "&nbsp;&nbsp;%s<br>" % entry['ref_text']
                text += "doi: %s<br>" % entry['doi']
                text += "Authority: https://doi.org/<br><br>"
 

        text += "<br><b>Related URLs:</b><br>"
        if data['datasets']:
            for entry in data['datasets']:
                text += "Description: %s<br>" % entry['title']
                text += "&nbsp;&nbsp;URL Content Type: Distribution URL<br>"
                text += "&nbsp;&nbsp;Type: Get Data<br>"
                text += "&nbsp;&nbsp;URL: %s<br>" % entry['url']
    
        text += "<br>Description: USAP-DC Project page<br>"
        text += "&nbsp;&nbsp;Collection URL<br>"
        text += "&nbsp;&nbsp;Type: Project Home Page<br>"
        text += "&nbsp;&nbsp;URL : https://www.usap-dc.org/view/project/%s<br>" % uid

        if data['website']:
            for entry in data['website']:
                text += "<br>Description: %s<br>" % entry['title']
                text += "&nbsp;&nbsp;Collection URL<br>"
                text += "&nbsp;&nbsp;Type: Project Home Page<br>"
                text += "&nbsp;&nbsp;URL : %s<br>" % entry['url']
        
        if data['funding']:
            for entry in data['funding']:
                text += "<br>Description: NSF Award Information<br>"
                text += "&nbsp;&nbsp;Publication URL<br>"
                text += "&nbsp;&nbsp;View Related Information<br>"
                text += "&nbsp;&nbsp;URL   : http://www.nsf.gov/awardsearch/showAward.do?AwardNumber=%s<br>" % entry['award']


        query = """SELECT gcmd_key_id FROM project_gcmd_science_key_map WHERE proj_uid = '%s';""" % uid
        cur.execute(query)
        res = cur.fetchall()
        text += "<br><b>Science Keywords:</b><br>"
        for entry in res:
            text += "%s<br>" % entry['gcmd_key_id']
        
        query = """SELECT k1.keyword_label 
				   FROM project_keyword_map p1 
                   inner join keyword_usap k1 on p1.keyword_id = k1.keyword_id 
                   WHERE p1.proj_uid = '%s';""" % uid
        cur.execute(query)
        res = cur.fetchall()
        text += "<br><b>Auxillary Keywords:</b><br>"
        text += "USAP-DC<br>"
        text += "AMD<br>"
        text += "AMD/US<br>"
        text += "USA/NSF<br><br>"
        for entry in res:
            text += "%s<br>" % entry['keyword_label']

        text += "<br><b>Platform:</b><br>"
        text += "&nbsp;&nbsp;empty or choose one<br>"

        text += "<br><b>Instrument:</b><br>"
        text += "&nbsp;&nbsp;empty or choose one<br>"

        text += "<br><b>Project:</b><br>"
        text += "Short name: NSF/OPP<br>"
        text += "Long Name: Office of Polar Programs, National Science Foundation<br>"
        query = """SELECT deployment_id, deployment_type  FROM project_deployment WHERE proj_uid = '%s';""" % uid
        cur.execute(query)
        res = cur.fetchall()
        text += "Deployments: %s<br>" % res

        text += "<br><b>Temporal Extent:</b><br>"
        text += "Range<br>"
        text += "Ends At Present Flag: False<br>"
        text += "start : %s<br>" % data['start_date']
        text += "end : %s<br>" % data['end_date']
        
        text += "<br><b>Temporal Keywords:</b><br>"
        text += "leave empty, if not approriate<br>"

        text += "<br><b>Paleo Temporal Coverage:</b><br>"
        text += "leave empty or choose one<br>"

        text += "<br><b>Spatial Extent:</b><br>"
        text += "Spatial Coverage Type: Horizontal<br>"
        text += "Coordinate System: Cartesian<br>"
        text += "bounding box<br>"
        if data['spatial_bounds']:
            text += "North : %s<br>" % data['spatial_bounds'][0]['north']
            text += "South : %s<br>" %  data['spatial_bounds'][0]['south']
            text += "West  : %s<br>" %  data['spatial_bounds'][0]['west']
            text += "East : %s<br>" %  data['spatial_bounds'][0]['east']
        else:
            text += "North :<br>" 
            text += "South :<br>"
            text += "West  :<br>"
            text += "East :<br>"

        text += "Granule Spatial Representation: Cartesian<br>"

        text += "<br><b>Location Keywords:</b><br>"
        if data['gcmd_locations']:
            for item in data['gcmd_locations']:
                text += "%s<br>" % item['id']
        text += "GEOGRAPHIC REGION > POLAR<br>"

        text += "<br><b>Data Centers:</b><br>"
        text += "Archiver Distributor<br>"
        text += "USAP-DC<br>"
        text += "United States Antarctic Program Data Center<br>"
        text += "&nbsp;&nbsp;Contact Type: email<br>"
        text += "&nbsp;&nbsp;&nbsp;&nbsp;value: info@usap-dc.org<br>"

        text += "<br><b>Data Center related URL:</b><br>"
        text += "Data Center URL<br>"
        text += "Home Page<br>"
        text += "https://www.usap-dc.org/<br>"


        text += "<br><b>Data Contacts:</b><br>"
        text += "Data Center Contact Person<br>"
        text += "Data Center Contact<br>"
        text += "USAP-DC<br>"
        text += 'First: "Data"; Last: "Manager"<br>'
        text += "&nbsp;&nbsp;Contact Type: email<br>"
        text += "&nbsp;&nbsp;&nbsp;&nbsp;value: info@usap-dc.org<br>"

        text += "<br>Non Data Center Contact Person<br>"
        if data['persons']:
            for item in data['persons']:
                text += "%s<br>" % item['id']
                text += "&nbsp;&nbsp;role: %s<br>" % item['role'] # role
                text += "&nbsp;&nbsp;org:  %s<br>" % item['org'] # org
                text += "&nbsp;&nbsp;email: %s<br>" % item['email'] # email
                text += "&nbsp;&nbsp;country: United States<br>"


        text += "<br><b>Collection Citations:</b><br>"
        text += "Title:<br>"
        text += "&nbsp;&nbsp;%s<br>" % data['title']
        text += "Creator:<br>"
        text += "&nbsp;&nbsp;%s<br>" % '; '.join(p['id'] for p in data['persons'] if p['role'] == "Investigator and contact")
        text += "Release Date:<br>"
        text += "&nbsp;&nbsp;%s<br>" % data['date_created']
        text += "Publisher:<br>"
        text += "&nbsp;&nbsp;USAP-DC<br>"
        text += "Online Resource:<br>"
        text += "&nbsp;&nbsp;Name: USAP-DC Project Landing Page<br>"
        text += "&nbsp;&nbsp;Linkage : https://www.usap-dc.org/view/project/%s<br>" % data['proj_uid']
        text += "&nbsp;&nbsp;Description: Project Landing Page<br>"

        text += "<br><b>Metadata Language:</b><br>"
        text += "English<br>"

        text += "<br><b>Metadata Dates:</b><br>"
        text += "&nbsp;&nbsp;no need to set anything here<br>"

        text += "<br><b>File Archive (required):</b><br>"
        text += "&nbsp;&nbsp;Format: Not Provided<br>"
        text += "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;or use main file type<br>"

        text += "<br><b>File Distribution (required):</b><br>"
        text += "&nbsp;&nbsp;Format: Not Provided<br>"
        text += "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;or use main file type<br>"

        text += "<br><b>Direct Distribution Info:</b><br>"
        text += "&nbsp;&nbsp;(for cloud services - leave empty)<br>"

        return (text, None)
    except Exception as e:
        return (None, str(e)) 


# get uncompressed file size of gzipped file
def get_uncompressed_size(file):
    fileobj = open(file, 'r')
    fileobj.seek(-8, 2)
    crc32 = gzip.read32(fileobj)
    isize = gzip.read32(fileobj)  # may exceed 2GB
    fileobj.close()
    return isize


def get_file_info(ds_id, url, dataset_dir, replace):
    (conn, cur) = usap.connect_to_db()  
    dir_name = ''
    sql_out = ''
    # if replace, remove existing entries from database first
    if replace:
        sql_out += "DELETE FROM dataset_file WHERE dataset_id = '%s';\n\n" % ds_id

    if config['USAP_DOMAIN']+'dataset' in url:
        dir_name = url.replace(config['USAP_DOMAIN']+'dataset', '')
        for root, dirs, files in os.walk(dataset_dir):
            for name in files:
                mime_types = set()
                doc_types = set()
                path_name = os.path.join(root, name)
                if name.endswith('.tar.Z'):
                    try:
                        process = Popen(('zcat', path_name), stdout=PIPE)
                        output = check_output(('tar', 'tf', '-'), stdin=process.stdout)
                        files = output.split('\n')
                        for file in files:
                            if file != '':
                                mime_type, doc_type = getMimeAndDocTypes(file, path_name, cur)
                                mime_types.add(mime_type)
                                doc_types.add(doc_type)

                    except Exception as err:
                        doc_types.add('Unknown')
                        sql_out += "--ERROR: Couldn't open tar.Z file %s\n" % name

                elif '.tar' in name.lower() or name.endswith('.tgz'):
                    try:
                        with tarfile.open(path_name) as archive:
                            for member in archive:
                                if member.isreg():
                                    if '.zip' in member.name:
                                        archive.extract(member.name, path='tmp')
                                        zp = zipfile.ZipFile(os.path.join('tmp', member.name))
                                        for z in zp.filelist:
                                            if not z.filename.endswith('/'):
                                                mime_type, doc_type = getMimeAndDocTypes(z.filename, os.path.join('tmp', member.name), cur, zp)
                                                mime_types.add(mime_type)
                                                doc_types.add(doc_type)
                                        shutil.rmtree('tmp', ignore_errors=True)

                                    elif member.name.endswith('.tar.Z'):
                                        archive.extract(member.name, path='tmp')
                                        try:
                                            process = Popen(('zcat', os.path.join('tmp', member.name)), stdout=PIPE)
                                            output = check_output(('tar', 'tf', '-'), stdin=process.stdout)
                                            files = output.split('\n')
                                            for file in files:
                                                # print(file)
                                                if file != '':
                                                    mime_type, doc_type = getMimeAndDocTypes(file, os.path.join('tmp', member.name), cur)
                                                    mime_types.add(mime_type)
                                                    doc_types.add(doc_type)

                                        except Exception as err:
                                            doc_types.add('Unknown')
                                            print("Couldn't open tar.Z file %s\n" % member.name)
                                            print(err)

                                    elif '.tar' in member.name.lower() or member.name.endswith('.tgz'):
                                        archive.extract(member.name, path='tmp')
                                        if os.path.basename(member.name).startswith('._'):
                                            continue
                                        
                                        with tarfile.open(os.path.join('tmp', member.name)) as archive2:
                                            for member2 in archive2:
                                                if member2.isreg():
                                                    mime_type, doc_type = getMimeAndDocTypes(member2.name, os.path.join('tmp', member2.name), cur, None, archive2)
                                                    mime_types.add(mime_type)
                                                    doc_types.add(doc_type)
                                        shutil.rmtree('tmp', ignore_errors=True)
                                    else:
                                        mime_type, doc_type = getMimeAndDocTypes(member.name, path_name, cur, None, archive)
                                        mime_types.add(mime_type)
                                        doc_types.add(doc_type)
                    except:
                        sql_out += "--ERROR: Couldn't open tar file %s\n" % name

                elif '.zip' in name:
                    zp = zipfile.ZipFile(path_name)
                    for z in zp.filelist:
                        if not z.filename.endswith('/'):
                            if z.filename.endswith('.tar') or z.filename.endswith('.tar.gz') or z.filename.endswith('.tgz'):
                                if z.filename.startswith('__MACOSX'):
                                    continue
                                zp.extract(z.filename, 'tmp')
                                with tarfile.open(os.path.join('tmp', z.filename)) as archive2:
                                    for member2 in archive2:
                                        if member2.isreg():
                                            mime_type, doc_type = getMimeAndDocTypes(member2.name, os.path.join('tmp', member2.name), cur, None, archive2)
                                            mime_types.add(mime_type)
                                            doc_types.add(doc_type)
                                shutil.rmtree('tmp', ignore_errors=True)
                            else:
                                mime_type, doc_type = getMimeAndDocTypes(z.filename, path_name, cur, zp)
                                mime_types.add(mime_type)
                                doc_types.add(doc_type)
                
                elif name.endswith('.7z'):
                    with open(path_name) as fp:
                        archive = py7zlib.Archive7z(fp)
                        for z in archive.getnames():
                            if not z.endswith('/'):
                                mime_type, doc_type = getMimeAndDocTypes(z, path_name, cur)
                                mime_types.add(mime_type)
                                doc_types.add(doc_type)
                else:
                    mime_type, doc_type = getMimeAndDocTypes(path_name, path_name, cur)
                    mime_types.add(mime_type)
                    doc_types.add(doc_type)

                file_size = os.path.getsize(path_name)

                # if file is zipped, get  the uncompressed file size too
                if '.gz' in name or name.endswith('.Z'):
                    u_file_size = get_uncompressed_size(path_name)
                elif '.zip' in name:
                    zp = zipfile.ZipFile(path_name)
                    u_file_size = sum(zinfo.file_size for zinfo in zp.filelist)
                else:
                    u_file_size = file_size

                # calculate checksums (hashlib library won't work on large files, so need to use openssl in unix)
                process = Popen(['openssl', 'sha256', path_name], stdout=PIPE)
                (output, err) = process.communicate()
                if err:
                    text = "Error calculating SHA256 checksum.  %s" % err.decode('ascii')
                    print(text)
                    return "--%s\n " % text
                checksum = output.decode('ascii').split(")= ")[1].replace("\n", "")

                process = Popen(['openssl', 'md5', path_name], stdout=PIPE)
                (output, err) = process.communicate()
                if err:
                    text = "Error calculating MD5 checksum.  %s" % err.decode('ascii')
                    print(text)
                    return "--%s\n " % text
                checksum_md5 = output.decode('ascii').split(")= ")[1].replace("\n", "")

                # check if dataset_id already exist in table
                sql_line = "SELECT * "\
                            "FROM dataset_file "\
                            "WHERE dataset_id = %s AND dir_name = %s AND file_name = %s;"
                cur.execute(sql_line, (ds_id, dir_name, name))
                data = cur.fetchall()

                mime_str = "; ".join([m for m in mime_types if m])
                doc_str = "; ".join([d for d in doc_types if d])

                # if no data returned use insert otherwise update
                if not data or replace:
                    sql_out += "INSERT INTO dataset_file "\
                        "(dataset_id, dir_name, file_name, file_size, file_size_uncompressed, sha256_checksum, " \
                        "md5_checksum, mime_types, document_types) VALUES " \
                        "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');\n\n" % (ds_id, dir_name, name, file_size, 
                        u_file_size, checksum, checksum_md5, mime_str, doc_str)
                else:
                    sql_out += "UPDATE dataset_file SET file_size = '%s', file_size_uncompressed = '%s', sha256_checksum = '%s', " \
                        "md5_checksum = '%s', mime_types = '%s', document_types = '%s' " \
                        "WHERE dataset_id = '%s' AND dir_name = '%s' AND file_name = '%s';\n\n" % (file_size, u_file_size,
                        checksum, checksum_md5, mime_str, doc_str, ds_id, dir_name, name)
        
        return sql_out
    else:
        return None


def getMimeAndDocTypes(name, path_name, cur, zp=None, tar_archive=None):

    mime_type = mimetypes.guess_type(name)[0]
    ext = '.' + name.split('.')[-1]

    if ext in ['.old', '.gz']:
        ext = '.' + name.split('.')[-2]
    cur.execute("SELECT * FROM file_type WHERE extension = %s", (ext.lower(),))

    res = cur.fetchone()

    if res:
        doc_type = res['document_type']
    else:
        doc_type = 'Unknown'

    if 'README' in name.upper():
        doc_type = 'Readme Text File'

    if doc_type == 'Unknown':
        try:
            # determine whether text or binary
            textchars = bytearray({7, 8, 9, 10, 12, 13, 27} | set(range(0x20, 0x100)) - {0x7f})
            is_binary_string = lambda bytes: bool(bytes.translate(None, textchars))
            if zp:
                f = zp.open(name, 'r')
            elif tar_archive:
                tar_archive.extract(name, path='tmp')
                f = open(os.path.join('tmp',name), 'rb')
            else:
                f = open(path_name, 'rb')

            data_file = ''
            if ext.lower() == '.dat':
                data_file = 'Data '
            if is_binary_string(f.read(1024)):
                doc_type = '%sBinary File' % data_file
            else:
                doc_type = '%sText File' % data_file
            if tar_archive:
                shutil.rmtree('tmp')

        except Exception as e:
            # for dataset 601323
            if str(e) == 'Bad magic number for file header':
                doc_type = None
            print(e)

    return mime_type, doc_type