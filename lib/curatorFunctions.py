import urllib2
import json
import xml.dom.minidom as minidom
from lib.ezid import formatAnvlRequest, issueRequest, encode, MyHTTPErrorProcessor
import os
import psycopg2
import psycopg2.extras
import sys
import requests
from flask import session, url_for
from subprocess import Popen, PIPE
from json2sql import makeBoundsGeom

UPLOAD_FOLDER = "upload"
DATASET_FOLDER = "dataset"
SUBMITTED_FOLDER = "submitted"
DCXML_FOLDER = "watch/dcxml"
ISOXML_FOLDER = "watch/isoxml"
DOCS_FOLDER = "doc"
DOI_REF_FILE = "inc/doi_ref"
CURATORS_LIST = "inc/curators.txt"
EZID_FILE = "inc/ezid.json"
DATACITE_TO_ISO_XSLT = "static/DataciteToISO19139v3.2.xslt"
ISOXML_SCRIPT = "bin/makeISOXMLFile.py"
PYTHON = "/opt/rh/python27/root/usr/bin/python"
LD_LIBRARY_PATH = "/opt/rh/python27/root/usr/lib64"

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


def submitToEZID(uid):

    datacite_file = getDCXMLFileName(uid)
    # Read in EZID connection details
    with open(EZID_FILE) as ezid_file:
        ezid_details = json.load(ezid_file)

    # Submit the datacite file to EZID to get the DOI
    try:
        opener = urllib2.build_opener(MyHTTPErrorProcessor())
        h = urllib2.HTTPBasicAuthHandler()
        h.add_password("EZID", ezid_details['SERVER'], ezid_details['USER'], ezid_details['PASSWORD'])
        opener.add_handler(h)

        landing_page = url_for("landing_page", dataset_id=uid, _external=True)
        data = formatAnvlRequest(["datacite", "@%s" % datacite_file, "_target", landing_page])

        # if using mint to generate random DOI id:
        # response = issueRequest(ezid_details['SERVER'], opener, "shoulder/%s" % encode(ezid_details['SHOULDER']), "POST", data)
        # if using the create option, rather than mint:
        id = ezid_details['SHOULDER'] + uid

        response = issueRequest(ezid_details['SERVER'], opener, "id/%s" % encode(id), "PUT", data)
        # print("RESPONSE: %s" % response)
        if response == "Error: bad request - unrecognized DOI shoulder\n":
            return("Error: unrecognized DOI shoulder")

        elif "Missing child element" in response:
            return("Error generating DOI: missing required DataCite fields.<br/>" +
                   "Make sure the following are all populated:<br/>Publisher<br/>Year<br/>Resource Type<br/>Title<br/>Author")

        elif "doi" not in response:
            return("Error generating DOI. Returned response from EZID: %s" % response)

        else:
            doi = response.split(" ")[1]
            doi = doi.replace("doi:", "")

            return("Successfully registered dataset at EZID, doi: %s" % doi)

    except urllib2.HTTPError as e:
        return("Error: Failed to authenticate with EZID server")

    except urllib2.URLError as e:
        return("Error connecting to EZID server")

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


def isRegisteredWithEZID(uid):
    with open(EZID_FILE) as ezid_file:
        ezid_details = json.load(ezid_file)
    id = ezid_details['SHOULDER'] + uid
    ezid_url = ezid_details['SERVER'] + '/id/' + id
    r = requests.get(ezid_url)
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
        # os.remove(isoxml_file)


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
