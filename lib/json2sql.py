# -*- coding: utf-8 -*-
"""
Created on Sat Jul  8 14:23:05 2017

@author: fnitsche

Modified for USAP-DC curator page by Neville Shane March 1 2018
"""

import os
import json
import psycopg2
from flask import url_for
import usap
from lib.curatorFunctions import makeBoundsGeom, updateSpatialMap

config = json.loads(open('config.json', 'r').read())
dc_config = json.loads(open('inc/datacite.json', 'r').read())


def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER'],
                            password=info['PASSWORD'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


def parse_json(data):

    # --- checking for required fields, make sure they all have data
    fields = ["abstract", "authors", "awards", "title", "timestamp",
              "geo_e", "geo_w", "geo_n", "geo_s", "start", "stop",
              "publications", "orcid", "email"]
    # for field in fields:
    #    print(data[field])

    # --- fix some json fields
    # TODO Handle multiple authors
    data["author"] = data["authors"][0]
    (first, last) = (data["author"]["first_name"], data["author"]["last_name"])
    data["author"] = "{}, {}".format(last, first)
    print("corrected author: ", data["author"])
    if data.get('submitter_name') and data["submitter_name"] != "":
        (first, last) = data["submitter_name"].split(' ', 1)
        data["submitter_first"] = first
        data["submitter_last"] = last
        data["submitter_name"] = "{}, {}".format(last, first)
        print("corrected submitter: ", data["submitter_name"])
    else:
        data["submitter_name"] = data["author"]

    # --- fix award field
    for i in range(len(data["awards"])):
        if data["awards"][i] not in ["None", "Not In This List"] and len(data["awards"][i].split(" ", 1)) > 1:
            (data["awards"][i], dummy) = data["awards"][i].split(" ", 1)  # throw away the rest of the award string

    # --- should add something here to check lat lon fields

    for field in fields:
        if field not in data:
            print(field)
            data[field] = ''
        else:
            print(field, " - ok")

    # print('Error reading file', in_file)
    # print('check json file for wrong EOL')

    return data


def make_sql(data, id):
    # --- prepare some parameter
    if data.get('release_date'):
        release_date = data['release_date']
    else:
        release_date = data["timestamp"][0:10]
    date_created = data["timestamp"][0:10]

    url = config['USAP_DOMAIN'] + '/dataset/usap-dc/' + id + '/' + data["timestamp"] + '/'
    
    curator = "Nitsche"

    sql_out = ""
    sql_out += 'START TRANSACTION;\n\n'
    sql_out += '--NOTE: include NSF PI(s), submitter, and author(s); email+orcid optional\n'

    conn, cur = connect_to_db()

    person_ids = []
    for author in data["authors"]:
        first_name = author.get("first_name")
        last_name = author.get("last_name")
        person_id = "%s, %s" % (last_name, first_name)
        person_ids.append(person_id)

        query = "SELECT  * FROM person WHERE id = '%s'" % person_id
        cur.execute(query)
        res = cur.fetchone()
  
        if not res and person_id != "":
            if author == data["authors"][0]:
                line = "INSERT INTO person(id,first_name, last_name, email) VALUES ('{}','{}','{}','{}');\n".format(person_id, first_name, last_name, data["email"])
            else:
                line = "INSERT INTO person(id,first_name, last_name) VALUES ('{}','{}','{}');\n".format(person_id, first_name, last_name)

            sql_out += line

            if person_id == data.get('submitter_name') and data.get('submitter_orcid'):
                line = "UPDATE person SET id_orcid = '{}' WHERE id = '{}';\n".format(data['submitter_orcid'], person_id)
                sql_out += line
        else:
            if person_id == data.get('submitter_name') and data.get('submitter_orcid') and data['submitter_orcid'] != res['id_orcid'] and data['submitter_orcid'] != '':
                line = "UPDATE person SET id_orcid = '{}'  WHERE id = '{}';\n".format(data['submitter_orcid'], person_id)
                sql_out += line  
            if person_id == data.get('submitter_name') and data.get('submitter_email') and data['submitter_email'] != res['email'] and data['submitter_email'] != '':
                line = "UPDATE person SET email = '{}'  WHERE id = '{}';\n".format(data['submitter_email'], person_id)
                sql_out += line  

    if data["submitter_name"] not in person_ids and data["submitter_name"] != '':
        query = "SELECT * FROM person WHERE id = '%s'" % data["submitter_name"]
        cur.execute(query)
        res = cur.fetchone()
        if not res:
            line = "INSERT INTO person(id, first_name, last_name, email,id_orcid) VALUES ('{}','{}','{}','{}','{}');\n".format(data["submitter_name"], data["submitter_first"], data["submitter_last"], data.get("submitter_email", ''), data.get("submitter_orcid", ''))
            sql_out += line
        else:
            if data.get('submitter_orcid') and data['submitter_orcid'] != res['id_orcid'] and data['submitter_orcid'] != '':
                line = "UPDATE person SET id_orcid = '{}'  WHERE id = '{}';\n".format(data['submitter_orcid'], data['submitter_name'])
                sql_out += line 
            if data.get('submitter_email') and data['submitter_email'] != res['email'] and data['submitter_email'] != '':
                line = "UPDATE person SET email = '{}'  WHERE id = '{}';\n".format(data['submitter_email'], data['submitter_name'])
                sql_out += line   
    
    # if this is a replacement dataset, increment the version number from the previous dataset
    if data.get('related_dataset'):
        query = "SELECT version::integer FROM dataset WHERE id = '%s';" % data['related_dataset']
        cur.execute(query)
        version = cur.fetchone()['version'] + 1
    else:
        version = 1

    sql_out += '\n--NOTE: submitter_id = JSON "submitter_name"\n'
    sql_out += '--NOTE: creator = JSON "author"\n'
    sql_out += '--NOTE: url suffix = JSON "timestamp"\n'
    sql_line = """INSERT INTO dataset (id,title,submitter_id,creator,release_date,abstract,version,url,superset,language_id,
    status_id,url_extra,review_status,date_created,date_modified)
    VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','In Work','{}','{}');\n""" \
            .format(id,
                    data["title"], 
                    data["submitter_name"], 
                    '; '.join(person_ids), 
                    release_date, 
                    data["abstract"], 
                    version, 
                    url, 
                    'usap-dc', 
                    'English',
                    'Complete', 
                    '/doc/' + id + '/README_' + id + '.txt',
                    date_created,
                    date_created)

    sql_out += sql_line

    sql_out += '\n--NOTE: add to project_dataset table\n'
    sql_out += "INSERT INTO project_dataset (dataset_id, repository, title, url, status) VALUES ('%s', '%s', '%s', '%s', 'exists');\n" % \
        (id, 'USAP-DC', data.get('title'), url_for('landing_page', dataset_id=id, _external=True))

    sql_out += '\n--NOTE: same set of persons from above (check name and spelling)\n'
    for person_id in person_ids:
            line = "INSERT INTO dataset_person_map(dataset_id,person_id) VALUES ('%s','%s');\n" % (id, person_id)
            sql_out += line

    if data["submitter_name"] not in person_ids and data["submitter_name"] != '':
        line = "INSERT INTO dataset_person_map(dataset_id,person_id) VALUES ('%s','%s');\n" % (id, data["submitter_name"])
        sql_out += line
  
    sql_out += '\n--NOTE: AWARDS functions:\n'
    if len(data.get('awards') and data['awards']) == 0:
        sql_out += "--NOTE: NO AWARD SUBMITTED\n"
    for award in data['awards']:
        if award == 'None':
            sql_out += "--NOTE: NO AWARD SUBMITTED\n"
        else:
            if 'Not_In_This_List' in award:
                sql_out += "--NOTE: AWARD NOT IN PROVIDED LIST\n"
                (dummy, award) = award.split(':', 1)
            # check if this award is already in the award table
            query = "SELECT COUNT(*) FROM  award WHERE award = '%s'" % award
            cur.execute(query)
            res = cur.fetchone()
            if res['count'] == 0:
                # Add award to award table
                sql_out += "--NOTE: Adding award %s to award table. Curator should update with any known fields.\n" % award
                sql_out += "INSERT INTO award(award, dir, div, title, name) VALUES ('%s', 'GEO', 'OPP', 'TBD', 'TBD');\n" % award
                sql_out += "--UPDATE award SET iscr='f', isipy='f', copi='', start='', expiry='', sum='', email='', orgcity='', orgzip='', dmp_link='' WHERE award='%s';\n" % award
                sql_out += "INSERT INTO dataset_award_map(dataset_id,award_id) VALUES ('%s','%s');\n" % (id, award)

            else:
                query = "SELECT * FROM dif_award_map WHERE award = '%s';" % award
                cur.execute(query)
                res = cur.fetchone()               
                if res:
                    sql_out += "\n--NOTE: Linking dataset to DIF via award\n"
                    sql_out += "INSERT INTO dataset_dif_map(dataset_id,dif_id) VALUES ('%s','%s');\n" % (id, res['dif_id'])

                sql_out += '\n--NOTE: check the award #\n'
                line = "INSERT INTO dataset_award_map(dataset_id,award_id) VALUES ('%s','%s');\n" % (id, award)
                sql_out += line

                # look up award to see if already mapped to a program
                query = "SELECT program_id FROM award_program_map WHERE award_id = '%s';" % award
                cur.execute(query)
                res = cur.fetchone()
          
                if res is None:
                    sql_out += "\n--NOTE: Need to map award to a program."
                    sql_out += "\n--NOTE: look up at https://www.nsf.gov/awardsearch/showAward?AWD_ID={}\n".format(award)
                    sql_out += "--INSERT INTO award_program_map(award_id,program_id) VALUES ('{}','Antarctic Earth Sciences');\n".format(award)
                    sql_out += "--INSERT INTO award_program_map(award_id,program_id) VALUES ('{}','Antarctic Glaciology');\n".format(award)
                    sql_out += "--INSERT INTO award_program_map(award_id,program_id) VALUES ('{}','Antarctic Organisms and Ecosystems');\n".format(award)
                    sql_out += "--INSERT INTO award_program_map(award_id,program_id) VALUES ('{}','Antarctic Integrated System Science');\n".format(award)
                    sql_out += "--INSERT INTO award_program_map(award_id,program_id) VALUES ('{}','Antarctic Astrophysics and Geospace Sciences');\n".format(award)
                    sql_out += "--INSERT INTO award_program_map(award_id,program_id) VALUES ('{}','Antarctic Ocean and Atmospheric Sciences');\n\n".format(award)

                    sql_out += "INSERT INTO dataset_program_map(dataset_id,program_id) VALUES ('{}','Antarctic Ocean and Atmospheric Sciences');\n\n".format(id)
                else:
                    sql_out += "INSERT INTO dataset_program_map(dataset_id,program_id) VALUES ('{}','{}');\n\n".format(id, res['program_id'])
                    # if res['program_id'] == 'Antarctic Glaciology':
                    #     curator = 'Bauer'

                # look up award to see if already mapped to a project
                query = "SELECT proj_uid FROM project_award_map WHERE award_id = '%s';" % award
                cur.execute(query)
                res = cur.fetchall()
                if len(res) > 0:
                    sql_out += "--NOTE: Linking dataset to project via award.  Comment out any lines you don't wish to add to DB\n"
                    for project in res:
                        sql_out += "INSERT INTO project_dataset_map (proj_uid, dataset_id) VALUES ('%s', '%s');\n" % (project.get('proj_uid'), id)    
     
    # sql_out += "\n--NOTE: reviewer is Bauer for Glaciology-funded dataset, else Nitsche\n"
    sql_out += "\nUPDATE dataset SET review_person='{}' WHERE id='{}';\n".format(curator, id)

    sql_out += "\n--NOTE: spatial and temp map, check coordinates\n"

    if data["start"] != "" or data["stop"] != "":
        line = "INSERT INTO dataset_temporal_map(dataset_id,start_date,stop_date) VALUES ('%s','%s','%s');\n\n" % (id, data["start"], data["stop"])
        sql_out += line

    if (data["geo_w"] != '' and data["geo_e"] != '' and data["geo_s"] != '' and data["geo_n"] != '' and data["cross_dateline"] != ''):
        west = float(data["geo_w"])
        east = float(data["geo_e"])
        south = float(data["geo_s"])
        north = float(data["geo_n"])
        mid_point_lat = (south - north) / 2 + north
        mid_point_long = (east - west) / 2 + west

        geometry = "ST_GeomFromText('POINT(%s %s)', 4326)" % (mid_point_long, mid_point_lat)
        bounds_geometry = "ST_GeomFromText('%s', 4326)" % makeBoundsGeom(north, south, east, west, data["cross_dateline"])

        sql_out += "\n--NOTE: need to update geometry; need to add mid x and y\n"

        line = "INSERT INTO dataset_spatial_map(dataset_id,west,east,south,north,cross_dateline,geometry,bounds_geometry) VALUES ('%s','%s','%s','%s','%s','%s',%s,%s);\n" % \
            (id, west, east, south, north, data["cross_dateline"], geometry, bounds_geometry)
        sql_out += line

    # Add Initiative
    if data.get('project') and data['project'] != '' and data['project'] != 'None':
        sql_out += "\n--NOTE: adding initiative\n"
        sql_out += "INSERT INTO dataset_initiative_map(dataset_id,initiative_id) VALUES ('{}','{}');\n\n".format(id, data['project'])
    else:
        sql_out += "\n--NOTE: optional; every dataset does NOT belong to an initiative\n"
        sql_out += "--INSERT INTO initiative(id) VALUES ('WAIS Divide Ice Core');\n"
        line = "--INSERT INTO dataset_initiative_map(dataset_id,initiative_id) VALUES ('{}','{}');\n\n".format(id, 'WAIS Divide Ice Core')
        sql_out += line

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
            query = "SELECT * FROM reference WHERE doi='%s' AND ref_text = '%s';" % (pub.get('doi'), pub.get('text'))
            cur.execute(query)
            res = cur.fetchall()
            if len(res) == 0:
                # sql_out += "--NOTE: adding %s to reference table\n" % pub['name']
                old_uid += 1
                ref_uid = 'ref_%0*d' % (7, old_uid)
                sql_out += "INSERT INTO reference (ref_uid, ref_text, doi) VALUES ('%s', '%s', '%s');\n" % \
                    (ref_uid, pub.get('text'), pub.get('doi'))
            else:
                ref_uid = res[0]['ref_uid']
            # sql_out += "--NOTE: adding reference %s to dataset_reference_map\n" % ref_uid
            sql_out += "INSERT INTO dataset_reference_map (dataset_id, ref_uid) VALUES ('%s', '%s');\n" % \
                (id, ref_uid)

        sql_out += "\n"

    # Add locations
    if data.get('locations') and len(data['locations']) > 0:
        sql_out += "--NOTE: adding locations\n"

        next_id = False
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
                sql_out += "INSERT INTO dataset_keyword_map(dataset_id,  keyword_id) VALUES ('%s','uk-%s'); -- %s\n" % (id, next_id, loc)

            else:
                sql_out += "INSERT INTO dataset_keyword_map(dataset_id,  keyword_id) VALUES ('%s','%s'); -- %s\n" % (id, res['keyword_id'], loc)
               
        sql_out += "\n"

    sql_out += "--NOTE: add keywords\n"
    sql_out += "INSERT INTO dataset_keyword_map(dataset_id,  keyword_id) VALUES ('{}','ik-0001'); -- Antarctica\n".format(id)
    sql_out += "INSERT INTO dataset_keyword_map(dataset_id,  keyword_id) VALUES ('{}','ik-0052'); -- Cryosphere\n".format(id)
    sql_out += "--INSERT INTO dataset_keyword_map(dataset_id,  keyword_id) VALUES ('{}','ik-0009'); -- Glaciers and Ice sheets\n".format(id)
    sql_out += "--INSERT INTO dataset_keyword_map(dataset_id,  keyword_id) VALUES ('{}','ik-0067'); -- Snow Ice\n".format(id)
    sql_out += "--INSERT INTO dataset_keyword_map(dataset_id,  keyword_id) VALUES ('{}','ik-0031'); -- Glaciology\n".format(id)
    sql_out += "--INSERT INTO dataset_keyword_map(dataset_id,  keyword_id) VALUES ('{}','ik-0032'); -- Ice Core Records\n".format(id)

    # user keywords
    if data["user_keywords"] != "":
        last_id = None
        sql_out += "--NOTE: add user keywords\n"
        for keyword in data["user_keywords"].split(','):
            keyword = keyword.strip()
            # first check if the keyword is already in the database - check keyword_usap and keyword_ieda tables
            query = "SELECT keyword_id FROM keyword_ieda WHERE UPPER(keyword_label) = UPPER('%s') UNION SELECT keyword_id FROM keyword_usap WHERE UPPER(keyword_label) = UPPER('%s')" % (keyword, keyword)
            cur.execute(query)
            res = cur.fetchone()
            if res is not None:
                sql_out += "INSERT INTO dataset_keyword_map(dataset_id,  keyword_id) VALUES ('{}','{}'); -- {}\n".format(id, res['keyword_id'], keyword)
            else:
                #if not found, add to keyword_usap
                # first work out the last keyword_id used
                query = "SELECT keyword_id FROM keyword_usap ORDER BY keyword_id DESC"
                cur.execute(query)
                res = cur.fetchone()
                if not last_id:
                    last_id = res['keyword_id'].replace('uk-', '')
                next_id = int(last_id) + 1
                sql_out += "--INSERT INTO keyword_usap (keyword_id, keyword_label, keyword_type_id, source) VALUES ('uk-%s', '%s', 'REPLACE_ME', 'user');\n" % \
                    (next_id, keyword)
                sql_out += "--INSERT INTO dataset_keyword_map(dataset_id,  keyword_id) VALUES ('{}','uk-{}');\n".format(id, next_id)
                last_id = next_id

    # if this dataset is replacing an old dataset, need to deprecate the old one
    if data.get('related_dataset'):
        sql_out += "\n--NOTE: this dataset will replace dataset %s\n" % data['related_dataset']
        sql_out += "UPDATE dataset SET replaces = '%s' WHERE id = '%s';\n" % (data['related_dataset'], id)
        sql_out += "UPDATE dataset SET replaced_by = '%s' WHERE id = '%s';\n" % (id, data['related_dataset'])
        sql_out += "UPDATE project_dataset SET status = 'deprecated' WHERE dataset_id = '%s';\n" % data['related_dataset']

    sql_out += '\nCOMMIT;\n'

    return sql_out


def editDatasetJson2sql(data, uid):
    conn, cur = connect_to_db()

    # get existing values from the database and compare them with the JSON file
    orig = usap.dataset_db2form(uid)

    # construct pi_id for first author
    first_name = data['authors'][0].get("first_name")
    last_name = data['authors'][0].get("last_name")
    pi_id = "%s, %s" % (last_name, first_name)

    # submitter
    if data["submitter_name"] != "":
        (first, last) = data["submitter_name"].split(' ', 1)
        data["submitter_name"] = "{}, {}".format(last, first)

    # compare original with edited json
    updates = set()
    for k in orig.keys():
        if orig[k] != data.get(k):
            print(k)
            print("orig:", orig.get(k))
            print("new:", data.get(k))
            if k in ['geo_e', 'geo_n', 'geo_s', 'geo_w', 'cross_dateline']:
                updates.add('spatial_extents')
            else:
                updates.add(k)    

    # check for orcid update
    query = "SELECT id_orcid FROM person WHERE id = '%s'" % data['submitter_name']
    cur.execute(query)
    res = cur.fetchone()
    if res and res['id_orcid'] != data.get('submitter_orcid'):
        updates.add('orcid')

    # --- fix award fields (throw out PI name)
    data["awards_num"] = data['awards'][:]
    for i in range(len(data["awards"])):
        if data["awards"][i] not in ["None", "Not In This List"] and len(data["awards"][i].split(" ", 1)) > 1:
            data["awards_num"][i] = data["awards"][i].split(" ")[0]  # throw away the rest of the award string

    # update database with edited values
    sql_out = ""
    sql_out += "START TRANSACTION;\n"
    for k in updates:
        if k == 'abstract':
            sql_out += "\n--NOTE: UPDATING ABSTRACT\n"
            sql_out += "UPDATE dataset SET abstract = '%s' WHERE id = '%s';\n" % (data['abstract'], uid)
        
        if k == 'authors':
            sql_out += "\n--NOTE: UPDATING AUTHORS\n"

            # remove existing co-pis from project_person_map
            sql_out += "\n--NOTE: First remove all existing authors from dataset_person_map\n"
            sql_out += "DELETE FROM dataset_person_map WHERE dataset_id = '%s' and person_id != '%s';\n" % (uid, data['submitter_name'])
            # make sure authors are in person table
            person_ids = []
            for author in data["authors"]:
                first_name = author.get("first_name")
                last_name = author.get("last_name")
                person_id = "%s, %s" % (last_name, first_name)
                person_ids.append(person_id)

                # if already updating the submitter, don't need to do it here
                if 'submitter_name' in updates and person_id == data['submitter_name']:
                    continue

                query = "SELECT  * FROM person WHERE id = '%s'" % person_id
                cur.execute(query)
                res = cur.fetchone()
          
                if not res and person_id != "":
                    if author == data["authors"][0]:
                        line = "INSERT INTO person(id,first_name, last_name, email) VALUES ('{}','{}','{}','{}');\n".format(person_id, first_name, last_name, data["email"])
                    else:
                        line = "INSERT INTO person(id,first_name, last_name) VALUES ('{}','{}','{}');\n".format(person_id, first_name, last_name)

                    sql_out += line

                    if person_id == data.get('submitter_name') and data.get('submitter_orcid'):
                        line = "UPDATE person SET id_orcid = '{}' WHERE id = '{}';\n".format(data['submitter_orcid'], person_id)
                        sql_out += line
                else:
                    if person_id == data.get('submitter_name') and data.get('submitter_orcid') and data['submitter_orcid'] != res['id_orcid'] and data['submitter_orcid'] != '':
                        line = "UPDATE person SET id_orcid = '{}'  WHERE id = '{}';\n".format(data['submitter_orcid'], person_id)
                        sql_out += line  
                    if person_id == data.get('submitter_name') and data.get('submitter_email') and data['submitter_email'] != res['email'] and data['submitter_email'] != '':
                        line = "UPDATE person SET email = '{}'  WHERE id = '{}';\n".format(data['submitter_email'], person_id)
                        sql_out += line  

            # add people back in to project_person_map
            for person_id in person_ids:
                if person_id != data['submitter_name']:
                    sql_out += "\n--NOTE: adding %s to dataset_person_map\n" % person_id
                    sql_out += "INSERT INTO dataset_person_map(dataset_id,person_id) VALUES ('%s','%s');\n" % (uid, person_id)

            # update creator field in dataset table
            sql_out += "\n--NOTE: updating creator field in dataset table\n"
            sql_out += "UPDATE dataset SET creator = '%s' WHERE id = '%s';\n" % ('; '.join(person_ids), uid)

        if k == 'awards':
            sql_out += "\n--NOTE: UPDATING AWARDS\n"

            # remove existing awards from dataset_award_map
            sql_out += "\n--NOTE: First remove existing awards from dataset_award_map\n"
            sql_out += "DELETE FROM dataset_award_map WHERE dataset_id = '%s';\n" % (uid)
            sql_out += "\n--NOTE: Then remove dataset from project_dataset_map\n"
            sql_out += "DELETE FROM project_dataset_map WHERE dataset_id = '%s';\n" % (uid)

            for award in data['awards_num']:
                if award == 'None':
                    sql_out += "\n--NOTE: NO AWARD SUBMITTED\n"
                else:
                    if 'Not_In_This_List' in award:
                        sql_out += "\n--NOTE: Award not in provided list\n"
                        (dummy, award) = award.split(':', 1)
                    # check if this award is already in the award table
                    query = "SELECT COUNT(*) FROM  award WHERE award = '%s'" % award
                    cur.execute(query)
                    res = cur.fetchone()
                    if res['count'] == 0:
                        # Add award to award table
                        sql_out += "\n--NOTE: Adding award %s to award table. Curator should update with any know fields.\n" % award
                        sql_out += "INSERT INTO award(award, dir, div, title, name) VALUES ('%s', 'GEO', 'OPP', 'TBD', 'TBD');\n" % award
                        sql_out += "\n--UPDATE award SET iscr='f', isipy='f', copi='', start='', expiry='', sum='', email='', orgcity='', orgzip='', dmp_link='' WHERE award='%s';\n" % award
                   
                    sql_out += "\n--NOTE: add award %s to dataset_award_map\n" % award
                    sql_out += "INSERT INTO dataset_award_map(dataset_id,award_id) VALUES ('%s','%s');\n" % (uid, award)

                    # look up award to see if already mapped to a program
                    query = "SELECT program_id FROM award_program_map WHERE award_id = '%s';" % award
                    cur.execute(query)
                    res = cur.fetchone()
              
                    # if res is None:
                    #     sql_out += "\n--NOTE: Need to map award to a program."
                    #     sql_out += "\n--NOTE: look up at https://www.nsf.gov/awardsearch/showAward?AWD_ID={}\n".format(award)
                    #     sql_out += "--INSERT INTO award_program_map(award_id,program_id) VALUES ('{}','Antarctic Earth Sciences');\n".format(award)
                    #     sql_out += "--INSERT INTO award_program_map(award_id,program_id) VALUES ('{}','Antarctic Glaciology');\n".format(award)
                    #     sql_out += "--INSERT INTO award_program_map(award_id,program_id) VALUES ('{}','Antarctic Organisms and Ecosystems');\n".format(award)
                    #     sql_out += "--INSERT INTO award_program_map(award_id,program_id) VALUES ('{}','Antarctic Integrated System Science');\n".format(award)
                    #     sql_out += "--INSERT INTO award_program_map(award_id,program_id) VALUES ('{}','Antarctic Astrophysics and Geospace Sciences');\n".format(award)
                    #     sql_out += "--INSERT INTO award_program_map(award_id,program_id) VALUES ('{}','Antarctic Ocean and Atmospheric Sciences');\n\n".format(award)

                    #     sql_out += "INSERT INTO dataset_program_map(dataset_id,program_id) VALUES ('{}','Antarctic Ocean and Atmospheric Sciences');\n\n".format(id)
                    # else:
                    #     sql_out += "INSERT INTO dataset_program_map(dataset_id,program_id) VALUES ('{}','{}');\n\n".format(id, res['program_id'])
                    #     if res['program_id'] == 'Antarctic Glaciology':
                    #         curator = 'Bauer'

                    # look up award to see if already mapped to a project
                    query = "SELECT proj_uid FROM project_award_map WHERE award_id = '%s';" % award
                    cur.execute(query)
                    res = cur.fetchall()
                    if len(res) > 0:
                        sql_out += "--NOTE: Linking dataset to project via award.  Comment out any lines you don't wish to add to DB\n"
                        for project in res:
                            sql_out += "INSERT INTO project_dataset_map (proj_uid, dataset_id) VALUES ('%s', '%s');\n" % (project.get('proj_uid'), uid)   

        if k == 'content':
            sql_out += "\n--NOTE: UPDATING DATA CONTENT DESCRIPTION IN README FILE\n"

        if k == 'data_processing':
            sql_out += "\n--NOTE: UPDATING DATA PROCESSING DESCRIPTION IN README FILE\n"

        if k == 'devices':
            sql_out += "\n--NOTE: UPDATING INSTRUMENTS AND DEVICES DESCRIPTION IN README FILE\n"

        if k == 'email':
            # first check if first author is already in person DB table - if not, email will get added when authors are updated elsewhere in the code
            query = "SELECT COUNT(*) FROM person WHERE id = '%s'" % pi_id
            cur.execute(query)
            res = cur.fetchone()
            if res['count'] > 0:
                sql_out += "\n--NOTE: UPDATING EMAIL ADDRESS\n"
                sql_out += "UPDATE person SET email = '%s' WHERE id='%s';\n" % (data['email'], pi_id)
        
        if k == 'feature_name':
            # feature name not currently stored in DB or Readme file, so this will do nothing.
            # Update will just be stored in the new json file.
            pass

        if k == 'issues':
            sql_out += "\n--NOTE: UPDATING KNOWN ISSUES/LIMITATIONS IN README FILE\n"

        if k == 'locations':
            sql_out += "\n--NOTE: UPDATING LOCATIONS\n"

            # remove existing locations from dataset_keyword_map
            sql_out += "\n--NOTE: First remove all existing locations from dataset_keyword_map\n"
            sql_out += """DELETE FROM dataset_keyword_map dkm WHERE dataset_id = '%s' AND keyword_id IN (SELECT keyword_id FROM vw_location);\n""" % uid
            
            # Add locations
            if data.get('locations') is not None and len(data['locations']) > 0:
                sql_out += "\n--NOTE: adding locations\n"
            next_id = False
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
                    sql_out += "INSERT INTO dataset_keyword_map(dataset_id,  keyword_id) VALUES ('%s','uk-%s'); -- %s\n" % (uid, next_id, loc)

                else:
                    sql_out += "INSERT INTO dataset_keyword_map(dataset_id,  keyword_id) VALUES ('%s','%s'); -- %s\n" % (uid, res['keyword_id'], loc)
                   
            sql_out += "\n"

        if k == 'orcid':
            sql_out += "\n--NOTE: UPDATING ORCID FOR SUBMITTER\n"
            sql_out += "UPDATE person SET id_orcid = '%s' WHERE id='%s';\n" % (data['submitter_orcid'], data['submitter_name'])

        if k == 'procedures':
            sql_out += "\n--NOTE: UPDATING ACQUISITION PROCEDURES DESCRIPTION IN README FILE\n"

        if k == 'project': 
            if data['project'] == 'None' and orig['project'] is None:
                continue
            sql_out += "\n--NOTE: UPDATING INITIATIVE\n"

            # remove existing initiative from project_award_map
            sql_out += "\n--NOTE: First remove existing initiatives from dataset_initiative_map\n"
            sql_out += "DELETE FROM dataset_initiative_map WHERE dataset_id = '%s';\n" % (uid)

            if data.get('project') is not None and data['project'] != "":
                sql_out += "\n--NOTE: adding initiative to dataset_initiative_map\n"
                sql_out += "INSERT INTO dataset_initiative_map (dataset_id, initiative_id) VALUES ('%s', '%s');\n" % \
                    (uid, data['project'])

        if k == 'publications':
            sql_out += "\n--NOTE: UPDATING PUBLICATIONS\n"

            # remove existing publications from dataset_reference_map
            sql_out += "\n--NOTE: First remove all existing publications from dataset_reference_map\n"
            sql_out += "DELETE FROM dataset_reference_map WHERE dataset_id = '%s';\n" % uid
            
            # Add references
            if data.get('publications') is not None and len(data['publications']) > 0:
                sql_out += "\n--NOTE: adding references\n"

                #first find the highest ref_uid already in the table
                query = "SELECT MAX(ref_uid) FROM reference;"
                cur.execute(query)
                res = cur.fetchall()
                old_uid = int(res[0]['max'].replace('ref_', ''))

                for pub in data['publications']:
                    # see if they are already in the references table
                    query = "SELECT * FROM reference WHERE doi='%s' AND ref_text = '%s';" % (pub.get('doi'), pub.get('text'))
                    cur.execute(query)
                    res = cur.fetchall()
                    if len(res) == 0:
                        # sql_out += "--NOTE: adding %s to reference table\n" % pub['name']
                        old_uid += 1
                        ref_uid = 'ref_%0*d' % (7, old_uid)
                        sql_out += "INSERT INTO reference (ref_uid, ref_text, doi) VALUES ('%s', '%s', '%s');\n" % \
                            (ref_uid, pub.get('text'), pub.get('doi'))
                    else:
                        ref_uid = res[0]['ref_uid']
                    # sql_out += "--NOTE: adding reference %s to dataset_reference_map\n" % ref_uid
                    sql_out += "INSERT INTO dataset_reference_map (dataset_id, ref_uid) VALUES ('%s', '%s');\n" % \
                        (uid, ref_uid)

                sql_out += "\n"

        if k == 'related_fields':
            # related fields not currently stored in DB or Readme file, so this will do nothing.
            # Update will just be stored in the new json file.
            pass
        
        if k == 'release_date':      
            sql_out += "\n--NOTE: UPDATING RELEASE DATE\n"
            sql_out += "UPDATE dataset SET release_date = '%s' WHERE id = '%s';\n" % (data['release_date'], uid)

        if k == 'spatial_extents':
            sql_out += "\n--NOTE: UPDATING DATASET_SPATIAL_MAP\n"
            sql_out += updateSpatialMap(uid, data, False)

        if k == 'start':
            sql_out += "\n--NOTE: UPDATING START DATE\n"
            sql_out += "UPDATE dataset_temporal_map SET start_date = '%s' WHERE dataset_id = '%s';\n" % (data['start'], uid)

        if k == 'stop':
            sql_out += "\n--NOTE: UPDATING STOP DATE\n"
            sql_out += "UPDATE dataset_temporal_map SET stop_date = '%s' WHERE dataset_id = '%s';\n" % (data['stop'], uid)
        
        if k == 'submitter_name':
            if data["submitter_name"] != '':
                sql_out += "\n--NOTE: UPDATING SUBMITTER\n"

                query = "SELECT COUNT(*) FROM person WHERE id = '%s'" % data["submitter_name"]
                cur.execute(query)
                res = cur.fetchone()
                if res['count'] == 0:
                    first_name, last_name = data["submitter_name"].split(', ', 1)
                    line = "INSERT INTO person(id,first_name,last_name,email,id_orcid) VALUES ('{}','{}','{}','{}','{}');\n".format(data["submitter_name"], first_name, last_name, data.get("submitter_email", ''), data.get("submitter_orcid", ''))
                    sql_out += line
                query = "SELECT COUNT(*) FROM dataset_person_map WHERE dataset_id = '%s' AND person_id = '%s'" % (uid, data["submitter_name"])
                cur.execute(query)
                res = cur.fetchone()
                if res['count'] == 0:
                    line = "INSERT INTO dataset_person_map (dataset_id, person_id) VALUES ('%s','%s');\n" % (uid, data["submitter_name"])
                    sql_out += line

                sql_out += "UPDATE dataset SET submitter_id = '%s' WHERE id= '%s';\n" % (data['submitter_name'], uid)

        if k == 'title':      
            sql_out += "\n--NOTE: UPDATING TITLE\n"
            sql_out += "UPDATE dataset SET title = '%s' WHERE id = '%s';\n" % (data['title'], uid)

        if k == 'user_keywords': 
            sql_out += "\n--NOTE: UPDATING USER KEYWORDS\n"

            # remove existing locations from project_features
            sql_out += "\n--NOTE: First remove all user keywords from dataset_keyword_map\n"
            sql_out += "DELETE FROM dataset_keyword_map WHERE dataset_id = '%s' AND keyword_id ~ 'uk-';\n" % uid

            if data["user_keywords"] != "":
                last_id = None
                sql_out += "\n--NOTE: add user keywords\n"
                for keyword in data["user_keywords"].split(','):
                    keyword = keyword.strip()
                    # first check if the keyword is already in the database - check keyword_usap and keyword_ieda tables
                    query = "SELECT keyword_id FROM keyword_ieda WHERE UPPER(keyword_label) = UPPER('%s') UNION SELECT keyword_id FROM keyword_usap WHERE UPPER(keyword_label) = UPPER('%s')" % (keyword, keyword)
                    cur.execute(query)
                    res = cur.fetchone()
                    if res is not None:
                        sql_out += "INSERT INTO dataset_keyword_map(dataset_id,  keyword_id) VALUES ('{}','{}'); -- {}\n".format(uid, res['keyword_id'], keyword)
                    else:
                        #if not found, add to keyword_usap
                        # first work out the last keyword_id used
                        query = "SELECT keyword_id FROM keyword_usap ORDER BY keyword_id DESC"
                        cur.execute(query)
                        res = cur.fetchone()
                        if not last_id:
                            last_id = res['keyword_id'].replace('uk-', '')
                        next_id = int(last_id) + 1
                        sql_out += "--INSERT INTO keyword_usap (keyword_id, keyword_label, keyword_type_id, source) VALUES ('uk-%s', '%s', 'REPLACE_ME', 'user');\n" % \
                            (next_id, keyword)
                        sql_out += "--INSERT INTO dataset_keyword_map(dataset_id,  keyword_id) VALUES ('{}','uk-{}');\n".format(uid, next_id)
                        last_id = next_id

    sql_out += "\nUPDATE dataset SET date_modified = '%s' WHERE id= '%s';\n" % (data['timestamp'][0:10], uid)
    sql_out += '\nCOMMIT;\n'

    return sql_out


def write_readme(data, id):
    doc_dir = os.path.join("doc", id)
    try:
        if not os.path.exists(doc_dir):
            oldmask = os.umask(000)
            os.makedirs(doc_dir, 0o775)
            os.umask(oldmask)

        out_filename = os.path.join(doc_dir, 'README_' + id + '.txt')
        text = []
        text.append('USAP-DC Dataset# ' + id + '\n')
        text.append(data["timestamp"][:10] + '\n')
        text.append(dc_config['SERVER'] + '/' + dc_config['SHOULDER'] + id + '\n')
        text.append('\nabstract:\n')
        text.append(data["abstract"] + '\n')
        text.append('\nInstruments and devices:\n')
        text.append(data["devices"] + '\n')
        text.append('\nAcquisition procedures:\n')
        text.append(data["procedures"] + '\n')
        text.append('\nContent and processing steps:\n')
        text.append(data["content"] + '\n\n')
        text.append(data["data_processing"] + '\n')
        text.append('\nLimitations and issues:\n')
        text.append(data["issues"] + '\n')
        text.append('\nCheckboxes:\n')
        text.append('* All the data are referenced in time and space.\n')
        text.append('* The data column, properties, and attributes listed/used in the data files are explained either in the description and/or the data files themselves.\n')
        text.append('* Graphs and maps (if provided) have legends.\n')
          
        #--- write the text to output file
        with open(out_filename, 'w') as out_file:
            out_file.writelines(text)
        os.chmod(out_filename, 0o664)
        
        return out_filename
    except Exception as e:
        return "Error writing README file: \n%s" % str(e)


def json2sql(data, id):
    if data:
        # check if we are editing an existing project
        if data.get('edit') and data['edit'] == 'True':
            sql = editDatasetJson2sql(data, id)
        else:
            data = parse_json(data)
            sql = make_sql(data, id)
        readme_file = write_readme(data, id)
        return sql, readme_file
    else:
        print('Error: no data have been processed!')
    return None
