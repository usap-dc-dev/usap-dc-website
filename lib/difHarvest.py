from pickle import FALSE
from selectors import EpollSelector
import xml.dom.minidom as minidom
import xml.etree.ElementTree as ET
import usap
import lib.curatorFunctions as cf
from flask import request
import requests
import json
import itertools


config = json.loads(open('config.json', 'r').read())

def getUpdateSQL(proj_uid, dif_id):
    # db_xml = getDifXMLFromDB(proj_uid)
    amd_xml = getDifXMLFromAMD(dif_id)

    (conn, cur) = usap.connect_to_db()
    sql = ""
    if not amd_xml:
        sql += "-- NO DIF record found in AMD for project %s, dif_id %s\n" % (proj_uid, dif_id)
    else:
        sql += getPlatforms(amd_xml, '{http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/}', cur, proj_uid)
        sql += getPaleoTime(amd_xml, '{http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/}', cur, proj_uid)
        sql += getProgress(amd_xml, '{http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/}', cur, proj_uid)
        sql += getProductLevel(amd_xml, '{http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/}', cur, proj_uid)
        sql += getDataType(amd_xml, '{http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/}', cur, proj_uid)
        sql += getAncillaryKeywords(amd_xml, '{http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/}', cur, proj_uid)
        sql += getFormat(amd_xml, '{http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/}', cur, proj_uid)
        sql += getPersonnel(amd_xml, '{http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/}', cur, proj_uid)
        sql += "\nCOMMIT;"
    return sql
    # return(cf.prettify(amd_xml))
    # return (amd_platforms)


def getPersonnel(root, ns, cur, proj_uid):
    sql = ''
    if root:
        for person in root.findall(ns+'Personnel'):
            role = person.find(ns+'Role').text
            contact = person.find(ns+'Contact_Person')
            if not contact:
                continue
            first_name = ''
            if contact.find(ns+'First_Name') != None: 
                first_name = contact.find(ns+'First_Name').text
            middle_name = ''
            if contact.find(ns+'Middle_Name') != None: 
                middle_name = contact.find(ns+'Middle_Name').text
            last_name = contact.find(ns+'Last_Name').text
            email = ''
            if contact.find(ns+'Email') != None:
                email = contact.find(ns+'Email').text
            phone = ''
            fax = ''
            for ph in contact.findall(ns+'Phone'):
                if ph.find(ns+'Type') != None and ph.find(ns+'Type').text.lower() == 'fax':
                    fax = ph.find(ns+'Number').text
                else:
                    phone = ph.find(ns+'Number').text
            address = []
            city = ''
            state = ''
            zip = ''
            country = ''
            ca = contact.find(ns+'Address')
            if ca != None: 
                for street in ca.findall(ns+'Street_Address'):
                    address.append(street.text)
                if ca.find(ns+'City') != None:
                    city = ca.find(ns+'City').text
                if ca.find(ns+'State_Province') != None:
                    state = ca.find(ns+'State_Province').text
                if ca.find(ns+'Postal_Code') != None:
                    zip = ca.find(ns+'Postal_Code').text
                if ca.find(ns+'Country') != None:
                    country = ca.find(ns+'Country').text
            address_str = ', '.join(address)

            sql += "-- HARVESTED PERSONNEL DATA:\n"
            sql += "-- ROLE: %s;  FIRST_NAME: %s;  MIDDLE_NAME: %s;  LAST_NAME: %s\n" % (role, first_name, middle_name, last_name)
            # sql += "-- EMAIL: %s;  PHONE: %s;  FAX: %s\n" % (email, phone, fax)
            # sql += "-- STREET: %s;  CITY: %s;  STATE: %s;  ZIP: %s;  COUNTRY: %s\n" % (address_str, city, state, zip, country)


            #search for person in person table
            person_id = "%s, %s" % (last_name, first_name)
            if email:
                query = "SELECT * FROM person WHERE id ~* %s AND id ~* %s OR email = %s"  
                cur.execute(query,(usap.escapeQuotes(first_name.split(' ')[0]), usap.escapeQuotes(last_name.split(' ')[0]), email))
            else:
                query = "SELECT * FROM person WHERE id ~* '%s' AND id ~* '%s'"  %(usap.escapeQuotes(first_name.split(' ')[0]), usap.escapeQuotes(last_name.split(' ')[0]))
                cur.execute(query)
            res = cur.fetchall()
            if len(res) == 0:
                sql += "-- No person found in person that matches %s\n" % person_id
                sql += "INSERT INTO person (id, first_name, last_name, middle_name, email, phone, fax, address, city, state, zip, country) " \
                        "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');\n" % \
                        (usap.escapeQuotes(person_id), usap.escapeQuotes(first_name), usap.escapeQuotes(last_name), usap.escapeQuotes(middle_name), 
                         email, phone, fax, usap.escapeQuotes(address_str), usap.escapeQuotes(city), usap.escapeQuotes(state), usap.escapeQuotes(zip), usap.escapeQuotes(country))
                sql += addToProjectPersonMap(cur, proj_uid, person_id, role)
            
            elif len(res) == 1:
                person_id = res[0]['id']
                sql += "-- person found.  ID: %s\n" % person_id
                sql += addToProjectPersonMap(cur, proj_uid, person_id, role)
            else:
                mult_sql = "-- Multiple possible entries found for %s.  Uncomment most suitable option.\n" % person_id
                found = False
                for r in res:
                    this_person_sql = addToProjectPersonMap(cur, proj_uid, r['id'], role)
                    if 'Person already mapped to project' in this_person_sql:
                        sql += "-- person found.  ID: %s\n" % r['id']
                        sql += this_person_sql
                        found = True
                        break
                    mult_sql += "-- %s" % this_person_sql
                if not found:
                    sql += mult_sql

            sql+= "\n"
    
    return sql


def addToProjectPersonMap(cur, proj_uid, person_id, role):
    # first check if person is already mapped to person
    query = "SELECT * FROM project_person_map WHERE proj_uid = %s AND person_id = %s;"
    cur.execute(query, (proj_uid, person_id))
    res = cur.fetchone()
    if res:
        return "-- Person already mapped to project\n"

    # check if harvested role matches entries in role table
    query = "SELECT * FROM role WHERE lower(id) = lower(%s);"
    cur.execute(query, (role,))
    res = cur.fetchone()
    if res:
        sql = "INSERT INTO project_person_map (proj_uid, person_id, role) VALUES ('%s', '%s', '%s');\n" % (proj_uid, usap.escapeQuotes(person_id), res['id'])
    else:
        sql = "-- Harvested role :%s not found in database. Manually enter value\n"
        sql += "INSERT INTO project_person_map (proj_uid, person_id, role) VALUES ('%s', '%s', 'TBD');\n" % (proj_uid, usap.escapeQuotes(person_id))
    return sql



def getFormat(root, ns, cur, proj_uid):
    sql = ''
    formats = []
    if root:
        for d in root.findall(ns+'Distribution'):
            if not d.find(ns+'Distribution_Format'):
                continue
            format = d.find(ns+'Distribution_Format').text
            sql += "-- HARVESTED DISTRIBUTION FORMAT: %s\n" %format
            #search for keyword in gcmd_data_format table
            query = "SELECT * from gcmd_data_format WHERE lower(short_name) = lower(%s)"
            cur.execute(query,(format,))
            res = cur.fetchall()
            if len(res) == 0:
                sql += "-- No entry found in gcmd_data_format that matches harvested type %s\n" % format
            elif len(res) == 1:
                formats.append(res[0]['short_name'])
            else:
                sql += "-- ERROR - multiple entries found in gcmd_data_format for id %s\n" % format
        if len(formats) > 0:
            sql += "UPDATE project_dataset SET data_format='%s' WHERE proj_uid='%s';\n" % (';'.join(formats), proj_uid)    
            sql += "\n"
    return sql


def getAncillaryKeywords(root, ns, cur, proj_uid):
    sql = ''

    if root:

        # first work out the last keyword_id used
        query = "SELECT keyword_id FROM keyword_usap ORDER BY keyword_id DESC"
        cur.execute(query)
        res = cur.fetchone()
        last_id = res['keyword_id'].replace('uk-', '')
        next_id = int(last_id) + 1

        # sometime keywords are combined by commas, and sometimes they have their own entry
        # combine them all into one list
        keywords = [ak.text.split(', ') for ak in root.findall(ns+'Ancillary_Keyword')]
        keywords = list(itertools.chain.from_iterable(keywords))

        for keyword in keywords:
            sql += "-- HARVESTED ANCILLARY KEYWORD: %s\n" %keyword
            #search for keyword in keyword_usap table
            query = "SELECT * from keyword_usap WHERE lower(keyword_label) = lower(%s)"
            cur.execute(query,(keyword,))
            res = cur.fetchall()
            if len(res) == 0:
                sql += "-- New keyword (NB keyword_type will need to be added manually)\n" 
                kw = 'uk-' + str(next_id)
                sql += "INSERT INTO keyword_usap (keyword_id, keyword_label, keyword_type_id, source) VALUES ('%s', '%s', 'TBD', 'AMD Harvest');\n" % (kw, keyword)
                sql += "INSERT INTO project_keyword_map (proj_uid, keyword_id) VALUES ('%s', '%s');\n" % (proj_uid, kw)
                next_id +=1
            elif len(res) == 1:
                #check if keyword already mapped to project
                query = "SELECT * FROM project_keyword_map WHERE proj_uid = %s AND keyword_id = %s"
                cur.execute(query,(proj_uid,res[0]['keyword_id']))
                res2 = cur.fetchall()
                if len(res2) == 0:
                    sql += "INSERT INTO project_keyword_map (proj_uid, keyword_id) VALUES ('%s', '%s');\n" % (proj_uid, res[0]['keyword_id'])
                else:
                    sql += "-- Keyword already mapped to project\n"
            else:
                sql += "-- ERROR - multiple entries found in keyword_usap for %s\n" % keyword
            sql += "\n"    
    return sql        



def getDataType(root, ns, cur, proj_uid):
    sql = ''
    if root:
        for dt in root.findall(ns+'Collection_Data_Type'):
            type = dt.text
            sql += "-- HARVESTED COLLECTION DATA TYPE: %s\n" %type
            #search for keyword in gcmd_collection_data_type table
            query = "SELECT * from gcmd_collection_data_type WHERE lower(id) = lower(%s)"
            cur.execute(query,(type,))
            res = cur.fetchall()
            if len(res) == 0:
                sql += "-- No entry found in gcmd_collection_data_type that matches harvested type %s\n" % type
            elif len(res) == 1:
                sql += "UPDATE project SET collection_data_type='%s' WHERE proj_uid='%s';\n" % (res[0]['id'],proj_uid)
            else:
                sql += "-- ERROR - multiple entries found in gcmd_collection_data_type for id %s\n" % type
            sql += "\n"
    return sql


def getProductLevel(root, ns, cur, proj_uid):
    sql = ''
    if root:
        for pl in root.findall(ns+'Product_Level_Id'):
            level = pl.text
            sql += "-- HARVESTED PRODUCT LEVEL ID: %s\n" %level
            #search for level in product_level table
            query = "SELECT * from product_level WHERE lower(id) = lower(%s)"
            cur.execute(query,(level,))
            res = cur.fetchall()
            if len(res) == 0:
                sql += "-- No entry found in product_level that matches harvested level %s\n" % level
            elif len(res) == 1:
                sql += "UPDATE project SET product_level_id='%s' WHERE proj_uid='%s';\n" % (res[0]['id'],proj_uid)
            else:
                sql += "-- ERROR - multiple entries found in product_level for id %s\n" % level
            sql += "\n"
    return sql


def getProgress(root, ns, cur, proj_uid):
    sql = ''
    if root:
        for dp in root.findall(ns+'Dataset_Progress'):
            progress = dp.text
            sql += "-- HARVESTED DATASET PROGRESS: %s\n" %progress
            #search for progress in gcmd_collection_progress table
            query = "SELECT * from gcmd_collection_progress WHERE lower(id) = lower(%s)"
            cur.execute(query,(progress,))
            res = cur.fetchall()
            if len(res) == 0:
                sql += "-- No entry found in gcmd_collection_progress that matches harvested progress %s\n" % progress
            elif len(res) == 1:
                sql += "UPDATE project SET project_progress='%s' WHERE proj_uid='%s';\n" % (res[0]['id'],proj_uid)
            else:
                sql += "-- ERROR - multiple entries found in gcmd_collection_progress for id %s\n" % progress
            
            sql += "\n"
    return sql


def getPaleoTime(root, ns, cur, proj_uid):
    sql = ''
    if root:
        for tc in root.findall(ns+'Temporal_Coverage'):
            paleo_time = tc.findall(ns + 'Paleo_DateTime')
            # if len(paleo_time) > 0 :
            #     sql += "-- FOUND PALEO TIME!!!\n"
            for pt in tc.findall(ns + 'Paleo_DateTime'):
                start_date = None
                stop_date = None
                chrono = None
                if pt.find(ns+'Paleo_Start_Date') != None:
                    start_date = pt.find(ns+'Paleo_Start_Date').text
                if pt.find(ns+'Paleo_Stop_Date') != None:
                    stop_date = pt.find(ns+'Paleo_Stop_Date').text 
                for cu in pt.findall(ns+'Chronostratigraphic_Unit'):
                    chrono = None
                    
                    if cu.find(ns+'Eon') != None:
                        chrono = cu.find(ns+'Eon').text
                    if cu.find(ns+'Era') != None:
                        chrono += " > " + cu.find(ns+'Era').text
                    if cu.find(ns+'Period') != None:
                        chrono += " > " + cu.find(ns+'Period').text
                    if cu.find(ns+'Epoch') != None:
                        chrono += " > " + cu.find(ns+'Epoch').text
                    if cu.find(ns+'Age') != None:
                        chrono += " > " + cu.find(ns+'Age').text
                    if cu.find(ns+'Sub_Age') != None:
                        chrono += " > " + cu.find(ns+'Sub_Age').text     

                    if chrono:
                        sql+= "-- HARVESTED PALEO TIME: %s\n" %chrono
                    if start_date:
                        sql += "-- START DATE: %s\n" %start_date
                    if stop_date:
                        sql += "-- END DATE: %s\n" %stop_date

                    #search for paleo_time in gcmd_paleo_time table
                    if chrono:
                        query = "SELECT * from gcmd_paleo_time WHERE lower(id) = lower(%s)"
                        cur.execute(query,(chrono,))
                        res = cur.fetchall()
                        if len(res) == 0:
                            sql += "-- No entry found in gcmd_paleo_time that matches %s\n" % chrono
                        elif len(res) == 1:
                            sql += "INSERT INTO project_gcmd_paleo_time_map (proj_uid, paleo_time_id, paleo_start_date, paleo_stop_date) VALUES ('%s', '%s', '%s', '%s');\n" \
                                    %(proj_uid, res[0]['id'],  usap.escapeQuotes(start_date),  usap.escapeQuotes(stop_date) )
                        else:
                            sql += "-- ERROR - multiple entries found in gcmd_paleo_time for id %s\n" % chrono
                if not chrono and (start_date or stop_date):
                    sql += "INSERT INTO project_gcmd_paleo_time_map (proj_uid, paleo_start_date, paleo_stop_date) VALUES ('%s', '%s', '%s');\n" \
                                    %(proj_uid,  usap.escapeQuotes(start_date),  usap.escapeQuotes(stop_date))

                sql += "\n"        
    return sql


def getPlatforms(root, ns, cur, proj_uid):
    sql = ''
    if root:
        for platform in root.findall(ns+'Platform'):
            short_name = platform.find(ns+'Short_Name').text
            long_name = ''
            if platform.find(ns+'Long_Name'):
                long_name = platform.find(ns+'Long_Name').text
            sql += "-- HARVESTED PLATFORM VALUES:\n"
            sql += "-- TYPE: %s;  SHORT_NAME: %s;  LONG_NAME: %s\n" % (platform.find(ns+'Type').text, short_name, long_name)
            #search for plaform in gcmd_platform table
            query = "SELECT * from gcmd_platform WHERE lower(short_name) = lower(%s)"
            cur.execute(query,(short_name,))
            res = cur.fetchall()
            if len(res) == 0:
                sql += "-- No entry found in gcmd_table that matches short_name %s\n" % short_name
            elif len(res) == 1:
                sql += "INSERT INTO project_gcmd_platform_map (proj_uid, platform_id) VALUES ('%s', '%s');\n" % (proj_uid, res[0]['id'])
            else:
                sql += "-- Multiple options found in gcmd_platform for short_name %s.  Uncomment most suitable option.\n" % short_name
                for r in res:
                    sql += "-- INSERT INTO project_gcmd_platform_map (proj_uid, platform_id) VALUES ('%s', '%s');\n" % (proj_uid, r['id'])
            

            for inst in platform.findall(ns+'Instrument'):
                instrument = inst.find(ns+'Short_Name').text
                sql += "\n-- HARVESTED INSTRUMENT VALUES:\n"
                sql += "-- SHORT_NAME: %s\n" % (instrument)
                # search for instrument in gcmd_instrument table
                query = "SELECT * from gcmd_instrument WHERE lower(short_name) = lower(%s)"
                cur.execute(query,(instrument,))
                res2 = cur.fetchall()
                if len(res2) == 0:
                    sql += "-- No entry found in gcmd_table that matches short_name %s\n" % instrument
                elif len(res2) == 1:
                    if len(res) > 0:
                        sql += "INSERT INTO project_gcmd_platform_instrument_map (proj_uid, platform_id, instrument_id) VALUES ('%s', '%s', '%s');\n" % (proj_uid, res[0]['id'], res2[0]['id'])
                    else:
                        sql += "INSERT INTO project_gcmd_platform_instrument_map (proj_uid, platform_id, instrument_id) VALUES ('%s', 'Not provided', '%s');\n" % (proj_uid, res2[0]['id'])
                else:
                    sql += "-- Multiple options found in gcmd_instrument for short_name %s.  Uncomment most suitable option.\n" % instrument
                    for r in res2:
                        sql += "-- INSERT INTO project_gcmd_platform__instrument_map (proj_uid, platform_id, instrument_id) VALUES ('%s', '%s', '%s');\n" % (proj_uid, res[0]['id'], r['id'])
            sql += "\n"
        sql += "\n"


    return sql


def getDifXMLFromDB(uid):

    data = usap.get_project(uid)

    root = ET.Element("DIF")
    root.set("xmlns", "http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/")
    root.set("xmlns:dif", "http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/")
    root.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
    root.set("xsi:schemaLocation", "http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/ https://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/dif_v10.2.xsd")

    # --- entry and title
    xml_entry = ET.SubElement(root, "Entry_ID")
    short_id = ET.SubElement(xml_entry, "Short_Name")
    short_id.text = cf.getDifID(uid).split('_')[0]
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

    # --- platform
    xml_platform = ET.SubElement(root, "Platform")
    xml_platform_type = ET.SubElement(xml_platform, "Type")
    xml_platform_type.text = "Not applicable"
    xml_platform_sname = ET.SubElement(xml_platform, "Short_Name")
    xml_platform_sname.text = "Not applicable"
    xml_instrument = ET.SubElement(xml_platform, "Instrument")
    xml_instrument_sname = ET.SubElement(xml_instrument, "Short_Name")
    xml_instrument_sname.text = "Not applicable"

    # --- temporal coverage
    xml_time = ET.SubElement(root, "Temporal_Coverage")
    xml_time_range = ET.SubElement(xml_time, "Range_DateTime")
    xml_time_begin = ET.SubElement(xml_time_range, "Beginning_Date_Time")
    xml_time_begin.text = str(data.get('start_date'))
    xml_time_end = ET.SubElement(xml_time_range, "Ending_Date_Time")
    xml_time_end.text = str(data.get('end_date'))

    # --- progress
    xml_progress = ET.SubElement(root, "Dataset_Progress")
    xml_progress.text = "COMPLETE"
    
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

    return root


def getDifXMLFromAMD(dif_id):

    # get dif record from AMD
    api_url = config['CMR_API'] + dif_id
    try:
        r = requests.get(api_url).json()
        concept_id = r['feed']['entry'][0]['id']
        # generate the GCMD page URL
        cmr_url = config['CMR_URL'] + concept_id + '.dif10'
        print(cmr_url)
        xml = requests.get(cmr_url).content

        root = ET.fromstring(xml)
        ET.register_namespace("", "http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/")
    except:
        return None

    return root
