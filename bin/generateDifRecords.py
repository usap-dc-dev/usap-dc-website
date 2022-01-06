import xml.etree.ElementTree as ET
import psycopg2
import psycopg2.extras
import json
import os
import xml.dom.minidom as minidom
import sys


config = json.loads(open('config.json', 'r').read())
DIFXML_FOLDER = "/web/usap-dc/htdocs/watch/difxml_new"


def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER_CURATOR'],
                            password=info['PASSWORD_CURATOR'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


def prettify(elem):
    """Return a pretty-printed XML string for the Element.
    """
    rough_string = ET.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="    ")


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
                        ) keywords ON keywords.kw_proj_uid = p.proj_uid
                        WHERE p.proj_uid = '%s' ORDER BY p.title''' % project_id)
        cur.execute(query_string)
        return cur.fetchone()


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
    xml_entry.text = data.get('title').decode('utf-8')

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
            xml_key_cat.text = keys[0].strip().decode('utf-8')
            xml_key_topic = ET.SubElement(xml_key, "Topic")
            xml_key_topic.text = keys[1].strip().decode('utf-8')
            xml_key_term = ET.SubElement(xml_key, "Term")
            xml_key_term.text = keys[2].strip().decode('utf-8')
            if len(keys) > 3:
                xml_key_level1 = ET.SubElement(xml_key, "Variable_Level_1")
                xml_key_level1.text = keys[3].strip().decode('utf-8')

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
            xml_loc_cat.text = loc_val[0].strip().decode('utf-8')
            if len(loc_val) > 1:
                xml_loc_type = ET.SubElement(xml_loc, "Location_Type")
                xml_loc_type.text = loc_val[1].strip().decode('utf-8')
            if len(loc_val) > 2:
                xml_loc_sub1 = ET.SubElement(xml_loc, "Location_Subregion1")
                xml_loc_sub1.text = loc_val[2].strip().decode('utf-8')

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
    xml_org_url.text = "https://www.usap-dc.org"

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
    xml_abstract.text = text.decode('utf-8')
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
    if file_name:
        with open(file_name, 'w') as out_file:
            out_file.write(prettify(root).encode('utf-8'))
        os.chmod(file_name, 0o664)
    else:
        print('NO AWARD FOUND FOR %s' % uid)

    return prettify(root)


def getDifID(uid):
    conn, cur = connect_to_db()
    query = "SELECT award_id FROM project_award_map WHERE is_main_award = 'True' AND proj_uid = '%s';" % uid
    cur.execute(query)
    res = cur.fetchone()
    if res:
        return "USAP-%s_1" % res['award_id']
    return None


def getDifXMLFileName(uid):
    if getDifID(uid):
        return os.path.join(DIFXML_FOLDER, "%s.xml" % getDifID(uid)).replace(' ', '')
    return None

# def addDifToDB(uid):
#     (conn, cur) = connect_to_db(curator=True)
#     status = 1
#     if type(conn) is str:
#         out_text = conn
#         status = 0
#     else:
#         try:
#             sql_cmd = ""
#             dif_id, title = getDifIDAndTitle(uid)
#             # Add to dif table if not already there
#             query = "SELECT * FROM dif WHERE dif_id = '%s';" % dif_id
#             cur.execute(query)
#             res = cur.fetchall()
#             if len(res) == 0:
#                 sql_cmd += "INSERT INTO dif (dif_id, date_created, date_modified, title, is_usap_dc, is_nsf) VALUES ('%s', '%s', '%s', '%s', %s, %s);\n" % \
#                     (dif_id, datetime.now().date(), datetime.now().date(), title, True, True)

#             # add to project_dif_map
#             query = "SELECT * FROM project_dif_map WHERE proj_uid = '%s' AND dif_id = '%s';" % (uid, dif_id)
#             cur.execute(query)
#             res = cur.fetchall()
#             if len(res) == 0:
#                 sql_cmd += "INSERT INTO project_dif_map (proj_uid, dif_id) VALUES ('%s', '%s');" % (uid, dif_id)

#             sql_cmd += "COMMIT;"

#             cur.execute(sql_cmd)

#             out_text = "Succesfully added DIF record to database."
#         except Exception as e:
#             out_text = "Error adding DIF record to database. \n%s" % str(e)
#             status = 0

#     return (out_text, status)


if __name__ == '__main__':
    # get all usap_dc projects
    (conn, cur) = connect_to_db()
    query = "SELECT proj_uid from project ORDER BY proj_uid"
    cur.execute(query)
    projects = cur.fetchall()
    for project in projects:
        uid = project['proj_uid']
        # if uid != 'p0010126': continue
        print(uid)
        if getDifID(uid):
            proj_data = get_project(uid)
            difxml = getDifXML(proj_data, uid)
        else:
            print('NO AWARD FOUND')

    print("DONE")
