# -*- coding: utf-8 -*-
"""
Created on Sat Jul  8 14:23:05 2017

@author: fnitsche
"""

import json
import xml.etree.ElementTree as ET
from xml.dom import minidom

dif_id = 'USAP-1341476'

in_file = '{}.json'.format(dif_id)
dif_file = '{}.xml'.format(dif_id)
sql_file = '{}.sql'.format(dif_id)


def prettify(elem):
    """Return a pretty-printed XML string for the Element.
    """
    rough_string = ET.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")


class DifData:

    def __init__(self):
        self.dif_id = ''
        self.version = 1
        self.award = ''
        self.email = ''
        self.orcid = ''
        self.org = ''
        self.start = ''
        self.end = ''
        self.entry = ''
        self.abstract = ''
        self.locations = ''
        self.title = ''
        self.copi = ''
        self.parameters = ''
        self.pi_name = ''
        self.pi_fname = ''
        self.pi_lname = ''
        self.date_created = ''
        self.date_modified = ''

    def display(self):
        print()
        print(self.dif_id)

    def parse_from_json(self, file_name):
        data = {}
        with open(file_name) as data_file:
            data = json.load(data_file)

        # --- checking for required fields, make sure they all have data
        self.dif_id = 'USAP-' + data["award"] + '_1'
        self.award = data["award"]
        self.title = data["title"]
        self.email = data["email"]
        self.org = data["org"]
        self.copi = data["copi"]
        self.start = data["start"]
        self.end = data["end"]
        self.locations = data["locations"]
        self.parameters = data["parameters"]
        self.abstract = data["sum"]
        self.date = data["timestamp"]
        self.version = 1
        self.south = data["geo_s"]
        self.north = data["geo_n"]
        self.west = data["geo_w"]
        self.east = data["geo_e"]

        if data["name"] != "":
            (last, first) = data["name"].split(',')
            self.pi_name = data["name"]
            self.pi_fname = first.strip()
            self.pi_lname = last.strip()
        else:
            print("Warning -- no PI name")
            self.pi_fname = ''
            self.pi_lname = ''

        self.date_created = data["timestamp"][0:10]
        self.date_modified = data["timestamp"][0:10]
        # print(self.date_created)

        return

    def write_sql(self):
        sql_file_name = dif.dif_id + '.sql'
        with open(sql_file_name, 'w') as sql_file:
            sql_file.write('Begin;\n')
            line = "insert into dif_test(dif_id, award, date_created, date_modified, title, pi_name, co_pi, is_usap_dc,is_nsf)" \
                   " values ('{}','{}','{}','{}','{}','{}','{}','t','t');\n\n"\
                   .format(self.dif_id, self.award, self.date_created, self.date_modified,
                           self.title, self.pi_name, self.copi)
            sql_file.write(line)
            
            summary = self.abstract.replace("'","''")
            line = "update dif_test set summary = '{}' where dif_id = '{}';\n\n"\
            .format(summary, self.dif_id)
            sql_file.write(line)
            
            line = "insert into dif_data_url_map(dif_id, repository, title, dataset_id, url)" \
                   " values ('{}','','','','');\n\n"\
                   .format(self.dif_id, self.title)
            sql_file.write(line)
            
            line = "insert into dif_award_map(dif_id, award) values" \
                "('{}','{}');\n\n".format(self.dif_id, self.award)
            sql_file.write(line)
            
            line = "insert into dif_spatial_map(dif_id, west, east, south, north) values" \
                "('{}',{},{},{},{});\n\n".format(self.dif_id, self.west,self.east, self.south, self.north)
            sql_file.write(line)

            line = "insert into dif(dif_id) values ('{}');\n\n".format(self.dif_id)
            sql_file.write(line)
            line = "insert into dataset_dif_map(dataset_id, dif_id)" \
                   " values ('','{}');\n\n".format(self.dif_id)
            sql_file.write(line)
            sql_file.write('Commit;\n')
        return


    def write_tab_info(self):
        file_name = dif.dif_id + '.tab'

        with open(file_name, 'w') as out_file:
            line = "{0}\t{1}\t{2}\t{3}\t\t\t{4}\t{4}\n"\
                .format(self.award, self.dif_id, self.pi_name, self.title, self.date_created)
            out_file.write(line)

        return


    def write_xml(self):
        file_name = dif.dif_id + '.xml'

        root = ET.Element("DIF")
        root.set("xmlns", "http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/")
        root.set("xmlns:dif", "http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/")
        root.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.set("xsi:schemaLocation", "http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/ https://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/dif_v10.2.xsd")

        # --- entry and title
        xml_entry = ET.SubElement(root, "Entry_ID")
        short_id = ET.SubElement(xml_entry, "Short_Name")
        short_id.text = self.dif_id.replace('_1','')
        version = ET.SubElement(xml_entry, "Version")
        version.text = "1"
        xml_entry = ET.SubElement(root, "Entry_Title")
        xml_entry.text = self.title

        # ---- personel
        xml_pi = ET.SubElement(root, "Personnel")
        xml_pi_role = ET.SubElement(xml_pi, "Role")
        xml_pi_role.text = "INVESTIGATOR"
        xml_pi_contact = ET.SubElement(xml_pi, "Contact_Person")
        xml_pi_contact_fname = ET.SubElement(xml_pi_contact, "First_Name")
        xml_pi_contact_fname.text = self.pi_fname
        xml_pi_contact_lname = ET.SubElement(xml_pi_contact, "Last_Name")
        xml_pi_contact_lname.text = self.pi_lname
        xml_pi_contact_address = ET.SubElement(xml_pi_contact, "Address")
        xml_pi_contact_address1 = ET.SubElement(xml_pi_contact_address, "Street_Address")
        xml_pi_contact_address1.text = self.org
        xml_pi_contact_email = ET.SubElement(xml_pi_contact, "Email")
        xml_pi_contact_email.text = self.email

        # --- science keywords
        # print(self.parameters)
        for keyword in self.parameters:
            keys = keyword.split('>')
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
        xml_time_begin.text = self.start
        xml_time_end = ET.SubElement(xml_time_range, "Ending_Date_Time")
        xml_time_end.text = self.end

        # --- Spatial coverage
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
        xml_space_geom_south.text = self.south
        xml_space_geom_south = ET.SubElement(xml_space_geom_bound, "Northernmost_Latitude")
        xml_space_geom_south.text = self.north
        xml_space_geom_south = ET.SubElement(xml_space_geom_bound, "Westernmost_Longitude")
        xml_space_geom_south.text = self.west
        xml_space_geom_south = ET.SubElement(xml_space_geom_bound, "Easternmost_Longitude")
        xml_space_geom_south.text = self.east

        # --- location
        # print(self.locations)
        for location in self.locations:
            loc_val = location.split('>')
            xml_loc = ET.SubElement(root, "Location")
            xml_loc_cat = ET.SubElement(xml_loc, "Location_Category")
            xml_loc_cat.text = loc_val[0].strip()
            if len(loc_val) > 1:
                xml_loc_type = ET.SubElement(xml_loc, "Location_Type")
                xml_loc_type.text = loc_val[1].strip()
            if len(loc_val) > 2:
                xml_loc_sub1 = ET.SubElement(xml_loc, "Location_Subregion1")
                xml_loc_sub1.text = loc_val[2].strip()

        # --- project
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
        text = self.abstract.replace('<br/>','\n\n')
        xml_abstract.text = text

        # --- related URL
        xml_url = ET.SubElement(root, "Related_URL")
        xml_url_ctype = ET.SubElement(xml_url, "URL_Content_Type")
        xml_url_type = ET.SubElement(xml_url_ctype, "Type")
        xml_url_type.text = "VIEW RELATED INFORMATION"
        xml_url_url = ET.SubElement(xml_url, "URL")
        xml_url_url.text = "http://www.nsf.gov/awardsearch/showAward.do?AwardNumber={0}".format(self.award)
        xml_url_desc = ET.SubElement(xml_url, "Description")
        xml_url_desc.text = "NSF Award Abstract"

        xml_url = ET.SubElement(root, "Related_URL")
        xml_url_ctype = ET.SubElement(xml_url, "URL_Content_Type")
        xml_url_type = ET.SubElement(xml_url_ctype, "Type")
        xml_url_type.text = "GET DATA"
        xml_url_url = ET.SubElement(xml_url, "URL")
        xml_url_url.text = "[VALUE]"
        xml_url_desc = ET.SubElement(xml_url, "Description")
        xml_url_desc.text = "[VALUE]"

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
        xml_meta_date_create.text = self.date_created
        xml_meta_date_mod = ET.SubElement(xml_meta_date, "Metadata_Last_Revision")
        xml_meta_date_mod.text = self.date_modified
        xml_data_date_create = ET.SubElement(xml_meta_date, "Data_Creation")
        xml_data_date_create.text = self.date_created
        xml_data_date_mod = ET.SubElement(xml_meta_date, "Data_Last_Revision")
        xml_data_date_mod.text = self.date_created

        # xml_product_level = ET.SubElement(root, "Product_Level_Id")
        # xml_product_level.text = "Not provided"

        # tree = ET.ElementTree(root)
        # tree.write(file_name)

        with open(file_name, 'w') as out_file:
            out_file.write(prettify(root))
        return


def check_data(data):

    return data


# ---- main
print('\npreparing ', dif_id)
# data = parse_json(in_file)

dif = DifData()
dif.parse_from_json(in_file)
dif.write_sql()
dif.write_xml()
dif.write_tab_info()

print("done")
