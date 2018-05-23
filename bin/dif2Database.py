# Read in GCMD DIF files that have been extracted using the following command for page_num=1 to 4:
# curl -i "https://cmr.earthdata.nasa.gov/search/collections.dif10?page_num=1&page_size=1000&keyword=AMD/US" >> amd_us_2018_04_25_all.xml
# and import them in to the database
# run from main usap directory with >bin/dif2Database.py

import xml.etree.ElementTree as ET
import psycopg2
import psycopg2.extras
import json

config = json.loads(open('config.json', 'r').read())

repo_dict = {"www.usap-dc.org": "USAP-DC",
             "www.marine-geo.org": "MGDS",
             "www.iris.edu": "IRIS",
             "nsidc.org": "NSIDC",
             "amrc.ssec.wisc.edu": "AMRC",
             "usjgofs.whoi.edu": "JGOF",
             "www.ncdc.noaa.gov": "NCEI",
             "www.ncbi.nlm.nih.gov": "GenBank",
             "www.washington.edu/burkemuseum": "Burke Museum",
             "accession.nodc.noaa.gov": "NCEI",
             "www.unavco.org": "UNAVCO",
             "oceaninformatics.ucsd.edu": "PALTER",
             "www.mcmlter.org": "MCMELTER",
             "www.arf.fsu.edu": "AMGRF"             
             }


def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER'],
                            password=info['PASSWORD'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


def alreadyInDB(dif_id):
    conn, cur = connect_to_db()
    query = "SELECT COUNT(*) FROM dif_test WHERE dif_id = '%s'" % dif_id
    cur.execute(query)
    res = cur.fetchone()
    return int(res['count']) > 0


def updateExistingRecord(dif_id, summary):
    conn, cur = connect_to_db()
    query = """UPDATE dif_test SET (is_usap_dc, is_nsf, dif_name, summary) = (TRUE, TRUE, '%s', '%s') WHERE dif_id = '%s';""" % (dif_id, summary, dif_id)
    #print(query)
    cur.execute(query)
    cur.execute("COMMIT;")


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


def parse_xml(xml_file_name):
    conn, cur = connect_to_db()

    tree = ET.parse(xml_file_name)
    root = tree.getroot()

    count = 1
    for result in root.iter('result'):
        for dif_node in list(result):
            dif_id = ''
            version = 'NULL'
            name = ''
            title = ''
            award = ''
            creation_date = ''
            revision_date = ''
            pi_name = ''
            co_pi = ''
            datasets = []
            south = None
            north = None
            west = None
            east = None
            usap_flag = False
            nsf_flag = False
            summary = ''
            cross_dateline = False  # will need to be set in DB manually

            item_list = list(dif_node)
            for sub in item_list:
                if 'Entry_ID' in sub.tag:
                    for sub2 in sub.iter():
                        if 'Short_Name' in sub2.tag:
                            name = sub2.text
                        if 'Version' in sub2.tag and sub2.text.isdigit():
                            version = sub2.text
                            dif_id = name + "_" + version
                        else:
                            dif_id = name

                if 'Metadata_Dates' in sub.tag:
                    for sub2 in sub.iter():
                        if 'Metadata_Creation' in sub2.tag:
                            creation_date = sub2.text
                        if 'Metadata_Last_Revision' in sub2.tag:
                            revision_date = sub2.text

                if 'Entry_Title' in sub.tag:
                    title = sub.text.replace("'", "''")

                if 'Personnel' in sub.tag:
                    first_name = ''
                    last_name = ''
                    for sub2 in sub.iter():
                        if 'First_Name' in sub2.tag:
                            first_name = sub2.text.title().replace("'", "''")
                        if 'Last_Name' in sub2.tag:
                            last_name = sub2.text.title().replace("'", "''")

                    if not (last_name.lower() in 'user services'):
                        if not pi_name:
                            pi_name = "%s, %s" % (last_name, first_name)
                        else:
                            if len(co_pi) == 0:
                                co_pi = "%s, %s" % (last_name, first_name)
                            else:
                                co_pi += "; %s, %s" % (last_name, first_name)

                if 'Summary' in sub.tag:
                    for sub2 in sub.iter():
                        if 'Abstract' in sub2.tag:
                            summary = sub2.text.replace("'", "''")

                if 'Related_URL' in sub.tag:
                    data_type = ''
                    data_url = ''
                    data_desc = ''
                    for sub2 in sub.iter():

                        if '{http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/}URL' == sub2.tag:
                            data_url = sub2.text
                            if 'www.nsf.gov' in data_url:
                                award = data_url.replace('http://www.nsf.gov/awardsearch/showAward.do?AwardNumber=', '')\
                                                .replace('http://www.nsf.gov/awardsearch/showAward?AWD_ID=', '')\
                                                .replace('https://www.nsf.gov/awardsearch/showAward?AWD_ID=', '')\
                                                .replace('&HistoricalAwards=false', '')\
                                                .replace('&amp;HistoricalAwards=false', '')\
                                                .replace('http://www.nsf.gov/awardsearch/', '')
                        if 'Description' in sub2.tag:
                            data_desc = sub2.text.replace("'", "''")
                        if 'URL_Content_Type' in sub2.tag:
                            for sub3 in sub2.iter():
                                if 'Type' in sub3.tag:
                                    data_type = sub3.text
                    datasets.append((data_type, data_desc, data_url))

                if sub.tag == '{http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/}Organization':
                    for sub2 in sub.iter():
                        if 'Organization_Name' in sub2.tag:
                            for sub3 in sub2.iter():
                                if 'Short_Name' in sub3.tag:
                                    org = sub3.text
                                    if ('USAP/DCC' in org) or ('USAP-DC' in org) or ('NSIDC_AGDC' in org):
                                        usap_flag = True

                if 'IDN_Node' in sub.tag:
                    for sub2 in sub.iter():
                        if 'Short_Name' in sub2.tag and 'USA/NSF' in sub2.text:
                            nsf_flag = True

                if 'Spatial_Coverage' in sub.tag:
                    for sub2 in sub.iter():
                        if 'Southernmost_Latitude' in sub2.tag:
                            south = float(sub2.text)
                        if 'Northernmost_Latitude' in sub2.tag:
                            north = float(sub2.text)
                        if 'Westernmost_Longitude' in sub2.tag:
                            west = float(sub2.text)
                        if 'Easternmost_Longitude' in sub2.tag:
                            east = float(sub2.text)

            # check if id is already in the DB, if so, just update is_usap_dc, is_nsf, and summary
            if alreadyInDB(name):
                updateExistingRecord(name, summary)
            else:
                # add record to dif_test table
                query = """INSERT INTO dif_test (dif_id, award, date_created, date_modified, title, pi_name, co_pi, dif_name, dif_version, summary, is_usap_dc, is_nsf) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s');"""\
                    % (dif_id, award, creation_date, revision_date, title, pi_name, co_pi, name, version, summary, usap_flag, nsf_flag)
                #print(query)
                cur.execute(query)

                # add datasets to dif_data_url_map table
                for dataset in datasets:
                    ds_title = ''
                    ds_url = ''
                    ds_id = ''
                    ds_repo = ''

                    if 'GET DATA' in dataset[0] and 'www.nsf.gov' not in dataset[1]:
                        ds_title = dataset[1]
                        ds_url = dataset[2]
                        if 'www.usap-dc.org' in ds_url:
                            ds_id = ds_url[-6:]
                        for key in repo_dict.keys():
                            if key in ds_url:
                                ds_repo = repo_dict.get(key)

                        query = """INSERT INTO dif_data_url_map (dif_id, repository, title, dataset_id, url) VALUES('%s', '%s', '%s', '%s', '%s');"""\
                            % (dif_id, ds_repo, ds_title, ds_id, ds_url) 
                        #print(query)
                        cur.execute(query)

            # add to dif_spatial_map
            if (north and south and east and west):
                mid_point_lat = (south - north) / 2 + north
                mid_point_long = (east - west) / 2 + west
                geometry = "ST_GeomFromText('POINT(%s %s)', 4326)" % (mid_point_long, mid_point_lat)
                bounds_geometry = "ST_GeomFromText('%s', 4326)" % makeBoundsGeom(north, south, east, west, cross_dateline)

                this_dif_id = name if alreadyInDB(name) else dif_id
                query = """INSERT INTO dif_spatial_map (dif_id, west, east, south, north, geometry, cross_dateline, bounds_geometry) VALUES ('%s', '%s', '%s', '%s', '%s', %s, '%s', %s);"""\
                    % (this_dif_id, west, east, south, north, geometry, cross_dateline, bounds_geometry)
                #print(query)
                cur.execute(query)

            print(count)
            count += 1

    cur.execute("COMMIT;")


if __name__ == '__main__':
    parse_xml('inc/amd_us_2018_05_22_all.xml')
    print('done')
