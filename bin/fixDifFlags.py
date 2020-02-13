# Script needed to fix is_usap_dc and is_nsf flags in dev db.

# run from main usap directory with >python bin/fisDifFlags.py

import xml.etree.ElementTree as ET
import psycopg2
import psycopg2.extras
import json

config = json.loads(open('config.json', 'r').read())


def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER_CURATOR'],
                            password=info['PASSWORD_CURATOR'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


def alreadyInDB(dif_id):
    with open("bin/old_dif_test.txt") as f:
        existing = f.read().splitlines()
    return (dif_id in existing)


def updateExistingRecord(dif_id, usap_flag, nsf_flag):
    conn, cur = connect_to_db()
    query = """UPDATE dif_test SET (is_usap_dc, is_nsf) = (%s, %s) WHERE dif_id = '%s';""" % (usap_flag, nsf_flag, dif_id)
    print(query)
    cur.execute(query)
    cur.execute("COMMIT;")


def parse_xml(xml_file_name):
    conn, cur = connect_to_db()

    tree = ET.parse(xml_file_name)
    root = tree.getroot()

    count = 0
    for result in root.iter('result'):
        for dif_node in list(result):
            dif_id = ''
            version = 'NULL'
            name = ''
            usap_flag = False
            nsf_flag = False

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

            # check if id is already in the DB, if so, just update is_usap_dc, is_nsf, and summary
            if alreadyInDB(name):
                print('FOUND %s' % name)
                count += 1
                usap_flag = True
                nsf_flag = True
                updateExistingRecord(name, usap_flag, nsf_flag)
            elif alreadyInDB(dif_id):
                print('FOUND %s' % dif_id)
                count += 1
                usap_flag = True
                nsf_flag = True
                updateExistingRecord(dif_id, usap_flag, nsf_flag)
            else:
                updateExistingRecord(dif_id, usap_flag, nsf_flag)

    print(count)


if __name__ == '__main__':
    parse_xml('inc/amd_us_2018_07_09_all.xml')
    print('done')
