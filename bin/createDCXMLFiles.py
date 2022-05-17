# Creates a DataCite XML file from a uid and puts it in watch directory
# To run in production environment for one dataset:
# >sudo -E LD_LIBRARY_PATH=/usr/lib64 /usr/bin/python3 createDCXMLFiles.py <uid>
# To run for all datasets in the database:
# >sudo -E LD_LIBRARY_PATH=/usr/lib64 /usr/bin/python3 createDCXMLFiles.py all

import sys
import os
from lib.curatorFunctions import getDataCiteXML, connect_to_db, getDCXMLFileName
import xml.dom.minidom as minidom


def getAll():
    (conn, cur) = connect_to_db()
    if type(conn) is str:
        out_text = conn
    else:
        # query the database to get the XML for the submission ID
        sql_cmd = '''SELECT id, datacitexml FROM generate_datacite_xml;'''
        print(sql_cmd)
        cur.execute(sql_cmd)
        results = cur.fetchall()

        for res in results:
            uid = res['id']
            xml = minidom.parseString(res['datacitexml'])
            out_text = xml.toprettyxml().encode('utf-8').strip()
            # write the xml to a file
            xml_file = getDCXMLFileName(uid)
            with open(xml_file, "w") as myfile:
                myfile.write(out_text)
            os.chmod(xml_file, 0o664)
            print(xml_file)
        return("COMPLETED")

    return(out_text)


if __name__ == '__main__':
    uid = sys.argv[1]

    if uid == "all":
        print(getAll())
    else:
        print(getDataCiteXML(uid))