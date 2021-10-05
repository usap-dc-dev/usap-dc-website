#!/opt/rh/python27/root/usr/bin/python

"""
Created on 
file_count.py

@author: fnitsche & nshane
run from main usap directory with >python bin/file_count.py
"""

import os
# import requests
import psycopg2
import psycopg2.extras
import sys
import json
import tarfile
import gzip
import zipfile
import mimetypes
from subprocess import Popen, PIPE, check_output


top_dir = '/web/usap-dc/htdocs/dataset'

config = json.loads(open('/web/usap-dc/htdocs/config.json', 'r').read())


def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER_CURATOR'],
                            password=info['PASSWORD_CURATOR'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


# get uncompressed file size of gzipped file
def get_uncompressed_size(file):
    fileobj = open(file, 'r')
    fileobj.seek(-8, 2)
    crc32 = gzip.read32(fileobj)
    isize = gzip.read32(fileobj)  # may exceed 2GB
    fileobj.close()
    return isize


def get_file_info(ds_id, url):
    (conn, cur) = connect_to_db()  
    dir_name = ''

    if config['USAP_DOMAIN']+'dataset' in url:
        dir_name = url.replace(config['USAP_DOMAIN']+'dataset', '')
        dir_full = top_dir + dir_name
        for root, dirs, files in os.walk(dir_full):
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
                                mime_type, doc_type = getMimeAndDocTypes(file, cur)
                                mime_types.add(mime_type)
                                doc_types.add(doc_type)

                    except Exception as err:
                        doc_types.add('Unknown')
                        print("Couldn't open tar.Z file %s\n" % name)
                        print(err)

                elif '.tar' in name:
                    try:
                        with tarfile.open(path_name) as archive:
                            for member in archive:
                                if member.isreg():
                                    mime_type, doc_type = getMimeAndDocTypes(member.name, cur)
                                    mime_types.add(mime_type)
                                    doc_types.add(doc_type)
                    except Exception as err:
                        doc_types.add('Unknown')
                        print("Couldn't open tar file %s\n" % name)
                        print(err)

                elif '.zip' in name:
                    zp = zipfile.ZipFile(path_name)
                    for z in zp.filelist:
                        mime_type, doc_type = getMimeAndDocTypes(z.filename, cur)
                        mime_types.add(mime_type)
                        doc_types.add(doc_type)
                else:
                    mime_type, doc_type = getMimeAndDocTypes(name, cur)
                    mime_types.add(mime_type)
                    doc_types.add(doc_type)

                file_size = os.path.getsize(os.path.join(root, name))

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
                checksum = output.decode('ascii').split(")= ")[1].replace("\n", "")

                process = Popen(['openssl', 'md5', path_name], stdout=PIPE)
                (output, err) = process.communicate()
                if err:
                    text = "Error calculating MD5 checksum.  %s" % err.decode('ascii')
                    print(text)
                checksum_md5 = output.decode('ascii').split(")= ")[1].replace("\n", "")

                # check if dataset_id already exist in table
                sql_line = "SELECT * "\
                            "FROM dataset_file "\
                            "WHERE dataset_id = %s AND dir_name = %s AND file_name = %s;"
                cur.execute(sql_line, (ds_id, dir_name, name))
                data = cur.fetchall()

                mime_str = "; ".join([m for m in mime_types if m])
                doc_str = "; ".join([d for d in doc_types if d])

                # print(data)
                # if no data returned use insert otherwise update
                if not data:
                    sql_line = """INSERT INTO dataset_file 
                        (dataset_id, dir_name, file_name, file_size, file_size_uncompressed, sha256_checksum, md5_checksum, mime_types, document_types) VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""
                    cur.execute(sql_line, (ds_id, dir_name, name, file_size, u_file_size, checksum, checksum_md5, mime_str, doc_str))
                else:
                    sql_line = """ UPDATE dataset_file SET 
                       file_size = %s, file_size_uncompressed = %s, sha256_checksum = %s, 
                       md5_checksum = %s, mime_types = %s, document_types = %s
                       WHERE dataset_id = %s AND dir_name = %s AND file_name = %s;"""

                    cur.execute(sql_line, (file_size, u_file_size, checksum, checksum_md5, mime_str, doc_str, ds_id, dir_name, name))   
        # Make the changes to the database persistent
        conn.commit()
        cur.close()
        conn.close()
        print("Database updated for dataset %s" % ds_id)

    else:
        print(ds_id, ' external data', url)


def getMimeAndDocTypes(name, cur):
    mime_type = mimetypes.guess_type(name)[0]
    if not mime_type:
        ext = '.' + name.split('.')[-1]
        if ext == '.old':
            ext = '.' + name.split('.')[-2]
        cur.execute("SELECT * FROM file_types WHERE extension = %s", (ext.lower(),))
    else:
        cur.execute("SELECT * FROM file_types WHERE mime_type = %s", (mime_type,))
    res = cur.fetchone()

    if res: 
        doc_type = res['document_type']
    else:
        doc_type = 'Unknown'

    return mime_type, doc_type


# main
ds_id = sys.argv[1]

(conn, cur) = connect_to_db()

if ds_id == 'all':
    cur.execute("SELECT id, url FROM dataset where url IS NOT NULL ORDER BY id;")
elif ds_id == 'fix':
    cur.execute("SELECT DISTINCT(id), url FROM dataset JOIN dataset_file df ON dataset.id=df.dataset_id WHERE document_types ~* 'Unknown' ;")
else:
    cur.execute("SELECT id, url FROM dataset where id ='%s';" % ds_id)

data = cur.fetchall()
for ds in data:
    get_file_info(ds['id'], ds['url'])