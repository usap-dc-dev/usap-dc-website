#!/opt/rh/python27/root/usr/bin/python

"""
This script will go through all the files on the local disk in the
directory /web/usap-dc/htdocs/dataset and extract file sizes, mime types
and document types for each one. It will examine the contents of tarred and
zipped files.  The results will be written to the dataset_file table in the 
database.

Run from bin directory with >python get_dataset_files <arg>
<arg> can be: 
  - an individual dataset id: to run for a single dataset
  - all: run for all datasets
  - fix: run only for datasets returned by a specific query determined in the code.
"""

import os
import psycopg2
import psycopg2.extras
import sys
import json
import tarfile
import gzip
import zipfile
import mimetypes
from subprocess import Popen, PIPE, check_output
import shutil
import py7zlib


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
                rel_name = path_name.replace(dir_full, '')
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
                        print("Couldn't open tar.Z file %s\n" % name)
                        print(err)

                elif '.tar' in name or name.endswith('.tgz'):
                    try:
                        with tarfile.open(path_name) as archive:
                            for member in archive:
                                if member.isreg():
                                    # print(member.name)
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
                                                    if member2.name.lower().endswith('.tar') or member2.name.endswith('.tar.gz'):
                                                        archive2.extract(member2.name, path='tmp2')
                                                        with tarfile.open(os.path.join('tmp2', member2.name)) as archive3:
                                                            for member3 in archive3:
                                                                if member3.isreg():
                                                                    mime_type, doc_type = getMimeAndDocTypes(member3.name, os.path.join('tmp2', member3.name), cur, None, archive3)
                                                                    mime_types.add(mime_type)
                                                                    doc_types.add(doc_type)
                                                        shutil.rmtree('tmp2', ignore_errors=True)
                                                    else:
                                                        mime_type, doc_type = getMimeAndDocTypes(member2.name, os.path.join('tmp', member2.name), cur, None, archive2)
                                                        mime_types.add(mime_type)
                                                        doc_types.add(doc_type)
                                        shutil.rmtree('tmp', ignore_errors=True)
                                    else:
                                        mime_type, doc_type = getMimeAndDocTypes(member.name, path_name, cur, None, archive)
                                        mime_types.add(mime_type)
                                        doc_types.add(doc_type)
                    except Exception as err:
                        doc_types.add('Unknown')
                        print("Couldn't open tar file %s\n" % name)
                        print(err)

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

                            elif '.zip' in z.filename:
                                zp.extract(z.filename, 'tmp')
                                zp2 = zipfile.ZipFile(os.path.join('tmp', z.filename))
                                for z2 in zp2.filelist:
                                    if not z2.filename.endswith('/'):
                                        mime_type, doc_type = getMimeAndDocTypes(z2.filename, os.path.join('tmp', z.filename), cur, zp2)
                                        mime_types.add(mime_type)
                                        doc_types.add(doc_type)
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
                cur.execute(sql_line, (ds_id, dir_name, rel_name))
                data = cur.fetchall()

                mime_str = "; ".join([m for m in mime_types if m])
                doc_str = "; ".join([d for d in doc_types if d])

                # print(data)
                # if no data returned use insert otherwise update
                if not data:
                    sql_line = """INSERT INTO dataset_file 
                        (dataset_id, dir_name, file_name, file_size, file_size_uncompressed, sha256_checksum, md5_checksum, mime_types, document_types) VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""
                    cur.execute(sql_line, (ds_id, dir_name, rel_name, file_size, u_file_size, checksum, checksum_md5, mime_str, doc_str))
                else:
                    sql_line = """ UPDATE dataset_file SET 
                       file_size = %s, file_size_uncompressed = %s, sha256_checksum = %s, 
                       md5_checksum = %s, mime_types = %s, document_types = %s
                       WHERE dataset_id = %s AND file_name = %s AND dir_name = %s;"""

                    cur.execute(sql_line, (file_size, u_file_size, checksum, checksum_md5, mime_str, doc_str, ds_id, rel_name, dir_name))   
        # Make the changes to the database persistent
        conn.commit()
        cur.close()
        conn.close()
        print("Database updated for dataset %s" % ds_id)

    else:
        print(ds_id, ' external data', url)


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
    if doc_type == 'Unknown':
        print('%s - unknown' % name)
    if doc_type == 'ZIP Archive':
        print('%s - %s - zip' % (path_name, name))
        # sys.exit()
    # print('%s - %s' %(name, doc_type))
    return mime_type, doc_type


# main
ds_id = sys.argv[1]

(conn, cur) = connect_to_db()

if ds_id == 'all':
    cur.execute("SELECT id, url FROM dataset where url IS NOT NULL ORDER BY id;")
elif ds_id == 'fix':
    cur.execute("SELECT DISTINCT(id), url FROM dataset JOIN dataset_file df ON dataset.id=df.dataset_id WHERE document_types ~* 'tar' ORDER BY id;")
else:
    cur.execute("SELECT id, url FROM dataset where id ='%s';" % ds_id)

data = cur.fetchall()
for ds in data:
    get_file_info(ds['id'], ds['url'])