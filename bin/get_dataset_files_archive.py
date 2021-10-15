#!/usr/bin/python3

"""
This version to be run on the archive on seafloor-ph to get dataset_file info for the zipped bag files

Run from scripts directory with >python3 get_dataset_files_archive.py <dataset_id>

Will update the dataset_file table
"""

import os
import psycopg2
import psycopg2.extras
import sys
import json
import tarfile
import struct
import mimetypes
import shutil
import zipfile
from subprocess import Popen, PIPE, check_output

top_dir = '/archive/usap-dc/dataset/large_datasets'

config = json.loads(open('/archive/usap-dc/dataset/scripts/config.json', 'r').read())


def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER'],
                            password=info['PASSWORD'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


# get uncompressed file size of gzipped file
def get_uncompressed_size(filename):
    with open(filename, 'rb') as f:
        f.seek(-4, 2)
        return struct.unpack('I', f.read(4))[0]


def get_file_info(ds_id):
    
    name = '%s_bag.tar.gz' % ds_id
    mime_types = set()
    doc_types = set()
    path_name = os.path.join(top_dir, name)
    
    try:
        with tarfile.open(path_name) as archive:
            for member in archive:
                if member.isreg():
                    mname = member.name
                    print(mname)
                    mpath_name = os.path.join(top_dir, mname)
                    if mname.endswith('.tar.Z'):
                        try:
                            process = Popen(('zcat', mpath_name), stdout=PIPE)
                            output = check_output(('tar', 'tf', '-'), stdin=process.stdout)
                            files = output.split('\n')
                            for file in files:
                                if file != '':
                                    mime_type, doc_type = getMimeAndDocTypes(file, path_name)
                                    mime_types.add(mime_type)
                                    doc_types.add(doc_type)

                        except Exception as err:
                            doc_types.add('Unknown')
                            print("Couldn't open tar.Z file %s\n" % mname)
                            print(err)

                    elif '.tar' in mname or name.endswith('.tgz'):
                        try:
                            archive.extract(mname, path='tmp')
                            with tarfile.open(os.path.join('tmp', mname)) as archive2:
                                for member2 in archive2:
                                    if member2.isreg():
                                        print(member2.name)

                                        if '.zip' in member2.name:
                                            archive.extract(member2.name, path='tmp')
                                            zp = zipfile.ZipFile(os.path.join('tmp', member2.name))
                                            for z in zp.filelist:
                                                if not z.filename.endswith('/'):
                                                    mime_type, doc_type = getMimeAndDocTypes(z.filename, os.path.join('tmp', member2.name), zp)
                                                    mime_types.add(mime_type)
                                                    doc_types.add(doc_type)
                                            shutil.rmtree('tmp', ignore_errors=True)

                                        elif member2.name.endswith('.tar.Z'):
                                            archive.extract(member2.name, path='tmp')
                                            try:
                                                process = Popen(('zcat', os.path.join('tmp', member2.name)), stdout=PIPE)
                                                output = check_output(('tar', 'tf', '-'), stdin=process.stdout)
                                                files = output.split('\n')
                                                for file in files:
                                                    # print(file)
                                                    if file != '':
                                                        mime_type, doc_type = getMimeAndDocTypes(file, os.path.join('tmp', member2.name))
                                                        mime_types.add(mime_type)
                                                        doc_types.add(doc_type)

                                            except Exception as err:
                                                doc_types.add('Unknown')
                                                print("Couldn't open tar.Z file %s\n" % member2.name)
                                                print(err)

                                        elif '.tar' in member2.name.lower() or member2.name.endswith('.tgz'):
                                            archive.extract(member2.name, path='tmp')
                                            if os.path.basename(member2.name).startswith('._'):
                                                continue
                                            
                                            with tarfile.open(os.path.join('tmp', member2.name)) as archive3:
                                                for member3 in archive3:
                                                    if member3.isreg():
                                                        mime_type, doc_type = getMimeAndDocTypes(member3.name, os.path.join('tmp', member3.name), None, archive3)
                                                        mime_types.add(mime_type)
                                                        doc_types.add(doc_type)
                                            shutil.rmtree('tmp', ignore_errors=True)
                                        else:
                                            mime_type, doc_type = getMimeAndDocTypes(member2.name, os.path.join('tmp', member2.name), None, archive2)
                                            mime_types.add(mime_type)
                                            doc_types.add(doc_type)
                        except Exception as err:
                            doc_types.add('Unknown')
                            print("Couldn't open tar file %s\n" % mname)
                            print(err)

                    elif '.zip' in mname:
                        archive.extract(mname, path='tmp')
                        zp = zipfile.ZipFile(os.path.join('tmp', mname))
                        for z in zp.filelist:
                            if not z.filename.endswith('/'):
                                print(z.filename)
                                if z.filename.endswith('.tar') or z.filename.endswith('.tar.gz') or z.filename.endswith('.tgz'):
                                    if z.filename.startswith('__MACOSX'):
                                        continue
                                    zp.extract(z.filename, 'tmp')
                                    with tarfile.open(os.path.join('tmp', z.filename)) as archive2:
                                        for member2 in archive2:
                                            if member2.isreg():
                                                mime_type, doc_type = getMimeAndDocTypes(member2.name, os.path.join('tmp', member2.name), None, archive2)
                                                mime_types.add(mime_type)
                                                doc_types.add(doc_type)
                                    shutil.rmtree('tmp', ignore_errors=True)
                                else:                                
                                    mime_type, doc_type = getMimeAndDocTypes(z.filename, mpath_name, zp)
                                    mime_types.add(mime_type)
                                    doc_types.add(doc_type)
                            shutil.rmtree('tmp', ignore_errors=True)
                        

                    elif mname.endswith('.7z'):
                        with open(mpath_name) as fp:
                            archive = py7zlib.Archive7z(fp)
                            for z in archive.getnames():
                                if not z.endswith('/'):
                                    mime_type, doc_type = getMimeAndDocTypes(z, mpath_name)
                                    mime_types.add(mime_type)
                                    doc_types.add(doc_type)

                    else:
                        mime_type, doc_type = getMimeAndDocTypes(mpath_name, mpath_name)
                        mime_types.add(mime_type)
                        doc_types.add(doc_type)

    except Exception as err:
        doc_types.add('Unknown')
        print("Couldn't open tar file %s\n" % name)
        print(err)
    
    file_size = os.path.getsize(path_name)

    # if file is zipped, get  the uncompressed file size too
    u_file_size = get_uncompressed_size(path_name)

    with open('%s.sha256' % path_name, 'r') as file:
        checksum = file.read()
    with open('%s.md5' % path_name, 'r') as file:
        checksum_md5 = file.read()

    # check if dataset_id already exist in table
    (conn, cur) = connect_to_db() 
    sql_line = "SELECT * "\
                "FROM dataset_file "\
                "WHERE dataset_id = %s AND dir_name = %s AND file_name = %s;"
    cur.execute(sql_line, (ds_id, top_dir, name))
    data = cur.fetchall()

    mime_str = "; ".join([m for m in mime_types if m])
    doc_str = "; ".join([d for d in doc_types if d])

    # if no data returned use insert otherwise update
    if not data:
        sql_line = """INSERT INTO dataset_file 
            (dataset_id, dir_name, file_name, file_size, file_size_uncompressed, sha256_checksum, md5_checksum, mime_types, document_types) VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        cur.execute(sql_line, (ds_id, top_dir, name, file_size, u_file_size, checksum, checksum_md5, mime_str, doc_str))
    else:
        sql_line = """ UPDATE dataset_file SET 
            file_size = %s, file_size_uncompressed = %s, sha256_checksum = %s, 
            md5_checksum = %s, mime_types = %s, document_types = %s
            WHERE dataset_id = %s AND dir_name = %s AND file_name = %s;"""
        cur.execute(sql_line, (file_size, u_file_size, checksum, checksum_md5, mime_str, doc_str, ds_id, top_dir, name))   
    # Make the changes to the database persistent
    conn.commit()
    cur.close()
    conn.close()
    print("Database updated for dataset %s" % ds_id)


def getMimeAndDocTypes(name, path_name, zp=None, tar_archive=None):
    (conn, cur) = connect_to_db()
    mime_type = mimetypes.guess_type(name)[0]
    ext = '.' + name.split('.')[-1]

    if ext in ['.old', '.gz']:
        ext = '.' + name.split('.')[-2]

    if ext == '.zip':
        print("Contains ZIP file - manually inspect %s to work out doc type" % name)

    cur.execute("SELECT * FROM file_type WHERE extension = %s", (ext.lower(),))
    res = cur.fetchone()
    cur.close()
    conn.close()

    if res:
        doc_type = res['document_type']
    else:
        doc_type = 'Unknown'

    if 'README' in name.upper() and ext != '.pdf':
        doc_type = 'Readme Text File'

    if doc_type == 'Unknown':
        try:
            # determine whether text or binary
            textchars = bytearray({7, 8, 9, 10, 12, 13, 27} | set(range(0x20, 0x100)) - {0x7f})
            is_binary_string = lambda bytes: bool(bytes.translate(None, textchars))
            if zp:
                f = zp.open(name, 'r')
            elif tar_archive:
                tar_archive.extract(name, path='tmp2')
                f = open(os.path.join('tmp2', name), 'rb')
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
                shutil.rmtree('tmp2', ignore_errors=True)

        except Exception as e:
            # for dataset 601323
            if str(e) == 'Bad magic number for file header':
                doc_type = None
            else:
                print(e)

    if doc_type == 'Unknown':
        print('%s - unknown' % name)
    # print('%s - %s' %(name, doc_type))
    return mime_type, doc_type


# main
ds_id = sys.argv[1]
get_file_info(ds_id)
