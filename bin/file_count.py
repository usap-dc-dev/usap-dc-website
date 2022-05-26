#!/usr/bin/python3

"""
Created on 
file_count.py

@author: fnitsche & nshane
run from main usap directory with >python bin/file_count.py
"""

import os
import psycopg2
import psycopg2.extras
import json
import tarfile
import gzip
import zipfile
import mimetypes
import struct


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
def get_uncompressed_size(filename):
    with open(filename, 'rb') as f:
        f.seek(-4, 2)
        return struct.unpack('I', f.read(4))[0]


def usap_get_url_list_test():
    data_list = []
    (conn, cur) = connect_to_db()
    cur.execute("SELECT id, url FROM dataset where url IS NOT NULL;")
    data = cur.fetchall()

    print(('records: ', len(data)))
    for row in list(data):
        data_list.append(row)
        # print(row)
    # print(data)
    
    cur.close()
    conn.close()

    return data_list


def get_dir_info(topdir):
    # print()
    # print(topdir)
    file_count = 0
    subdir_count = -1  # because root is counted as one as well
    file_size = 0
    u_file_size = 0
    mime_types = set()
    for root, dirs, files in os.walk(topdir):
        subdir_count += 1
        for name in files:
            # print(name)
            path_name = os.path.join(root, name)
            if '.tar' in name:
                # print(path_name)
                try:
                    with tarfile.open(path_name) as archive:
                        count = 0
                        for member in archive:
                            if member.isreg():
                                count += 1
                                mime_types.add(mimetypes.guess_type(member.name)[0])
                        # count = sum(1 for member in archive if member.isreg())
                        file_count += count
                except:
                    print(("Couldn't open tar file %s\n" %name))
                    file_count += 1
            else:
                file_count += 1
                mime_types.add(mimetypes.guess_type(name)[0])
            
            this_file_size = os.path.getsize(os.path.join(root, name))
            file_size += this_file_size

            # if file is zipped, get  the uncompressed file size too
            if '.gz' in name :
                u_file_size += get_uncompressed_size(path_name)
            elif '.zip' in name:
                zp = zipfile.ZipFile(path_name)
                u_file_size += sum(zinfo.file_size for zinfo in  zp.filelist)
            else:
                u_file_size += this_file_size    
            
            
        # for name in dirs:
        #    print(os.path.join(root, name))
        # if os.path.isdir(name):
        #    print( "is os")
        # dir_list.append("jj")
    # print("-> subdirs = ", subdir_count)
    # print("-> files = ", file_count)
    # print("-> total size = ", file_size)
    # print("u_file_size: ", u_file_size)
    # print(mime_types)
    mime_types.discard(None)    
    return file_count, file_size, u_file_size, list(mime_types)


def get_dataset_info(in_list):
    result_list = []
    for row in in_list:
        # print(row['id'], row['url'])
        dataset_id = row['id']
        dir_name = ''
        if 'https://www.usap-dc.org/dataset' in row['url']:
            dir_name = row['url'].replace('https://www.usap-dc.org/dataset/', '')
            dir_full = os.path.join(top_dir, dir_name)
            # dir_full = 'usap/601011'
            # print(dir_full)
            (file_count, file_size, u_file_size, mime_types) = get_dir_info(dir_full)
            result_list.append([dataset_id, dir_name, file_count, file_size, u_file_size, mime_types])
        else:
            print((dataset_id, ' external data', row['id']))
    
    return result_list


def lst2pgarr(alist):
    print(alist)
    if len(alist) == 0:
        return None
    return '{' + ','.join(alist) + '}'


def update_db(data_list):
    '''
    link to database and create or update data_file_info table
    '''
    (conn, cur) = connect_to_db()

    for row in data_list:
        # print(row)
        # check if dataset_id already exist in table
        sql_line = "Select * "\
                    "From dataset_file_info "\
                    "WHERE dataset_id = %s AND dir_name = %s;"
        # print(sql_line)
        cur.execute(sql_line, (row[0], row[1]))
        data = cur.fetchall()
        # print(data)
        # if no data returned use insert otherwise update
        if not data:
            print("--> insert ", row[0])
            sql_line = """INSERT INTO dataset_file_info 
                       (dataset_id, dir_name, file_count, file_size_on_disk, file_size_uncompressed, mime_types) VALUES
                       (%s, %s, %s, %s, %s, %s);"""
            cur.execute(sql_line, (row[0], row[1], row[2], row[3], row[4], row[5]))

        else:
            print("update ", row[0])
            sql_line = """UPDATE dataset_file_info SET 
                       file_count = %s, file_size_on_disk = %s, file_size_uncompressed = %s, mime_types = %s
                       WHERE dataset_id = %s AND dir_name = %s;"""
                      
            cur.execute(sql_line, (row[2], row[3], row[4], row[5], row[0], row[1]))
    # Make the changes to the database persistent
    conn.commit()
    cur.close()
    conn.close()

    return
    
    
# main
url_list = usap_get_url_list_test()
results_list = get_dataset_info(url_list)
update_db(results_list)
