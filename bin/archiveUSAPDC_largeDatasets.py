"""
Cut down version of archiveUSAPDC.py that will create the tarred, gzipped bagit file,
calculate the checksums and update the database_archive table, setting a status of Ready For Upload.

Before running, need to run the original version of archiveUSAPDC.py on the USAP-DC server
to create a tarred and gzipped directory containing the data stored on that server, plus the
DataCite XML file.  That file then needs to be copied over to seafloor /archive/usap-dc/dataset.
Then unzip and untar the file and copy the large dataset that is currently stored in 
/archive/usap-dc/dataset in to the directory.  Running the prepare_for_upload.sh Bash script will
do all this.

Need to run on a system with the /archive/usap-dc/dataset directory mounted, but that has python 2.7 or 3
and the bagit package installed.

It is reccommended that this script be run as part of the prepare_for_upload.sh Bash script.
"""

import os
import sys
import json
import shutil
import bagit
import tarfile
from time import gmtime, strftime
from subprocess import Popen, PIPE
import psycopg2
import psycopg2.extras
import zipfile
import struct
import mimetypes


ROOT_DIR = "/archive/usap-dc/dataset"


config = json.loads(open('scripts/config.json', 'r').read())

dir_in = sys.argv[1]
print(dir_in)
if 'part' in dir_in:
    ds_id = dir_in.split('_')[0]
else:
    ds_id = dir_in
upload = sys.argv[2] in ['True', 'true', 'TRUE', 't', 'T']

print("STARTING PYTHON SCRIPT")


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


def get_file_info(dataset_dir):
    mime_types = set()
    doc_types = set()

    for root, dirs, files in os.walk(dataset_dir):
        for name in files:
            namel = name.lower()
            path_name = os.path.join(root, name)
            if namel.endswith('.tar.z'):
                try:
                    process = Popen(('zcat', path_name), stdout=PIPE)
                    output = check_output(('tar', 'tf', '-'), stdin=process.stdout)
                    files = output.split('\n')
                    for file in files:
                        if file != '':
                            mime_type, doc_type = getMimeAndDocTypes(file, path_name)
                            mime_types.add(mime_type)
                            doc_types.add(doc_type)

                except Exception as err:
                    doc_types.add('Unknown')
                    sql_out += "--ERROR: Couldn't open tar.Z file %s\n" % name

            elif namel.endswith('.tar') or namel.endswith('tar.gz') or namel.endswith('.tgz'):
                try:
                    with tarfile.open(path_name) as archive:
                        for member in archive:
                            if member.isreg():
                                mnamel = member.name.lower()
                                if mnamel.endswith('.zip'):
                                    archive.extract(member.name, path='tmp')
                                    zp = zipfile.ZipFile(os.path.join('tmp', member.name))
                                    for z in zp.filelist:
                                        if not z.filename.endswith('/'):
                                            mime_type, doc_type = getMimeAndDocTypes(z.filename, os.path.join('tmp', member.name), zp)
                                            mime_types.add(mime_type)
                                            doc_types.add(doc_type)
                                    shutil.rmtree('tmp', ignore_errors=True)

                                elif mnamel.endswith('.tar.z'):
                                    archive.extract(member.name, path='tmp')
                                    try:
                                        process = Popen(('zcat', os.path.join('tmp', member.name)), stdout=PIPE)
                                        output = check_output(('tar', 'tf', '-'), stdin=process.stdout)
                                        files = output.split('\n')
                                        for file in files:
                                            # print(file)
                                            if file != '':
                                                mime_type, doc_type = getMimeAndDocTypes(file, os.path.join('tmp', member.name))
                                                mime_types.add(mime_type)
                                                doc_types.add(doc_type)

                                    except Exception as err:
                                        doc_types.add('Unknown')
                                        print("Couldn't open tar.Z file %s\n" % member.name)
                                        print(err)

                                elif mnamel.endswith('.tar') or mnamel.endswith('.tar.gz') or mnamel.endswith('.tgz'):
                                    archive.extract(member.name, path='tmp')
                                    if os.path.basename(member.name).startswith('._'):
                                        continue
                                    
                                    with tarfile.open(os.path.join('tmp', member.name)) as archive2:
                                        for member2 in archive2:
                                            if member2.isreg():
                                                mime_type, doc_type = getMimeAndDocTypes(member2.name, os.path.join('tmp', member2.name), None, archive2)
                                                mime_types.add(mime_type)
                                                doc_types.add(doc_type)
                                    shutil.rmtree('tmp', ignore_errors=True)
                                else:
                                    mime_type, doc_type = getMimeAndDocTypes(member.name, path_name, None, archive)
                                    mime_types.add(mime_type)
                                    doc_types.add(doc_type)
                except Execption as err:
                    print("Error getting file types.  %s" % err.decode('ascii'))
                    sys.exit(0)

            elif namel.endswith('zip'):
                zp = zipfile.ZipFile(path_name)
                for z in zp.filelist:
                    if not z.filename.endswith('/'):
                        znamel = z.filename.lower()
                        if znamel.endswith('.tar') or znamel.endswith('.tar.gz') or znamel.endswith('.tgz'):
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
                            mime_type, doc_type = getMimeAndDocTypes(z.filename, path_name, zp)
                            mime_types.add(mime_type)
                            doc_types.add(doc_type)
            
            elif namel.endswith('.7z'):
                with open(path_name) as fp:
                    archive = py7zlib.Archive7z(fp)
                    for z in archive.getnames():
                        if not z.endswith('/'):
                            mime_type, doc_type = getMimeAndDocTypes(z, path_name)
                            mime_types.add(mime_type)
                            doc_types.add(doc_type)
            else:
                mime_type, doc_type = getMimeAndDocTypes(path_name, path_name)
                mime_types.add(mime_type)
                doc_types.add(doc_type)

    return mime_types, doc_types


def getMimeAndDocTypes(name, path_name, zp=None, tar_archive=None):
    (conn, cur) = connect_to_db()
    mime_type = mimetypes.guess_type(name)[0]
    ext = '.' + name.lower().split('.')[-1]

    if ext in ['.old', '.gz']:
        ext = '.' + name.lower().split('.')[-2]
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
            else:
                print(e)

    if doc_type == 'Unknown':
        print('Error getting file types.  %s - unknown' % name)
        sys.exit(0)

    return mime_type, doc_type


# Get file types for all files in dataset before bagging and zipping
print("GET FILE TYPES FOR ALL FILES IN DATASET")
mime_types, doc_types = get_file_info(os.path.join(ROOT_DIR, dir_in))

# Get dataset information from database
conn, cur = connect_to_db()
query = "SELECT ds.title, ds.doi FROM dataset ds WHERE id = '%s'" % ds_id
cur.execute(query)
res = cur.fetchone()
ds_title = res['title'] if res.get('title') else 'Not Available'
ds_doi = res['doi'] if res.get('doi') else 'Not Available'
cur.close()


bag_dir = os.path.join(ROOT_DIR, dir_in)

sub_meta = {
    "Source-Organization": "United States Antarctic Program Data Center (USAP-DC)",
    "Organization-Address": "Lamont-Doherty Earth Observatory, 61 Route 9W, Palisades, New York 10964 USA",
    "Contact-Email": "info@usap-dc.org",
    "Contact-Name": "Data Manager",
    "External-Description": ds_title,  # encode handles special characters
    "External-Identifier": "doi:%s" % ds_doi,
    "Internal-Sender_Description": "see dataCite Record in payload"
}
print("MAKING BAG")
bagit.make_bag(bag_dir, sub_meta, checksum=["sha256", "md5"])
tar_name = "%s_bag.tar.gz" % bag_dir

# tar and zip
print("TARRING AND ZIPPING")
with tarfile.open(tar_name, "w:gz") as tar:
   tar.add(bag_dir, arcname=os.path.basename(bag_dir))
shutil.rmtree(bag_dir)
print("BAGGIT MADE")

# calculate checksums (hashlib libarry won't work on large files, so need to use openssl in unix)
print("CALCULATING SHA256")
process = Popen(['openssl', 'sha256', tar_name], stdout=PIPE)
(output, err) = process.communicate()
if err:
    print("Error calculating SHA256 checksum.  %s" % err.decode('ascii'))
    sys.exit(0)
with open(tar_name + ".sha256", "w") as myfile:
    checksum = output.decode('ascii').split(")= ")[1].replace("\n", "")
    myfile.write(checksum)

print("CALCULATING MD5")
process = Popen(['openssl', 'md5', tar_name], stdout=PIPE)
(output, err) = process.communicate()
if err:
    print("Error calculating MD5 checksum.  %s" % err.decode('ascii'))
    sys.exit(0)
with open(tar_name + ".md5", "w") as myfile:
    checksum_md5 = output.decode('ascii').split(")= ")[1].replace("\n", "")
    myfile.write(checksum_md5)


# calculate compressed and uncompressed file size
btgz_name = dir_in + '_bag.tar.gz'
print("CALCULATING FILE SIZES")
file_size = os.path.getsize(os.path.join(ROOT_DIR, btgz_name))
u_file_size = get_uncompressed_size(os.path.join(ROOT_DIR, btgz_name))

print("MOVING TO ready_for_upload")
os.system('mv %s* ready_for_upload/' % btgz_name)


# update dataset_file table
print("UPDATING DATABASE - DATASET_FILE TABLE")
# check if dataset_id already exist in table
(conn, cur) = connect_to_db()
sql_line = "SELECT * "\
            "FROM dataset_file "\
            "WHERE dataset_id = %s AND dir_name = '/archive/usap-dc/dataset/large_datasets' AND file_name = %s;"
cur.execute(sql_line, (ds_id, btgz_name))
data = cur.fetchall()

mime_str = "; ".join([m for m in mime_types if m])
doc_str = "; ".join([d for d in doc_types if d])

# if no data returned use insert otherwise update
if not data:
    sql_line = """INSERT INTO dataset_file 
        (dataset_id, dir_name, file_name, file_size, file_size_uncompressed, sha256_checksum, md5_checksum, mime_types, document_types) VALUES
        (%s, '/archive/usap-dc/dataset/large_datasets', %s, %s, %s, %s, %s, %s, %s);"""
    cur.execute(sql_line, (ds_id, btgz_name, file_size, u_file_size, checksum, checksum_md5, mime_str, doc_str))
else:
    sql_line = """ UPDATE dataset_file SET 
        file_size = %s, file_size_uncompressed = %s, sha256_checksum = %s, 
        md5_checksum = %s, mime_types = %s, document_types = %s
        WHERE dataset_id = %s AND dir_name = '/archive/usap-dc/dataset/large_datasets' AND file_name = %s;"""
    cur.execute(sql_line, (file_size, u_file_size, checksum, checksum_md5, mime_str, doc_str, ds_id, btgz_name))  



# Print psql query
print("UPDATING DATABASE - READY FOR UPLOAD")
query = "SELECT * FROM dataset_archive where dataset_id = '%s' AND (bagit_file_name = 'large_datasets/%s' OR bagit_file_name IS NULL);" % (ds_id,  os.path.basename(tar_name))
cur.execute(query)
res = cur.fetchall()
if len(res) > 0:
    query = """UPDATE dataset_archive SET (sha256_checksum, md5_checksum, status) = ('%s', '%s', 'Ready For Upload')
               WHERE dataset_id = '%s' AND bagit_file_name = 'large_datasets/%s';""" % (checksum, checksum_md5, ds_id, os.path.basename(tar_name))
else:
    query = """INSERT INTO dataset_archive (dataset_id, bagit_file_name, sha256_checksum, md5_checksum, status) 
               VALUES ('%s', 'large_datasets/%s', '%s', '%s', 'Ready For Upload');""" % (ds_id, os.path.basename(tar_name), checksum, checksum_md5)
cur.execute(query)
cur.execute("COMMIT;")
cur.close()

# if we are using this script to upload to AWS run this section
if upload:
    print("UPLOADING TO AMAZON")
    process = Popen(['scripts/upload_to_s3.sh', dir_in, 'true'], stdout=PIPE)
    (output, err) = process.communicate()
    if err:
        print("Error uploading to Amazon.  %s" % err.decode('ascii'))
        sys.exit(0)
    if 'ETAGS DO NOT MATCH' in output.decode('ascii'):
        print("Error with Amazon ETag.  %s" % output.decode('ascii'))
        sys.exit(0)

    print(output.decode('ascii'))

    print("UPDATING DATABASE - ARCHIVED")
    bagitDate = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    conn, cur = connect_to_db()
    query = "SELECT * FROM dataset_archive where dataset_id = '%s' AND (bagit_file_name = 'large_datasets/%s' OR bagit_file_name IS NULL);" % (ds_id,  os.path.basename(tar_name))
    cur.execute(query)
    res = cur.fetchall()
    if len(res) > 0:
        query = """UPDATE dataset_archive SET (archived_date, status) = ('%s', 'Archived')
                WHERE dataset_id = '%s' AND bagit_file_name = 'large_datasets/%s';""" % (bagitDate, ds_id, os.path.basename(tar_name))
    else:
        query = """INSERT INTO dataset_archive (dataset_id, archived_date, bagit_file_name, sha256_checksum, md5_checksum, status) 
                VALUES ('%s', '%s', 'large_datasets/%s', '%s', '%s', 'Archived');""" % (ds_id, bagitDate, os.path.basename(tar_name), checksum, checksum_md5)
    cur.execute(query)
    cur.execute("COMMIT;")
    cur.close()



print("PYTHON SCRIPT DONE")
