#!/opt/rh/python27/root/usr/bin/python

"""
Author: Neville Shane
Institution: LDEO, Columbia University
Email: nshane@ldeo.columbia.edu

Usage: python bin/archiveUSAPDC.py -r <root_dir> -p <file_path_relative_to_root_path> -o <out_dir_relative_to_root_path> -i <id of dataset>
Inputs:
   
    root_dir: the root directory.  The locations of the dataset file directory and the output directory
              are given relative to this
    file_path: the file path for the directory containing the submission files. 
    out_dir: path of the output directory, relative to the root dir. The bagit packages will go here.
    id: uid of dataset
e.g.:
    python bin/archiveUSAPDC.py -r /web/usap-dc/htdocs -p dataset -o archive -i 700078

"""
import sys
import os
import getopt
import json
import hashlib
import shutil
import bagit
import tarfile
from time import gmtime, strftime
import subprocess
import psycopg2
import psycopg2.extras
import xml.dom.minidom as minidom

config = json.loads(open('config.json', 'r').read())

try:
    import boto3
except:
    print("ERROR: Unable to import boto3.  Check that ~/.aws/configuration is present and correct.")
    sys.exit(0)

# get the base_dir, JSON file, the path to the submission files,
# and the output directory from the input arguments
out_dir = None
root_dir = None
file_path = None

usage = "python archiveUSAPDC.py -r <root_dir> -p <file_path_relative_to_root_path> -o <out_dir_relative_to_root_path> -i <id of dataset>"

""" 
AWS Bucket name
BUCKET_NAME = "XXX"
"""

try:
    opts, args = getopt.getopt(sys.argv[1:], "h:r:p:o:i:", ["root_dir=", "file_path=", "out_dir=", "id="])
    if len(opts) == 0:
        print(usage)
        sys.exit(0)
except getopt.GetoptError:
    print(usage)
    sys.exit(0)
for opt, arg in opts:
    if opt == '-h':
        print(usage)
        sys.exit(0)
    elif opt in ('-r', '--root_dir'):
        root_dir = arg
    elif opt in ('-p', '--file_path'):
        file_path = arg
    elif opt in ('-o', '--out_dir'):
        out_dir = arg
    elif opt in ('-i', '--id'):
        ds_id = arg
if root_dir is None or file_path is None or out_dir is None or ds_id is None:
        print(usage)
        sys.exit(0)

out_dir = os.path.join(root_dir, out_dir)
doc_dir = os.path.join(root_dir, 'doc', ds_id)


def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER_CURATOR'],
                            password=info['PASSWORD_CURATOR'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


# Get dataset information from database
conn, cur = connect_to_db()
query = "SELECT ds.title, ds.doi FROM dataset ds WHERE id = '%s'" % ds_id
cur.execute(query)
res = cur.fetchone()
if res and res.get('title') and res.get('doi'):
    ds_title = res['title']
    ds_doi = res['doi']

    print("title:" + res['title'])
    print("doi:" + res['doi'])
else:
    print("ERROR: Unable to find Title and/or DOI for dataset_id = %s" % ds_id)
    sys.exit(0)


# Create Bagit package
bag_name = ds_id
bag_dir = os.path.join(out_dir, bag_name)

if os.path.exists(bag_dir):
    shutil.rmtree(bag_dir)
os.makedirs(bag_dir)

# copy the data files and the metadata file to the bag dir
# Get the data files (content items) from the dataset_file_info table
query = "select dir_name from dataset_file_info where dataset_id ='%s';" % ds_id
cur.execute(query)
res = cur.fetchall()
# res = [{'dir_name': 'usap-dc/700078/2018-07-02T16:53:16.3Z/'}]
# check if dataset contains files in the archive on seafloor
archive = False
for row in res:
    if row['dir_name'][0:7] == "archive":
        print("WARNING: %s contains archived files and needs to be bagged seperately" % ds_id)
        archive = True

for row in res:
    if row.get('dir_name') and row['dir_name'] != "" and row['dir_name'][0:7] != "archive":

        ds_dir = os.path.join(root_dir, file_path, row['dir_name'])
        # check that data dir exists
        try:
            subprocess.check_output(['ls', ds_dir])
        except:
            print("ERROR: data dir %s not found " % (ds_dir))
            shutil.rmtree(bag_dir)
            sys.exit(0)
        
        try:
            shutil.copytree(ds_dir, os.path.join(bag_dir, row['dir_name']))
        except:
            print("ERROR: Unable to copy directory %s to Bagit directory %s." % (ds_dir, bag_dir))
            print(sys.exc_info()[1])
            shutil.rmtree(bag_dir)
            sys.exit(0)

# copy readme files from doc directory to bagit directory
if os.path.exists(doc_dir):
    readme_files = os.listdir(doc_dir)
    for file_name in readme_files:
        full_file_name = os.path.join(doc_dir, file_name)
        if (os.path.isfile(full_file_name)):
            try:
                shutil.copy(full_file_name, bag_dir)
            except:
                print("ERROR: Unable to copy readme file %s to Bagit directory %s." % (full_file_name, bag_dir))
                print(sys.exc_info()[1])
                shutil.rmtree(bag_dir)
                sys.exit(0)

# copy the metadata file to the bagit dir

# Get the DataCite XML metadata file from the database
# query the database to get the XML for the submission ID
try:
    sql_cmd = '''SELECT datacitexml FROM generate_datacite_xml WHERE id='%s';''' % ds_id
    cur.execute(sql_cmd)
    res = cur.fetchone()
    xml = minidom.parseString(res['datacitexml'])
    out_text = xml.toprettyxml().encode('utf-8').strip()
except:
    out_text = "Error running database query. \n%s" % sys.exc_info()[1][0]
    print(out_text)

# write the xml to a file
xml_file = os.path.join(bag_dir, ds_id + ".xml")
with open(xml_file, "w") as myfile:
    myfile.write(out_text)

# add some submission meta for the bagit
if not archive:
    sub_meta = {
        "Source-Organization": "Interdisciplinary Earth Data Alliance (IEDA)",
        "Organization-Address": "Lamont-Doherty Earth Observatory, 61 Route 9W, Palisades, New York 10964 USA",
        "Contact-Email": "info@iedadata.org",
        "Contact-Name": "Data Manager",
        "External-Description": ds_title,  # encode handles special characters
        "External-Identifier": "doi:%s" % ds_doi,
        "Internal-Sender_Description": "United States Antarctic Program Data Center (USAP-DC)"
    }
    bagit.make_bag(bag_dir, sub_meta, checksum=["sha256", "md5"])
    tar_name = "%s_bag.tar.gz" % bag_dir
else:
    tar_name = "%s_need_archived_data.tar.gz" % bag_dir

# tar and zip
with tarfile.open(tar_name, "w:gz") as tar:
    tar.add(bag_dir, arcname=os.path.basename(bag_dir))
shutil.rmtree(bag_dir)


# calculate checksums
if not archive:
    hasher = hashlib.sha256()
    hasher_md5 = hashlib.md5()
    with open(tar_name, 'rb') as afile:
        buf = afile.read()
        hasher.update(buf)
        hasher_md5.update(buf)
        checksum = hasher.hexdigest()
        checksum_md5 = hasher_md5.hexdigest()
        # print("INFO: checksum generated for file %s: %s" % (tar_name, checksum))

  
    # upload to AWS S3
    s3_name = os.path.basename(tar_name)
    try:
        s3 = boto3.client('s3')
        s3.upload_file(tar_name, config['AWS_BUCKET'], s3_name, ExtraArgs={'StorageClass': 'STANDARD_IA'})

        # check MD5
        s3_md5sum = s3.head_object(Bucket=config['AWS_BUCKET'], Key=s3_name)['ETag'][1:-1]
        # print("File MD5: %s\nS3 MD5: %s\n" % (checksum_md5, s3_md5sum))
        if (s3_md5sum != checksum_md5):
            print("ERROR: AWS S3 MD5 checksum does not match for %s.\nFile MD5: %s\nS3 MD5: %s\n"
                  % (tar_name, checksum_md5, s3_md5sum))
            sys.exit(0)

    except:
        print("ERROR: unable to upload file %s to AWS S3" % s3_name)
        print(sys.exc_info()[1])
        sys.exit(0)
    

    # Update Bagit information in database
    bagitDate = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    conn, cur = connect_to_db()
    query = """INSERT INTO dataset_archive (dataset_id, archived_date, bagit_file_name, sha256_checksum, md5_checksum) VALUES ('%s', '%s', '%s', '%s',' %s');""" % (ds_id, bagitDate, os.path.basename(tar_name), checksum, checksum_md5)
    cur.execute(query)
    cur.execute("COMMIT;")

print("SUCCESS: %s successfully archived.\n" % ds_id)
