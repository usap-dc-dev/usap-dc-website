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

ROOT_DIR = "/archive/usap-dc/dataset"


config = json.loads(open('scripts/config.json', 'r').read())

ds_id = sys.argv[1]
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


# Get dataset information from database
conn, cur = connect_to_db()
query = "SELECT ds.title, ds.doi FROM dataset ds WHERE id = '%s'" % ds_id
cur.execute(query)
res = cur.fetchone()
ds_title = res['title'] if res.get('title') else 'Not Available'
ds_doi = res['doi'] if res.get('doi') else 'Not Available'
cur.close()


bag_dir = os.path.join(ROOT_DIR, ds_id)

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

print("MOVING TO ready_for_upload")
os.system('mv %s_bag.tar.gz* ready_for_upload/' % ds_id)


# Print psql query
print("UPDATING DATABASE - READY FOR UPLOAD")
conn, cur = connect_to_db()
query = "SELECT * FROM dataset_archive where dataset_id = '%s';" % ds_id
cur.execute(query)
res = cur.fetchall()
if len(res) > 0:
    query = """UPDATE dataset_archive SET (bagit_file_name, sha256_checksum, md5_checksum, status) = ('large_datasets/%s', '%s', '%s', 'Ready For Upload')
               WHERE dataset_id = '%s';""" % (os.path.basename(tar_name), checksum, checksum_md5, ds_id)
else:
    query = """INSERT INTO dataset_archive (dataset_id, bagit_file_name, sha256_checksum, md5_checksum, status) 
               VALUES ('%s', 'large_datasets/%s', '%s', '%s', 'Ready For Upload');""" % (ds_id, os.path.basename(tar_name), checksum, checksum_md5)
cur.execute(query)
cur.execute("COMMIT;")
cur.close()

# if we are using this script to upload to AWS run this section
if upload:
    print("UPLOADING TO AMAZON")
    process = Popen(['scripts/upload_to_s3.sh', ds_id, 'true'], stdout=PIPE)
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
    query = "SELECT * FROM dataset_archive where dataset_id = '%s';" % ds_id
    cur.execute(query)
    res = cur.fetchall()
    if len(res) > 0:
        query = """UPDATE dataset_archive SET (archived_date, bagit_file_name, sha256_checksum, md5_checksum, status) = ('%s', 'large_datasets/%s', '%s', '%s', 'Archived')
                WHERE dataset_id = '%s';""" % (bagitDate, os.path.basename(tar_name), checksum, checksum_md5, ds_id)
    else:
        query = """INSERT INTO dataset_archive (dataset_id, archived_date, bagit_file_name, sha256_checksum, md5_checksum, status) 
                VALUES ('%s', '%s', 'large_datasets/%s', '%s', '%s', 'Archived');""" % (ds_id, bagitDate, os.path.basename(tar_name), checksum, checksum_md5)
    cur.execute(query)
    cur.execute("COMMIT;")
    cur.close()



print("PYTHON SCRIPT DONE")
