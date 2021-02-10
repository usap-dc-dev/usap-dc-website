#!/opt/rh/python27/root/usr/bin/python

"""
Author: Neville Shane
Institution: LDEO, Columbia University
Email: nshane@ldeo.columbia.edu

Usage: python archiveUSAPDC.py -r <root_dir> -p <file_path_relative_to_root_path> -o <out_dir_relative_to_root_path> -i <id of dataset> -e <email>
Inputs:
   
    root_dir: the root directory.  The locations of the dataset file directory and the output directory
              are given relative to this
    file_path: the file path for the directory containing the submission files. 
    out_dir: path of the output directory, relative to the root dir. The bagit packages will go here.
    id: uid of dataset
    email: if True, will send the output in an email
e.g.:
    python archiveUSAPDC.py -r /web/usap-dc/htdocs -p dataset -o archive -i 700078 false

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
from subprocess import Popen, PIPE
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


config = json.loads(open('../config.json', 'r').read())

try:
    import boto3
    from boto3.s3.transfer import TransferConfig
except:
    print("ERROR: Unable to import boto3.  Check that ~/.aws/configuration is present and correct.")
    sys.exit(0)

# get the base_dir, JSON file, the path to the submission files,
# and the output directory from the input arguments
out_dir = None
root_dir = None
file_path = None
email = False
text = ''

usage = "python archiveUSAPDC.py -r <root_dir> -p <file_path_relative_to_root_path> -o <out_dir_relative_to_root_path> -i <id of dataset> -e <email>"

try:
    opts, args = getopt.getopt(sys.argv[1:], "h:r:p:o:i:e:", ["root_dir=", "file_path=", "out_dir=", "id=", "email="])
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
    elif opt in ('-e', '--email'):
        email = arg == 'True'
if root_dir is None or file_path is None or out_dir is None or ds_id is None or email not in [True, False]:
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


def sendEmail(message, subject):
    sender = config['USAP-DC_GMAIL_ACCT']
    recipients = [config['USAP-DC_GMAIL_ACCT']]

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)

    content = MIMEText(message, 'html', 'utf-8')
    msg.attach(content)

    smtp_details = config['SMTP']
    s = smtplib.SMTP(smtp_details["SERVER"], smtp_details['PORT'].encode('utf-8'))
    # identify ourselves to smtp client
    s.ehlo()
    # secure our email with tls encryption
    s.starttls()
    # re-identify ourselves as an encrypted connection
    s.ehlo()
    s.login(smtp_details["USER"], smtp_details["PASSWORD"])
    s.sendmail(sender, recipients, msg.as_string())
    s.quit()  


# Get dataset information from database
conn, cur = connect_to_db()
query = "SELECT ds.title, ds.doi FROM dataset ds WHERE id = '%s'" % ds_id
cur.execute(query)
res = cur.fetchone()
ds_title = res['title'] if res.get('title') else 'Not Available'
ds_doi = res['doi'] if res.get('doi') else 'Not Available'

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

# check if dataset contains files in the archive on seafloor
archive = False
for row in res:
    if row['dir_name'][0:7] == "archive":
        text += "WARNING: %s contains archived files and needs to be bagged seperately\n" % ds_id
        print(text)
        archive = True

for row in res:
    if row.get('dir_name') and row['dir_name'] != "" and row['dir_name'][0:7] != "archive":

        ds_dir = os.path.join(root_dir, file_path, row['dir_name'])
        # check that data dir exists
        try:
            subprocess.check_output(['ls', ds_dir])
        except:
            text += "ERROR: data dir %s not found " % (ds_dir)
            print(text)
            shutil.rmtree(bag_dir)
            if email: 
                sendEmail(text, 'Unsuccessful Dataset Archive: %s' % ds_id)
            sys.exit(0)
        
        try:
            shutil.copytree(ds_dir, os.path.join(bag_dir, row['dir_name']))
        except:
            text += "ERROR: Unable to copy directory %s to Bagit directory %s.\n%s" % (ds_dir, bag_dir, sys.exc_info()[1])
            print(text)
            shutil.rmtree(bag_dir)
            if email: 
                sendEmail(text, 'Unsuccessful Dataset Archive: %s' % ds_id)
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
                text += "ERROR: Unable to copy readme file %s to Bagit directory %s.\n%s" % (full_file_name, bag_dir, sys.exc_info()[1])
                print(text)
                if email: 
                    sendEmail(text, 'Unsuccessful Dataset Archive: %s' % ds_id)
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
    text += out_text
    print(out_text)

# write the xml to a file
xml_file = os.path.join(bag_dir, ds_id + ".xml")
with open(xml_file, "w") as myfile:
    myfile.write(out_text)

# add some submission meta for the bagit
if not archive:
    sub_meta = {
        "Source-Organization": "United States Antarctic Program Data Center (USAP-DC)",
        "Organization-Address": "Lamont-Doherty Earth Observatory, 61 Route 9W, Palisades, New York 10964 USA",
        "Contact-Email": "info@usap-dc.org",
        "Contact-Name": "Data Manager",
        "External-Description": ds_title,  # encode handles special characters
        "External-Identifier": "doi:%s" % ds_doi,
        "Internal-Sender_Description": "see dataCite Record in payload"
    }
    bagit.make_bag(bag_dir, sub_meta, checksum=["sha256", "md5"])
    tar_name = "%s_bag.tar.gz" % bag_dir
else:
    tar_name = "%s_need_archived_data.tar.gz" % bag_dir

# tar and zip
with tarfile.open(tar_name, "w:gz") as tar:
    tar.add(bag_dir, arcname=os.path.basename(bag_dir))
shutil.rmtree(bag_dir)

# archive=False
#tar_name = "%s_bag.tar.gz" % bag_dir

# calculate checksums
if not archive:
    
    # calculate checksums (hashlib library won't work on large files, so need to use openssl in unix)
    process = Popen(['openssl', 'sha256', tar_name], stdout=PIPE)
    (output, err) = process.communicate()
    if err:
        text += "Error calculating SHA256 checksum.  %s" % err.decode('ascii')
        print(text)
        if email: 
            sendEmail(text, 'Unsuccessful Dataset Archive: %s' % ds_id)
        sys.exit(0)
    checksum = output.decode('ascii').split(")= ")[1].replace("\n", "")

    process = Popen(['openssl', 'md5', tar_name], stdout=PIPE)
    (output, err) = process.communicate()
    if err:
        text += "Error calculating MD5 checksum.  %s" % err.decode('ascii')
        print(text)
        if email: 
            sendEmail(text, 'Unsuccessful Dataset Archive: %s' % ds_id)
        sys.exit(0)
    checksum_md5 = output.decode('ascii').split(")= ")[1].replace("\n", "")

  
    # upload to AWS S3
    s3_name = config['AWS_FOLDER'] + os.path.basename(tar_name)
    try:
        s3 = boto3.client('s3')

        # Ensure multipart uploading is used for files larger than 5TB (which is the max allowed single part
        # transfer size)
        GB = 1024 ** 3
        tc = TransferConfig(multipart_threshold=5*GB)
        s3.upload_file(tar_name, config['AWS_BUCKET'], s3_name, ExtraArgs={'StorageClass': 'STANDARD_IA'}, Config=tc)

        # check MD5
        s3_md5sum = s3.head_object(Bucket=config['AWS_BUCKET'], Key=s3_name)['ETag'][1:-1]

        # If S3 uses multipart uploading, the 'ETag' in the S3 file header will no longer
        # be the MD5 checksum. Use the s3etag.sh script to find what the ETag value should be.
        if os.path.getsize(tar_name) > 5*GB:
            process = Popen(['./s3etag.sh', tar_name, '8'], stdout=PIPE)
            (output, err) = process.communicate()
            if err:
                text += "Error calculating predicted ETag value.  %s" % err.decode('ascii')
                print(text)
                if email: 
                    sendEmail(text, 'Unsuccessful Dataset Archive: %s' % ds_id)
                sys.exit(0)
            etag = output.decode('ascii').split()[1]
            if (s3_md5sum != etag):
                text += "ERROR: AWS S3 ETag does not match for %s.\nFile ETag: %s\nS3 ETag: %s\n" % (tar_name, etag, s3_md5sum)
                print(text)
                if email: 
                    sendEmail(text, 'Unsuccessful Dataset Archive: %s' % ds_id)
                sys.exit(0)
        else:
            # print("File MD5: %s\nS3 MD5: %s\n" % (checksum_md5, s3_md5sum))
            if (s3_md5sum != checksum_md5):
                text += "ERROR: AWS S3 MD5 checksum does not match for %s.\nFile MD5: %s\nS3 MD5: %s\n" % (tar_name, checksum_md5, s3_md5sum)
                print(text)
                if email: 
                    sendEmail(text, 'Unsuccessful Dataset Archive: %s' % ds_id)
                sys.exit(0)

    except:
        text += "ERROR: unable to upload file %s to AWS S3\n%s" % (tar_name, sys.exc_info()[1])
        print(text)
        if email: 
            sendEmail(text, 'Unsuccessful Dataset Archive: %s' % ds_id)
        sys.exit(0)
    

    # Update Bagit information in database
    bagitDate = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    try:
        conn, cur = connect_to_db()
        query = "SELECT * FROM dataset_archive where dataset_id = '%s';" % ds_id
        cur.execute(query)
        res = cur.fetchall()
        if len(res) > 0:
            query = """UPDATE dataset_archive SET (archived_date, bagit_file_name, sha256_checksum, md5_checksum, status) = ('%s', 'small_datasets/%s', '%s', '%s', 'Archived')
                    WHERE dataset_id = '%s';""" % (bagitDate, os.path.basename(tar_name), checksum, checksum_md5, ds_id)
        else:
            query = """INSERT INTO dataset_archive (dataset_id, archived_date, bagit_file_name, sha256_checksum, md5_checksum, status) 
                    VALUES ('%s', '%s', 'small_datasets/%s', '%s', '%s', 'Archived');""" % (ds_id, bagitDate, os.path.basename(tar_name), checksum, checksum_md5)
        cur.execute(query)
        cur.execute("COMMIT;")
    except:
        text += "Error connecting to database. \n%s" % sys.exc_info()[1][0]
        print(text)
        if email: 
            sendEmail(text, 'Unsuccessful Dataset Archive: %s' % ds_id)
        sys.exit(0)

    
    # Copy bag file to /archive/usap-dc/small_datasets
    try:
        remotedir = os.path.join(config['LOCAL_ARCHIVE_DIR'],'small_datasets')
        remotefile = os.path.join(remotedir, os.path.basename(tar_name)) 
        os.system('rsync "%s" "%s:%s"' % (tar_name, config['LOCAL_ARCHIVE_SERVER'], remotefile))
    except:
        text += "Error transferring bagged dataset to local server. \n%s" % sys.exc_info()[1][0]
        print(text)
        if email: 
            sendEmail(text, 'Unsuccessful Dataset Archive: %s' % ds_id)
        sys.exit(0)

    # Delete bag file
    os.remove(tar_name)


    text += "SUCCESS: %s successfully archived.\n" % ds_id
    print(text)
    if email: 
        sendEmail(text, 'Successful Dataset Archive: %s' % ds_id)

else:
    # Copy bag file to /archive/usap-dc
    try:
        remotefile = os.path.join(config['LOCAL_ARCHIVE_DIR'], os.path.basename(tar_name)) 
        os.system('rsync "%s" "%s:%s"' % (tar_name, config['LOCAL_ARCHIVE_SERVER'], remotefile))
    except:
        text += "Error transferring bagged dataset to local server. \n%s" % sys.exc_info()[1][0]
        print(text)
        if email: 
            sendEmail(text, 'Unsuccessful Dataset Archive: %s' % ds_id)
        sys.exit(0)

    # Update database
    try:
        conn, cur = connect_to_db()
        query = "SELECT * FROM dataset_archive where dataset_id = '%s';" % ds_id
        cur.execute(query)
        res = cur.fetchall()
        if len(res) > 0:
            query = """UPDATE dataset_archive SET status = 'Large Dataset Awaiting Completion' WHERE dataset_id = '%s';""" % (ds_id)
        else:
            query = """INSERT INTO dataset_archive (dataset_id, status) VALUES ('%s', 'Large Dataset Awaiting Completion');""" % (ds_id)
        cur.execute(query)
        cur.execute("COMMIT;")
    except:
        text += "Error connecting to database. \n%s" % sys.exc_info()[1][0]
        print(text)
        if email: 
            sendEmail(text, 'Unsuccessful Dataset Archive: %s' % ds_id)
        sys.exit(0)

    # Delete bag file
    os.remove(tar_name)

    text = "%s is a large dataset.  Please go to the local archive server and run 'scripts/prepare_for_upload.sh %s true' to complete the archiving process." %(ds_id, ds_id)
    print(text)
    if email: 
        sendEmail(text, 'Large Dataset Archive Needs To Be Completed: %s' % ds_id)




