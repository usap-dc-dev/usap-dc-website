"""
Cut down version of archiveUSAPDC.py that will create the tarred, gzipped bagit file and
print out the PSQL command to insert in to the dataset_archive table in the database.

Before running, need to run the original version of archiveUSAPDC.py on the USAP-DC server
to create a tarred and gzipped directory containing the data stored on that server, plus the
DataCite XML file.  That file then needs to be copied over to seafloor /archive/usap-dc/dataset.
Then unzip and untar the file and copy the large dataset that is currently stored in 
/archive/usap-dc/dataset in to the directory.

Update ds_title, ds_doi and ds_id in the code, then run.

Need to run on a system with the /archive/usap-dc/dataset directory mounted, but that has python 2.7 
and the bagit package installed.

If checksum calculation doesn't work, due to memory constraints, try using the unix command:
> openssl sha256 601047_bag.tar.gz
"""

import os
import hashlib
import shutil
import bagit
import tarfile
from time import gmtime, strftime

ROOT_DIR = "/archive/usap-dc/dataset"

ds_title = "Radar Depth Sounder Echograms and Ice Thickness"
ds_doi = "10.15784/601047"
ds_id = "601047"
bag_dir = os.path.join(ROOT_DIR, ds_id)

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

# tar and zip
with tarfile.open(tar_name, "w:gz") as tar:
    tar.add(bag_dir, arcname=os.path.basename(bag_dir))
shutil.rmtree(bag_dir)
print("BAGGIT MADE")

# calculate checksums
hasher = hashlib.sha256()
hasher_md5 = hashlib.md5()
with open(tar_name, 'rb') as afile:
    buf = afile.read()
    hasher.update(buf)
    hasher_md5.update(buf)
    checksum = hasher.hexdigest()
    print("SHA256 checksum: " + checksum)
    checksum_md5 = hasher_md5.hexdigest()
    print("MD5 checksum: " + checksum_md5)

# Print psql query
bagitDate = strftime("%Y-%m-%d %H:%M:%S", gmtime())
query = """INSERT INTO dataset_archive (dataset_id, archived_date, bagit_file_name, sha256_checksum, md5_checksum) VALUES ('%s', '%s', '%s', '%s',' %s');""" % (ds_id, bagitDate, os.path.basename(tar_name), checksum, checksum_md5)
print(query)
