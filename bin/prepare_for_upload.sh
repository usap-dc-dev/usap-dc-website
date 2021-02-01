#!/bin/bash

# A Bash script that will prepare large datasets on /archive/usap-dc/dataset for upload to Amazon.
# This script should be run on seafloor-ph (or similar) in the location of the large datasets.

# Before running, need to run the original version of archiveUSAPDC.py on the USAP-DC server
# to create a tarred and gzipped directory containing the data stored on that server, plus the
# DataCite XML file. 

# This script will pull the directory from the USAP-DC server, unzip and untar it, combine with the
# data stored in the archive directory, then run the archiveUSAPDC_largeDatasets.py python script
# which will create the bag files and calculate checksums.  It will update the database_archive table
# setting a status of Ready For Upload.  The bagged datasets will be moved to the ready_for_upload directory.

# To run: >./prepare_for_upload.sh <dataset_id>

set -euo pipefail
if [ $# -ne 1 ]; then
    echo "Usage: $0 dataset_id";
    exit 0;
fi

dsid=$1

echo "TRANSFERRING DATA FROM USAP-DC Server"
scp nshane@www.usap-dc.org:/web/usap-dc/htdocs/archive/"$dsid"_need_archived_data.tar.gz .

echo "UNZIPPING"
gunzip "$dsid"_need_archived_data.tar.gz

echo "UNTARRING"
tar xvf "$dsid"_need_archived_data.tar
rm "$dsid"_need_archived_data.tar

echo "MOVING ARCHIVED DATA"
mkdir -p "$dsid"/archive/"$dsid"_need_archived_data
rsync -a --itemize-changes "$dsid"_archived_data/ "$dsid"/archive/"$dsid"_archived_data

echo "RUNNING PYTHON SCRIPT"
python3 archiveUSAPDC_largeDatasets.py "$dsid"

echo "MOVING TO ready_for_upload"
mv "$dsid"_bag.tar.gz* ready_for_upload/

echo "DONE"