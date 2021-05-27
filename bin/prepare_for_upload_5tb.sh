#!/bin/bash

# Use this version for datasets > 5TB.  First split the dataset up into separate directories that
# are < 5TB each and label them <dataset_id>_archived_data_part1, <dataset_id>_archived_data_part2, etc.

# A Bash script that will prepare large datasets on /archive/usap-dc/dataset for upload to Amazon.
# This script should be run on seafloor-ph (or similar) in the location of the large datasets.

# Before running, need to run the original version of archiveUSAPDC.py on the USAP-DC server
# to create a tarred and gzipped directory containing the data stored on that server, plus the
# DataCite XML file. 

# This script will pull the directory from the USAP-DC server, unzip and untar it, combine with the
# data stored in the archive directory, then run the archiveUSAPDC_largeDatasets.py python script
# which will create the bag files and calculate checksums.  It will update the database_archive table
# setting a status of Ready For Upload.  The bagged datasets will be moved to the ready_for_upload directory.
# If upload is 'true', the python script will also upload to Amazon, check the ETag, and mark the dataset as
# Archived in the database.

# To run: >scripts/prepare_for_upload_5tb.sh <dataset_id> <upload> <num_parts>

set -euo pipefail
if [ $# -ne 3 ]; then
    echo "Usage: $0 dataset_id upload num_parts";
    exit 0;
fi

dsid=$1
upload=$2
num_parts=$3

echo "UNZIPPING DATA FROM WEBSERVER"
gunzip "$dsid"_need_archived_data.tar.gz

for (( part=1; part<=$num_parts; part++ ))
do
    echo "PART $part"

    part_dir="$dsid"_part"$part"

    echo "UNTARRING"
    tar xvf "$dsid"_need_archived_data.tar 
    mv $dsid $part_dir


    echo "MOVING ARCHIVED DATA"
    mkdir -p "$part_dir"/archive/"$dsid"_archived_data
    rsync -a --itemize-changes "$dsid"_archived_data_part"$part"/ "$part_dir"/archive/"$dsid"_archived_data

    echo "RUNNING PYTHON SCRIPT"
    python3 scripts/archiveUSAPDC_largeDatasets.py "$part_dir" "$upload"

    if [[ $upload == 'true' ]]
    then
        echo "MOVING TO large_datasets"
        mv ready_for_upload/"$part_dir"* large_datasets/
    fi
done

rm "$dsid"_need_archived_data.tar

echo "$dsid DONE"






