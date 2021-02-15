#!/bin/bash

# Script to upload large bagged datasets up to Amazon S3 bucket and chcek ETags
# This script should be run on seafloor-ph (or similar) in the location of the large datasets.

# To run: >scripts/upload_to_s3.sh <dataset_id>  <upload>

set -euo pipefail
if [ $# -ne 2 ]; then
    echo "Usage: $0 dataset_id upload";
    exit 0;
fi


dsid=$1
echo "$dsid"
upload=$2

if [[ $upload == 'true' ]]
then
    echo "SYNCING TO AWS BUCKET"
    aws s3 sync ready_for_upload s3://archive-usap-dc/large_datasets
fi

echo "CHECKING ETAGS"
s3_etag=$(aws s3api head-object --bucket archive-usap-dc --key large_datasets/${dsid}_bag.tar.gz | awk '/ETag/ {gsub(/\\\"/," "); print $3}')
echo "S3 ETAG: $s3_etag" 

MB=$((1024**2))
GB=$((1024**3))
MAX_PARTS=10000
FILENAME=ready_for_upload/${dsid}_bag.tar.gz
FILESIZE=$(stat -c%s "$FILENAME")
FILESIZE_GB=$(($FILESIZE/GB))
echo "Size of $dsid = $FILESIZE_GB GB."
if [[ $FILESIZE -lt 8*$MB*$MAX_PARTS ]] 
then
    chunk_size=8
elif [[ $FILESIZE -lt 16*$MB*$MAX_PARTS ]] 
then
    chunk_size=16
elif [[ $FILESIZE -lt 32*$MB*$MAX_PARTS ]] 
then
    chunk_size=32
elif [[ $FILESIZE -lt 64*$MB*$MAX_PARTS ]] 
then
    chunk_size=64
elif [[ $FILESIZE -lt 128*$MB*$MAX_PARTS ]] 
then
    chunk_size=128
elif [[ $FILESIZE -lt 256*$MB*$MAX_PARTS ]] 
then
    chunk_size=256
elif [[ $FILESIZE -lt 512*$MB*$MAX_PARTS ]] 
then
    chunk_size=512
elif [[ $FILESIZE -lt 1024*$MB*$MAX_PARTS ]] 
then
    chunk_size=1024
elif [[ $FILESIZE -lt 2048*$MB*$MAX_PARTS ]] 
then
    chunk_size=2048
else
    chunk_size=4096
fi
echo "Chunk size = $chunk_size"

local_etag=$(scripts/s3etag.sh  $FILENAME $chunk_size | awk '{print $2}')
echo "LOCAL ETAG: $local_etag" 
if [ "$s3_etag" == "$local_etag" ] 
    then
        echo "ETAGS MATCH"
        echo "DONE"
    else
        echo "ETAGS DO NOT MATCH"
fi