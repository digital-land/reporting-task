#!/bin/bash
set -e

make all -j 8

# if bucket provided sync to reporting folder of that bucket
if [ ! -z "$COLLECTION_DATA_BUCKET" ]; then
    aws s3 sync ./reporting/ s3://$BUCKET/reporting/ --acl public-read
else
    echo "COLLECTION_DATA_BUCKET not provided, skipping sync to S3"
fi