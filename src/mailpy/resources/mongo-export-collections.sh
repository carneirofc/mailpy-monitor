#!/bin/bash
set -ex
CONNECTION_STR="mongodb://user:password@host:port/db_name"
COLLECTIONS=$(mongo ${CONNECTION_STR} --quiet --eval "db.getCollectionNames()" | sed 's/,/ /g; s/"//g; s/\]//g; s/\[//g' )

for collection in $COLLECTIONS; do
    echo "Exporting $collection ..."
    mongoexport ${CONNECTION_STR} -c $collection -o $collection.json --pretty --jsonArray
done
