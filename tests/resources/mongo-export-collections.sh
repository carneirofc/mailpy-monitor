#!/bin/bash
set -ex
CONNECTION_STR="mongodb://mailpyadmin:2021MailPy7391@db:27017/mailpy-db"
COLLECTIONS=$(mongo ${CONNECTION_STR} --quiet --eval "db.getCollectionNames()" | sed 's/,/ /g; s/"//g; s/\]//g; s/\[//g' )

for collection in $COLLECTIONS; do
    echo "Exporting $collection ..."
    mongoexport ${CONNECTION_STR} -c $collection -o $collection.json --pretty
done
