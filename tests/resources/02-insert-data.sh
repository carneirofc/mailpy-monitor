#!/bin/bash
set -exu
collections="conditions groups entries"

CONNECTION_OPTS="authSource=admin"
for collection in ${collections}; do
    mongoimport \
        mongodb://${MONGO_INITDB_ROOT_USERNAME}:${MONGO_INITDB_ROOT_PASSWORD}@localhost:27017/${MONGO_INITDB_DATABASE}?${CONNECTION_OPTS} \
        --collection ${collection} \
        /mailpy-db-init-data/${collection}.json
done
