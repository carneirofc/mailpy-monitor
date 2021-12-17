#!/bin/bash
set -exu
collections="conditions groups entries events"

CONNECTION_OPTS="authSource=admin"
MONGODB_CONNECTION_STRING="mongodb://${MONGO_INITDB_ROOT_USERNAME}:${MONGO_INITDB_ROOT_PASSWORD}@localhost:27017/${MONGO_INITDB_DATABASE}?${CONNECTION_OPTS}"

for collection in ${collections}; do
    mongoimport \
        ${MONGODB_CONNECTION_STRING} \
        --collection ${collection} \
        /mailpy-db-init-data/${collection}.json \
        --jsonArray
done
