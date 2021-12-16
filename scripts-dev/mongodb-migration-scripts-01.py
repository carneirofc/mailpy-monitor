#!/usr/bin/env python3
import argparse
import copy

from mailpy.db import DBManager
from mailpy.db.connector import DBConnector

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Migration scripts for mongodb")
    parser.add_argument(
        "--url",
        default="mongodb://test:test@localhost:27017/mailpy",
        help="mongodb connection string",
        dest="url",
    )
    url = parser.parse_args().url
    print(url)
    connector = DBConnector(url=url)
    connector.connect()

    db = connector.db
    entries_collection = db.get_collection(DBManager.ENTRIES_COLLECTION)
    groups_collection = db.get_collection(DBManager.GROUPS_COLLECTION)

    count = 0
    for e in entries_collection.find():
        new_e = copy.deepcopy(e)

        if "group_id" not in e:
            group = groups_collection.find_one({"name": e["group"]})
            new_e["group_id"] = group["_id"]

            entries_collection.update_one(
                {"_id": new_e["_id"]}, {"$set": {"group_id": new_e["group_id"]}}
            )
            count = count + 1
            print(
                f'update entry {new_e["_id"]}, inset field \'group_id\' = {new_e["group_id"]}'
            )
    print(f"Update count {count}")
