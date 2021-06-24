#!/usr/bin/env python
import logging
import threading
import typing

import pymongo
import pymongo.collection
import pymongo.database

from . import commons

logger = logging.getLogger("DB")


class DBException(Exception):
    def __init__(self, *args):
        super().__init__(*args)


class DBManager:
    DB_NAME = "mailpy-db"
    CONDITIONS_COLLECTION = "conditions"
    GROUPS_COLLECTION = "groups"
    ENTRIES_COLLECTION = "entries"
    # @todo: Consider adding a USERS_COLLECTION = 'users' to reference users and emails

    def __init__(self, url: str = "mongodb://localhost:27017/"):
        self.url = url
        self.client: typing.Optional[pymongo.MongoClient] = None
        self.mailpy_db: typing.Optional[pymongo.database.Database] = None
        self.lock = threading.RLock()

        self.connect()

    def disconnect(self):
        """Disconnect"""
        if self.client:
            self.client.close()

    def connect(self):
        """Estabilish mongo connection"""
        self.client = pymongo.MongoClient(self.url)
        self.mailpy_db: pymongo.database.Database = self.client[DBManager.DB_NAME]

    def get_entries(self):
        """Return all entries"""
        entries: pymongo.collection.Collection = self.mailpy_db[
            DBManager.ENTRIES_COLLECTION
        ]
        return [e for e in entries.find()]

    def create_entry(self, entry: commons.Entry):
        """Create an entry"""
        if not self.mailpy_db:
            raise RuntimeError("Database not initialised")

        if not entry.group:
            logger.warning(f"Invalid group for entry {entry}")
            return

        if not self.get_group(entry.group.name):
            self.create_group(entry.group)

        if not self.get_condition(entry.condition):
            logger.error(
                "Failed to crate entry {entry}, condition not found at the database"
            )

        entries: pymongo.collection.Collection = self.mailpy_db[
            DBManager.ENTRIES_COLLECTION
        ]

        logger.info(f"insert {entry.as_dict()}")
        result = entries.insert(entry.as_dict())
        logger.info(f"Inserted entry {entry} id {result}")

    def get_group(self, group_name: str) -> typing.Optional[str]:
        if not self.mailpy_db:
            raise RuntimeError("Database not initialised")
        groups: pymongo.collection.Collection = self.mailpy_db[
            DBManager.GROUPS_COLLECTION
        ]
        return groups.find_one({"_id": group_name})

    def create_group(self, group: commons.Group):
        """Create a group"""
        if not self.mailpy_db:
            raise RuntimeError("Database not initialised")
        groups: pymongo.collection.Collection = self.mailpy_db[
            DBManager.GROUPS_COLLECTION
        ]

        if groups.find_one({"name": group.name}):
            logger.warning(f"Group {group} already exists")
            return

        result = groups.insert(group.as_dict())
        logger.info(f"Inserted group {group} id {result}")

    def get_condition(self, name: str):
        """Get a condtion by name"""
        if not self.mailpy_db:
            raise RuntimeError("Database not initialised")

        conditions: pymongo.collection.Collection = self.mailpy_db[
            DBManager.CONDITIONS_COLLECTION
        ]
        return conditions.find_one({"name": name})

    def initialize_conditions(self):
        """Initialize the conditions collection using the supported ones from commons.Condition"""
        if not self.mailpy_db:
            raise RuntimeError("Database not initialised")

        self.mailpy_db.drop_collection(DBManager.CONDITIONS_COLLECTION)

        conditions: pymongo.collection.Collection = self.mailpy_db[
            DBManager.CONDITIONS_COLLECTION
        ]
        result = conditions.insert_many(commons.Condition.get_conditions())

        logger.info(f"Inserted {result.inserted_ids}")
