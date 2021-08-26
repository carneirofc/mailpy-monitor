import logging
import typing
import pymongo
import pymongo.database

from .connector import DBConnector
from app.entities import Entry, Group, Condition

logger = logging.getLogger()


class DBManager:
    CONDITIONS_COLLECTION = "conditions"
    GROUPS_COLLECTION = "groups"
    ENTRIES_COLLECTION = "entries"

    def __init__(self, db):
        self.db: typing.Optional[pymongo.database.Database] = db

    def get_entries(self) -> typing.List[Entry]:
        """Return all entries"""
        entries: pymongo.collection.Collection = self.db[DBManager.ENTRIES_COLLECTION]
        return [e for e in entries.find()]

    def create_entry(self, entry: Entry):
        """Create an entry"""
        if not entry.group:
            logger.warning(f"Invalid group for entry {entry}")
            return

        if not self.get_group(entry.group.name):
            self.create_group(entry.group)

        if not self.get_condition(entry.condition):
            logger.error(
                "Failed to crate entry {entry}, condition not found at the database"
            )

        entries: pymongo.collection.Collection = self.db[DBManager.ENTRIES_COLLECTION]

        logger.info(f"insert {entry.as_dict()}")
        result = entries.insert(entry.as_dict())
        logger.info(f"Inserted entry {entry} id {result}")

    def get_group(self, group_name: str) -> typing.Optional[str]:
        groups: pymongo.collection.Collection = self.db[DBManager.GROUPS_COLLECTION]
        return groups.find_one({"_id": group_name})

    def create_group(self, group: Group):
        """Create a group"""
        groups: pymongo.collection.Collection = self.db[DBManager.GROUPS_COLLECTION]

        if groups.find_one({"name": group.name}):
            logger.warning(f"Group {group} already exists")
            return

        result = groups.insert(group.as_dict())
        logger.info(f"Inserted group {group} id {result}")

    def get_condition(self, name: str):
        """Get a condtion by name"""
        conditions: pymongo.collection.Collection = self.db[
            DBManager.CONDITIONS_COLLECTION
        ]
        return conditions.find_one({"name": name})

    def initialize_conditions(self):
        """Initialize the conditions collection using the supported ones from commons.Condition"""
        self.db.drop_collection(DBManager.CONDITIONS_COLLECTION)
        try:
            conditions: pymongo.collection.Collection = self.db[
                DBManager.CONDITIONS_COLLECTION
            ]
            result = conditions.insert_many(Condition.get_conditions())

            logger.info(f"Inserted {result.inserted_ids}")
        except Exception:
            logger.exception("Conditions were not initialized")


def make_db(
    url: str = "mongodb://localhost:27017/", db_name: str = "mailpy-db"
) -> DBManager:
    db = DBConnector(url, db_name).connect()
    dbm = DBManager(db)
    return dbm
