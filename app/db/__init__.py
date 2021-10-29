import dataclasses
import logging
import typing

import pymongo
import pymongo.database

from app.entities import ConditionEnums, Entry, Group

from .connector import DBConnector

logger = logging.getLogger()


class DBException(Exception):
    """"""


class DBManagerNotInitializedExeption(DBException):
    """"""


@dataclasses.dataclass
class GroupData:
    id: str
    name: str
    enabled: bool
    description: str


@dataclasses.dataclass
class EntryData:
    id: str
    pvname: str
    emails: typing.List[str]
    condition: str
    alarm_values: str
    unit: str
    warning_message: str
    subject: str
    email_timeout: float
    group: str


class DBManager:
    CONDITIONS_COLLECTION = "conditions"
    GROUPS_COLLECTION = "groups"
    ENTRIES_COLLECTION = "entries"

    def __init__(self, db):
        self.db: typing.Optional[pymongo.database.Database] = db

    def _parse_group(self, data: typing.Any) -> GroupData:
        return GroupData(
            description=data["description"],
            enabled=bool(data["enabled"]),
            id=str(data["_id"]),
            name=data["name"],
        )

    def _parse_entry(self, data: typing.Any) -> EntryData:
        id = str(data["id"])
        del data["id"]

        return EntryData(id=id, **data)

    def get_entries(self) -> typing.List[EntryData]:
        if not self.db:
            raise DBManagerNotInitializedExeption()

        entries: pymongo.collection.Collection = self.db[DBManager.ENTRIES_COLLECTION]
        return [self._parse_entry(e) for e in entries.find()]

    def get_group(self, group_name: str) -> typing.Optional[GroupData]:
        if not self.db:
            raise DBManagerNotInitializedExeption()

        groups: pymongo.collection.Collection = self.db[DBManager.GROUPS_COLLECTION]

        return self._parse_group(groups.find_one({"name": group_name}))

    def get_condition(self, name: str):
        if not self.db:
            raise DBManagerNotInitializedExeption()

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
            result = conditions.insert_many(ConditionEnums.get_conditions())

            logger.info(f"Inserted {result.inserted_ids}")
        except Exception:
            logger.exception("Conditions were not initialized")


def make_db(url: str = "mongodb://localhost:27017/", db_name: str = "mailpy-db"):
    db = DBConnector(url, db_name).connect()
    return DBManager(db)
