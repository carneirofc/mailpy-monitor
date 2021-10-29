#!/usr/bin/env python
import logging
import typing

import pymongo
import pymongo.collection
import pymongo.database

logger = logging.getLogger("DB")


class DBConnector:
    def __init__(self, url: str, db_name: str):
        self.url = url
        self.client: typing.Optional[pymongo.MongoClient] = None
        self.db_name = db_name
        self.db: typing.Optional[pymongo.database.Database] = None

    def disconnect(self):
        """Disconnect"""
        if self.client:
            self.client.close()

    def connect(self) -> pymongo.database.Database:
        """Estabilish mongo connection"""
        self.client = pymongo.MongoClient(self.url)
        self.db = self.client[self.db_name]
        return self.db
