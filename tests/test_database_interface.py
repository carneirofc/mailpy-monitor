import unittest

from app.db import make_db, DBManager


class TestMongodbInterface(unittest.TestCase):
    test_db_url = "mongodb://localhost:27017/"
    test_db_name = "mailpy-db"
    db: DBManager

    def setUp(self):
        self.db: DBManager = make_db(url=self.test_db_name, name=self.test_db_name)
