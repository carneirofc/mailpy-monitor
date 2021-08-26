import logging
import os
import pandas
import pymongo
import typing

import app.entities
import app.db

logger = logging.getLogger()

dbm: app.db.DBManager = None


def connect(url="mongodb://localhost:27017/mailpy-db"):
    global dbm
    if not dbm:
        dbm = app.db.make_db(url=url)


def initialize_conditions():
    """ Initialize the conditions collection using the supported ones from app.entities.Condition """
    global dbm
    if dbm:
        dbm.initialize_conditions()


def create_entry(
    pvname: str,
    emails: str,
    condition: str,
    alarm_values: str,
    unit: str,
    warning_message: str,
    subject: str,
    email_timeout: float,
    group_name: str,
):
    global dbm
    entry: app.entities.Entry = None
    try:
        entry = app.entities.Entry(
            alarm_values=alarm_values,
            condition=condition,
            dummy=True,
            email_timeout=email_timeout,
            emails=emails,
            group=app.entities.Group(name=group_name, enabled=True),
            pvname=pvname,
            sms_queue=None,
            subject=subject,
            unit=unit,
            warning_message=warning_message,
        )
        dbm.create_entry(entry)
        logger.info(f"Creating entry {entry}")

    except pymongo.errors.DuplicateKeyError:
        logger.warn(f"Entry exists {entry}")

    except app.helpers.EntryException:
        logger.exception("Failed to create entry")


def load_csv_table(table: str):
    """ Populate the database from a csv file. Initial migration. """
    if not os.path.isfile(table):
        raise ValueError(f'Failed to load csv data. File "{table}" does not exist')

    df: typing.Optional[pandas.DataFrame] = pandas.read_csv(table)

    # parse other columns
    for _, row in df.iterrows():
        create_entry(
            pvname=row["PV"],
            emails=row["emails"],
            condition=row["condition"].lower().strip(),
            alarm_values=row["specified value"],
            unit=row["measurement unit"],
            warning_message=row["warning message"],
            subject=row["email subject"],
            email_timeout=row["timeout"],
            group_name=row["group"],
        )
