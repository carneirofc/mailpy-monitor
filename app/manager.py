import concurrent.futures
import logging
import queue
import threading
import time
import typing

import app.entities as entities
import app.helpers as helpers
from app.db import make_db

from .sms import MailService

logger = logging.getLogger("Manager")

SMS_MAX_QUEUE_SIZE = 50000


class Manager:
    def __init__(self, tls: bool, login: str, passwd: str, db_url: str):

        self.sms_queue: queue.Queue = queue.Queue(maxsize=SMS_MAX_QUEUE_SIZE)

        self.entries: typing.Dict[str, entities.Entry] = {}
        self.groups: typing.Dict[str, entities.Group] = {}
        self.tick: float = 15
        self.running: bool = True

        self.db = make_db(url=db_url)
        self.sms = MailService(login=login, passwd=passwd, tls=tls)

        self.main_thread_executor_workers = 1
        self.main_thread_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.main_thread_executor_workers,
            thread_name_prefix="SMSAction",
        )
        self.tick_thread = threading.Thread(
            daemon=False, target=self.do_tick, name="SMS Tick"
        )

    def load_from_database(self):
        """Load entries from database"""
        dict_entries = self.db.get_entries()
        for d_entry in dict_entries:

            # Create group if needed
            group_name = d_entry["group"]
            if group_name not in self.groups:
                self.groups[group_name] = entities.Group(name=group_name, enabled=True)
                logger.info(f"Creating group {self.groups[group_name]}")

            try:
                _id = d_entry["_id"]
                d_entry.pop(
                    "group", None
                )  # Remove the group name as we are using the object

                entry = entities.Entry(
                    group=self.groups[group_name], sms_queue=self.sms_queue, **d_entry
                )
                entry.connect()
                self.entries[_id] = entry
                logger.info(f"Creating entry {self.entries[_id]}")
            except helpers.EntryException:
                logger.exception("Failed to create entry")

    def send_email(self, event: entities.EmailEvent):
        self.sms.send_email(event)

    def start(self):
        # self.main_thread.start()
        for _ in range(self.main_thread_executor_workers):
            self.main_thread_executor.submit(self.consume_alarm_events)
        self.main_thread_executor.shutdown(wait=False)

        self.tick_thread.start()

    def join(self):
        self.tick_thread.join()

    def do_tick(self):
        """Trigger Entry processing"""
        while self.running:
            time.sleep(self.tick)
            for entry in self.entries.values():
                if not self.running:
                    break
                entry.trigger()

    def consume_alarm_events(self):
        """
        Application loop: Check email targets
        """

        while self.running:
            event = self.sms_queue.get(block=True, timeout=None)

            if type(event) != entities.EmailEvent:
                logger.warning(f"Unknown event type {event} obtained from queue.")
                continue

            # Send an email
            self.send_email(event)
