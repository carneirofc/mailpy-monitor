#!/usr/bin/env python3
import concurrent.futures
import logging
import queue
import smtplib
import ssl
import threading
import time
import typing
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import app.entities as entities
import app.helpers as helpers
from app.db import make_db

from .message import compose_msg_content

logger = logging.getLogger("SMS")


class SMSException(Exception):
    def __init__(self, *args):
        super().__init__(*args)


class SMSApp:
    def __init__(
        self,
        tls: bool,
        login: str,
        passwd: str,
        sms_queue: queue.Queue,
        db_url: str,
    ):
        self.sms_queue = sms_queue

        self.entries: typing.Dict[str, entities.Entry] = {}
        self.groups: typing.Dict[str, entities.Group] = {}
        self.tls: bool = tls
        self.login: str = login
        self.passwd: str = passwd
        self.tick: float = 15
        self.running: bool = True

        self.db = make_db(url=db_url)

        self.main_thread_executor_workers = 1
        self.main_thread_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.main_thread_executor_workers,
            thread_name_prefix="SMSAction",
        )
        self.tick_thread = threading.Thread(
            daemon=False, target=self.do_tick, name="SMS Tick"
        )

    def load_from_database(self):
        """ Load entries from database """
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
                # settings = entities.Entry

                entry = entities.Entry(
                    group=self.groups[group_name], sms_queue=self.sms_queue, **d_entry
                )
                entry.connect()
                self.entries[_id] = entry
                logger.info(f"Creating entry {self.entries[_id]}")
            except helpers.EntryException:
                logger.exception("Failed to create entry")

    def compose_msg(self, event: entities.EmailEvent) -> MIMEMultipart:
        """
        :param commons.EmailEvent event: Message content
        :return: body of the e-mail that will be sent
        """
        msg = MIMEMultipart("alternative")
        msg["From"] = self.login
        msg["To"] = event.emails.replace(";", ", ")
        msg["Cc"] = ""
        msg["Bcc"] = ""
        msg["Subject"] = event.subject

        text, html = compose_msg_content(event)

        text_part = MIMEText(text, "plain")
        html_part = MIMEText(html, "html")
        # add html/plain-text parts to MIMEMultipart message.
        # The email client will try to render the last part first (in this case the html)
        msg.attach(text_part)
        msg.attach(html_part)
        return msg

    def send_email(self, event: entities.EmailEvent, msg: MIMEMultipart):
        """
        Send an email
        :param commons.EmailEvent event: Email content specifics
        :param MIMEMultipart msg: Message content
        """
        email = event.emails.split(";")

        try:
            # Create a secure SSL context
            context = ssl.create_default_context()

            if self.tls:
                # Start an unsecured SMTP connection and then encrypt it.
                # Gmail SMTP server requires a connection with port 587 if using .starttls()
                port = 587
                # Create a secure SSL context
                # try to log in to server and send email
                with smtplib.SMTP("smtp.gmail.com", port) as server:
                    server.ehlo()
                    server.starttls(context=context)
                    server.ehlo()
                    server.login(self.login, self.passwd)
                    server.sendmail(self.login, email, msg.as_string())

            else:
                # Start a secured SMTP connection from the beginning.
                # Gmail SMTP server requires a connection with port 465 if using SMTP_SSL()
                port = 465
                with smtplib.SMTP_SSL(
                    "smtp.gmail.com", port, context=context
                ) as server:
                    server.login(self.login, self.passwd)
                    server.sendmail(self.login, email, msg.as_string())

            logger.info(f"Email sent with success. {email}")
        except ssl.SSLError:
            logger.exception("Error from the underlying SSL implementation")
        except smtplib.SMTPAuthenticationError:
            logger.exception(
                f"Failed to authenticate with username:{self.login} and password: {self.passwd}"
            )
        except smtplib.SMTPException:
            logger.exception("Error from the smtplib module")

    def authenticate_account(self) -> bool:
        """
        Authenticate gmail account
        :return bool: Successfully connected
        """
        logger.info(f"Trying to authenticate at Gmail with {self.login}")

        authentication = False
        try:
            context = ssl.create_default_context()
            while not authentication:
                with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
                    server.login(self.login, self.passwd)
                    authentication = True
                    logger.info(f"logged successfully with {self.login}")
                    return True

        except ssl.SSLError:
            logger.exception("Error from the underlying SSL implementation")
        except smtplib.SMTPAuthenticationError:
            logger.exception(
                f"Failed to authenticate with username:{self.login} and password: {self.passwd}"
            )
        except smtplib.SMTPException:
            logger.exception("Error from the smtplib module")

        self.passwd = None
        return False

    def start(self):
        # self.main_thread.start()
        for _ in range(self.main_thread_executor_workers):
            self.main_thread_executor.submit(self.do_main_action)
        self.main_thread_executor.shutdown(wait=False)

        self.tick_thread.start()

    def join(self):
        self.tick_thread.join()

    def do_tick(self):
        """ Trigger Entry processing """
        while self.running:
            time.sleep(self.tick)
            for entry in self.entries.values():
                if not self.running:
                    break
                entry.trigger()

    def do_main_action(self):
        """
        Application loop: Check email targets
        """
        if not self.authenticate_account():
            logger.fatal(
                "Failed to authenticate gmail mail account. SMS program shut down"
            )
            self.running = False
            return

        while self.running:
            event = self.sms_queue.get(block=True, timeout=None)

            if type(event) == entities.EmailEvent:
                # Send an email
                message = self.compose_msg(event)
                self.send_email(event, message)

            else:
                logger.warning(f"Unknown event type {event} obtained from queue.")
