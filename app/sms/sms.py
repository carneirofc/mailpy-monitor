#!/usr/bin/env python3
import logging
import smtplib
import ssl
import typing
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import app.entities as entities

from .message import compose_msg_content

logger = logging.getLogger("SMS")


class SMSException(Exception):
    def __init__(self, *args):
        super().__init__(*args)


class MailService:
    TLS_PORT = 587
    SSL_PORT = 465

    def __init__(self, login: str, passwd: str, debug_level=2, tls=True):
        self._login = login
        self._passwd = passwd
        self._tls = tls
        self._host = "smtp.gmail.com"
        self._port = self.TLS_PORT if tls else self.SSL_PORT
        self._ssl_context = ssl.create_default_context()
        self._server: typing.Optional[smtplib.SMTP] = None
        self._debug_level = debug_level

    def _create_server_tls(self):
        self._port = self.TLS_PORT
        self._server = smtplib.SMTP(context=self._ssl_context)
        self._server.ehlo()
        self._server.starttls(context=self._ssl_context)
        self._server.ehlo()

    def _create_server_ssl(self):
        self._port = self.SSL_PORT
        self._server = smtplib.SMTP_SSL(context=self._ssl_context)

    def _create_server(self):
        if self._tls:
            self._create_server_tls()
        else:
            self._create_server_ssl()

        self._server.set_debuglevel(self._debuglevel)
        (code, msg) = self._server.connect(host=self._host, port=self._port)
        logger.info(f"SMTP server connected with code {code} {msg}")

    def _disconnect(self):
        if not self._server:
            return

        self._server.quit()
        self._server.close()
        self._server = None

    def _authenticate(self):
        self._create_server()
        logger.info(f"Trying to authenticate at Gmail with {self._login}")

        self._server.login(self._login, self._passwd)
        logger.info(f"logged successfully with {self._login}")

    def _compose_msg(self, event: entities.EmailEvent) -> MIMEMultipart:
        """
        :param commons.EmailEvent event: Message content
        :return: body of the e-mail that will be sent
        """
        msg = MIMEMultipart("alternative")
        msg["From"] = self._login
        msg["To"] = ", ".join(event.emails)
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

    def send_email(self, event: entities.EmailEvent):
        try:
            self._authenticate()

            if not self._server:
                logger.error(
                    f"Failed to send email event {event}, SMTP server '{self._server}'is not connected"
                )
                return

            self._server.sendmail(
                self._login, event.emails, self._compose_msg(event).as_string()
            )
            logger.info(f"Email sent with success, event {event}")

        except Exception as e:
            logger.exception(f"Error from the sms module {e}")

        finally:
            self._disconnect()
