#!/usr/bin/env python3
import dataclasses
import smtplib
import typing
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import mailpy.entities as entities
import mailpy.logging as logging

from ..utils import check_required_fields
from .message import compose_msg_content

logger = logging.getLogger()


class Settings:
    GMAIL_TLS_PORT = 587
    GMAIL_SSL_PORT = 465
    GMAIL_HOSTNAME = "smtp.gmail.com"

    CNPEM_TLS_PORT = 25
    CNPEM_HOSTNAME = "mail.cnpem.br"


class SMSException(Exception):
    def __init__(self, *args):
        super().__init__(*args)


@dataclasses.dataclass
class MailClientArgs:
    login: str
    passwd: str
    port: int
    host: str
    tls: bool
    debug_level: int = 0


class MailClient:
    REQUIRED_FIELDS = [
        "_login",
        "_passwd",
        "_tls",
        "_host",
    ]

    def __init__(
        self,
        args: MailClientArgs,
        debug_level: int = 1,
    ):

        self._login: str = args.login
        self._passwd: str = args.passwd
        self._tls: bool = args.tls
        self._host: str = args.host
        self._port: int = args.port

        self._server: typing.Optional[smtplib.SMTP] = None
        self._debug_level = debug_level

        check_required_fields(self, self.REQUIRED_FIELDS)

        self._check_edge_cases()

    def _check_edge_cases(self):
        if (
            self._tls and self._host == Settings.GMAIL_HOSTNAME
        ):  # edge cases, consider moving this ...
            raise ValueError(
                f"tls is not supported for host '{self._host}' '{self._port}'"
            )
        elif (
            not self._tls
            and self._host == Settings.CNPEM_HOSTNAME
            or self._host == Settings.CNPEM_HOSTNAME
            and self._port != Settings.CNPEM_TLS_PORT
        ):
            raise ValueError(f"CNPEM requires tls and port {Settings.CNPEM_TLS_PORT}")

    def __enter__(self):
        self._authenticate()
        return self

    def __exit__(self, *args, **kwargs):
        self._disconnect()

    def _create_server_tls(self):
        self._server = smtplib.SMTP(self._host, timeout=10)
        response = self._server.connect(host=self._host, port=self._port)
        self._server.ehlo()
        self._server.starttls()
        self._server.ehlo()
        return response

    def _create_server_ssl(self):
        self._server = smtplib.SMTP_SSL(self._host)
        return self._server.connect(host=self._host, port=self._port)

    def _create_server(self):
        if self._tls:
            (code, msg) = self._create_server_tls()
        else:
            (code, msg) = self._create_server_ssl()

        self._server.set_debuglevel(self._debug_level)
        logger.info(f"SMTP server connected with code {code} {msg}")

    def _disconnect(self):
        if not self._server:
            return

        self._server.quit()
        self._server.close()
        self._server = None

    def _authenticate(self):
        self._create_server()
        if not self._passwd:
            logger.debug("Empty password ignoring login step")
            return

        logger.info(f"Trying to authenticate at Gmail with {self._login}")
        self._server.login(self._login, self._passwd)
        logger.info(f"logged successfully with {self._login}")

    def _compose_msg(self, event: entities.AlarmEvent) -> MIMEMultipart:
        """
        :return: body of the e-mail that will be sent
        """
        msg = MIMEMultipart("alternative")
        msg["From"] = self._login
        msg["To"] = ", ".join(event.emails)
        msg["Cc"] = ""
        msg["Bcc"] = ""
        msg["Subject"] = event.subject

        content = compose_msg_content(event)

        text_part = MIMEText(content.text, "plain")
        html_part = MIMEText(content.html, "html")
        # The email client will try to render the last part first (in this case the html)
        msg.attach(text_part)
        msg.attach(html_part)
        return msg

    def send_email(self, event: entities.AlarmEvent):
        if not self._server:
            logger.error(
                f"Failed to send email event {event}, SMTP server '{self._server}'is not connected"
            )
            return

        response = self._server.sendmail(
            from_addr=self._login,
            to_addrs=event.emails,
            msg=self._compose_msg(event).as_string(),
        )
        logger.info(f"Email sent with success, event {event} {response}")
