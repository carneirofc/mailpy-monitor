import logging
import multiprocessing
import queue
import threading
import time
import typing

import epics

from app.helpers import EntryException

from .Condition import (
    ConditionCheckResponse,
    ConditionException,
    create_condition,
    Condition,
)
from .EmailEvent import EmailEvent
from .Group import Group

logger = logging.getLogger()


class DummyPV:
    def __init__(self, pvname):
        self.pvname = pvname

    @property
    def value(self):
        return None

    def __str__(self):
        return f"<DummyPV pvname={self.pvname}>"


class EntrySettings(typing.NamedTuple):
    id: str
    pvname: str
    emails: str
    condition: str
    alarm_values: str
    unit: str
    warning_message: str
    subject: str
    email_timeout: float
    group: Group


class Entry:
    """
    Encapsulates a PV and the email logic associated with it
    :dummy bool: signal a dummy entry, used for tests and utility scripts.
    """

    def __init__(
        self,
        id: str,
        pvname: str,
        emails: str,
        condition: str,
        alarm_values: str,
        unit: str,
        warning_message: str,
        subject: str,
        email_timeout: float,
        group: Group,
        sms_queue: multiprocessing.Queue,
        dummy: bool = False,
        settings: EntrySettings = None,
    ):
        self._id = id
        self._pvname = pvname
        self._pv: typing.Optional[epics.PV] = None

        self._condition: Condition = create_condition(
            condition=condition.lower().strip(), alarm_values=alarm_values
        )

        self._dummy = dummy
        self._pvname = pvname.strip()
        self._sms_queue_dispatch_lock = threading.RLock()

        self.email_timeout = email_timeout
        self.emails = emails
        self.group = group
        self.sms_queue = sms_queue
        self.subject = subject
        self.unit = unit
        self.warning_message = warning_message

        # reset last_event_time for all PVs, so it start monitoring right away
        self.last_event_time = time.time() - self.email_timeout

        self._pv = DummyPV(pvname=self.pvname)

    def connect(self):
        if not self._dummy:
            # The last action is to create a PV
            self._pv: epics.PV = epics.PV(pvname=self._pvname.strip())
            self._pv.add_callback(self.check_alarms)

    @property
    def pvname(self):
        return self._pvname

    @property
    def alarm_values(self):
        return self.condition.alarm_values

    @property
    def condition(self):
        return self._condition.name

    @property
    def id(self):
        return self._id

    def as_dict(self):
        return {
            "id": self.id,
            "pvname": self.pvname,
            "condition": self.condition,
            "alarm_values": self.alarm_values,
            "unit": self.unit,
            "emails": self.emails,
            "group": self.group.name,
            "warning_message": self.warning_message,
            "subject": self.subject,
            "email_timeout": self.email_timeout,
        }

    def __str__(self):
        return f'<Entry={self._id} pvname="{self.pvname}" group={self.group} condition="{self.condition}" alarm_values={self.alarm_values} emails={self.emails}">'

    def handle_condition(self, value) -> typing.Optional[EmailEvent]:
        """
        Handle the alarm condition and return an post a request to the SMS queue.
        """
        event: typing.Optional[EmailEvent] = None
        cond_res: typing.Optional[ConditionCheckResponse] = None

        try:
            cond_res = self._condition.check_alarm(value)
        except ConditionException as e:
            logger.exception(
                f"Failed to check condition for input '{value}', error {e}"
            )
            return None

        if not cond_res:
            return None

        try:
            if cond_res:
                event = EmailEvent(
                    pvname=self.pvname,
                    specified_value_message=cond_res.message,
                    unit=self.unit,
                    warning=self.warning_message,
                    subject=self.subject,
                    emails=self.emails,
                    condition=self.condition,
                    value_measured="{:.4}".format(value),
                )

        except EntryException:
            logger.exception(f"Invalid entry {self}")

        return event

    def trigger(self):
        """Manual trigger"""
        self.check_alarms(value=self._pv.value)

    def is_timeout_active(self):
        timestamp = time.time()
        timedelta = timestamp - self.last_event_time
        return timedelta < self.email_timeout

    def check_alarms(self, ignore_email_timeout=False, **kwargs):
        """
        Define if an alarm check should be performed. Disconnected PVs will are not checked.
        Trigger this function as a callback and within a timer due to the sms timeout. e.g.: Callback may be ignored.
        :return bool: Perform or not the alarm check.
        """
        if not self.group.enabled:
            logger.debug(f"Ignoring {self} due to disabled group")
            return

        if not self._pv.connected:
            logger.info(f"Ignoring {self}, PV is disconnected")
            return

        if self.is_timeout_active() and not ignore_email_timeout:
            logger.info(f"Ignoring event from {self}, timeout still active.")
            return

        try:
            event = self.handle_condition(value=kwargs["value"])
            if not event:
                return

            self.dispatch_alarm_event(event)

        except Exception as e:
            logger.exception(f"Unexpected exception when handling PV event '{e}'.")

    def dispatch_alarm_event(self, event: EmailEvent):
        if not event:
            logger.error(f"Cannot dispatch empty event {event} from entry {self}")
            return

        try:
            with self._sms_queue_dispatch_lock:  # Lock used to maintain the last_event_time in sync with the queue
                logger.info(f"New event '{event}' being dispatched from {self}")
                self.sms_queue.put(event, block=False, timeout=None)
                self.last_event_time = time.time()

        except queue.Full:
            logger.exception(
                f"Failed to put entry in queue {self} {event}. Queue is full, something wrong is happening..."
            )
