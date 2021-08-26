import multiprocessing
import threading
import typing
import time
import logging
import queue


import epics

from .Group import Group
from .Condition import Condition
from .EmailEvent import EmailEvent

from app.helpers import EntryException


logger = logging.getLogger()


class DummyPV:
    def __init__(self, pvname):
        self.pvname = pvname

    def __str__(self):
        return f"<DummyPV pvname={self.pvname}>"


class Entry:
    """
    Encapsulates a PV and the email logic associated with it
    :dummy bool: signal a dummy entry, used for tests and utility scripts.
    """

    def __init__(
        self,
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
        _id=None,
        dummy: bool = False,
    ):
        self.pv: typing.Optional[epics.PV] = None
        self._id = _id
        self.condition = condition.lower().strip()
        self.alarm_values = alarm_values
        self.unit = unit
        self.warning_message = warning_message
        self.subject = subject
        self.email_timeout = email_timeout
        self.emails = emails
        self.group = group
        self.lock = threading.RLock()
        self.sms_queue = sms_queue
        self._dummy = dummy
        self._pvname = pvname

        self.step_level: int = 0  # Actual level
        self.step_values: typing.List[float] = []  # Step value according to level

        # reset last_event_time for all PVs, so it start monitoring right away
        self.last_event_time = time.time() - self.email_timeout

        if self.condition == Condition.OutOfRange:
            self._init_out_of_range()
        elif self.condition == Condition.SuperiorThan:
            self._init_superior_than()
        elif self.condition == Condition.InferiorThan:
            self._init_inferior_than()
        elif self.condition == Condition.IncreasingStep:
            self._init_increasing_step()
        else:
            # @todo: Condition.DecreasingStep condition not supported
            raise EntryException(f"Invalid condition {self.condition} for entry {self}")

        self.pv = DummyPV(
            pvname=self._pvname.strip()
        )  # Create the dummy PV, it will be replaced once the connection is estabilished

    def connect(self):
        if not self._dummy:
            # The last action is to create a PV
            self.pv: epics.PV = epics.PV(pvname=self._pvname.strip())
            self.pv.add_callback(self.check_alarms)

    def as_dict(self):
        return {
            "pvname": self.pv.pvname,
            "condition": self.condition,
            "alarm_values": self.alarm_values,
            "unit": self.unit,
            "emails": self.emails,
            "group": self.group.name,
            "warning_message": self.warning_message,
            "subject": self.subject,
            "email_timeout": self.email_timeout,
        }

    def _init_out_of_range(self):
        _min, _max = self.alarm_values.split(":")
        self.alarm_min = float(_min)
        self.alarm_max = float(_max)
        if self.alarm_min >= self.alarm_max:
            raise EntryException(
                f"Cannot create entry for {self.pv.pvname if self.pv else None} with values {self.alarm_values}. Min must be lesser than max"
            )

    def _init_superior_than(self):
        self.alarm_max = float(self.alarm_values)

    def _init_inferior_than(self):
        self.alarm_min = float(self.alarm_values)

    def _init_increasing_step(self):
        """
        create two dictionaries, "step_level" and "step":
         -------------------------------------------------------------------------
         * step_level:
             - keys: [int] indexes that contain the 'increasing step' condition
             - values: [int] represent the current step in the stair, where 0 is
                       the lowest level, and len(step[i]) is highest level
            e.g.:
               value = '1.5:2.0:2.5:3.0'
               step  = [1.5,2.0,2.5,3.0]
               len(step) = 4

               3.0 _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _  ___Level_4___
               2.5 _ _ _ _ _ _ _ _ _ _ _ _ _ _ __L3__|
               2.0 _ _ _ _ _ _ _ _ _ _  __L2__|
               1.5 _ _ _ _ _ _ _ __L1__|
                                |
                                |
                 0 ___Level_0___|

              -Level 0: lowest level, 0 ~ 1.5 u (normal operation)
                 ...
              -Level 4: highest level, 3.0+

          obs.: emails are only sent when moving from any level to a higher one
         -------------------------------------------------------------------------
         * step:
             - keys: [int] indexes that contain the 'increasing step' condition
             - values: [array of float] represent the steps that triggers mailing
        """
        self.step_level = 0  # Current step level
        self.step_values = [
            float(val) for val in self.alarm_values.split(":")
        ]  # Step level border

        s = self.step_values[0]
        for v in self.step_values[1:]:
            if s >= v:
                raise EntryException(
                    f"Failed to crate entry for {self.pv.pvname}, step values {self.alarm_values} are not sorted"
                )
            s = v
        self.min_level = 0
        self.max_level = len(self.step_values)

    def __str__(self):
        return f'<Entry={self._id} pvname="{self.pv.pvname if self.pv else None}" group={self.group} condition="{self.condition}" alarm_values={self.alarm_values} emails={self.emails}">'

    def find_next_level(self, value) -> int:
        loop_level = 0
        for step_value in self.step_values:
            # Locate the next level we belong
            if value < step_value:
                # If the value is lesser than the next level beginning, we found our current level
                return loop_level
            loop_level += 1

    def condition_increasing_step(self, value):
        specified_value_message = None
        next_step_limiar = self.step_values[self.step_level]
        if value >= next_step_limiar and (self.step_level + 1) <= self.max_level:
            # We are going up levels
            loop_level = self.find_next_level(value)
            self.step_level = loop_level
            specified_value_message = f"lower than {self.step_values[0]}{self.unit}"

        if value >= next_step_limiar and self.step_level == self.max_level:
            # We are already at the maximum level
            pass

        if value < next_step_limiar and self.step_level == self.min_level:
            # We are already at the lowest level
            pass

        if (
            value < next_step_limiar
            and self.step_level != self.min_level
            and value < self.step_values[self.step_level - 1]
        ):
            # Going down levels
            loop_level = self.find_next_level(value)
            logger.info(
                f"{self} going down from level {self.step_level} to {loop_level}"
            )
            self.step_level = loop_level
        return specified_value_message

    def handle_condition(self, value) -> typing.Optional[EmailEvent]:
        """
        Handle the alarm condition and return an post a request to the SMS queue.
        """
        specified_value_message: typing.Optional[str] = None
        event: typing.Optional[EmailEvent] = None

        if self.condition == Condition.OutOfRange and (
            value < self.alarm_min or value > self.alarm_max
        ):
            specified_value_message = (
                f"from {self.alarm_min}{self.unit} to {self.alarm_max} {self.unit}"
            )

        elif self.condition == Condition.SuperiorThan and value > self.alarm_max:
            specified_value_message = f"lower than {self.alarm_min}{self.unit}"

        elif self.condition == Condition.InferiorThan and value < self.alarm_min:
            specified_value_message = f"higher than {self.alarm_min}{self.unit}"

        elif self.condition == Condition.IncreasingStep:
            specified_value_message = self.condition_increasing_step(value)

        try:
            if specified_value_message:
                event = EmailEvent(
                    pvname=self.pv.pvname,
                    specified_value_message=specified_value_message,
                    unit=self.unit,
                    warning=self.warning_message,
                    subject=self.subject,
                    emails=self.emails,
                    condition=self.condition,
                    value_measured="{:.4}".format(
                        value
                    ),  # Consider using prec field from PV
                )

        except EntryException:
            logger.exception(f"Invalid entry {self}")

        finally:
            return event

    def trigger(self):
        """ Manual trigger """
        self.check_alarms(value=self.pv.value)

    def check_alarms(self, **kwargs):
        """
        Define if an alarm check should be performed. Disconnected PVs will are not checked.
        Trigger this function as a callback and within a timer due to the sms timeout. e.g.: Callback may be ignored.
        :return bool: Perform or not the alarm check.
        """
        if not self.group.enabled:
            logger.debug(f"Ignoring {self} due to disabled group")
            return

        if not self.pv.connected:
            # @todo: Consider sending an email due to disconnected PV
            logger.debug(f"Ignoring {self}, PV is disconnected")
            return

        timestamp = time.time()
        timedelta = timestamp - self.last_event_time
        if timedelta < self.email_timeout:
            logger.info(
                "Ignoring {}, timeout still active for {:.2}s".format(self, timedelta)
            )
            return

        try:
            with self.lock:
                event = self.handle_condition(value=kwargs["value"])

                if event:
                    self.sms_queue.put(event, block=False, timeout=None)
                    self.last_event_time = timestamp
                    logger.info(f"New event {event}")

        except queue.Full:
            logger.exception(
                f"Failed to put entry in queue {self} {event}. Queue is full, something wrong is happening..."
            )
