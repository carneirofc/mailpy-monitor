import dataclasses
import enum
import typing

from .timestamp import Timestamp


@enum.unique
class EventType(int, enum.Enum):
    ALARM = 0


@dataclasses.dataclass(frozen=True)
class Event:
    type: EventType
    ts: Timestamp


@dataclasses.dataclass(frozen=True)
class AlarmEvent(Event):
    """Email event sent by entry to the SMS queue to signal alarms"""

    pvname: str
    specified_value_message: str
    unit: str
    subject: str
    emails: typing.List[str]
    warning: str
    condition: str
    value_measured: str


def _value_to_string(value):
    if type(value) == float:
        return "{:.4}".format(value)

    return str(value)


def _check_emails(emails):
    if type(emails) == list:
        return emails
    raise ValueError(f"Invalid type for emails '{emails}', expected type {list}")


def create_alarm_event(
    pvname: str,
    specified_value_message: str,
    unit: str,
    warning: str,
    subject: str,
    emails: typing.List[str],
    condition: str,
    value_measured: typing.Any,
) -> AlarmEvent:

    return AlarmEvent(
        type=EventType.ALARM,
        pvname=pvname,
        specified_value_message=specified_value_message,
        unit=unit,
        warning=warning,
        subject=subject,
        emails=_check_emails(emails),
        condition=condition,
        value_measured=_value_to_string(value_measured),
        ts=Timestamp(),
    )
