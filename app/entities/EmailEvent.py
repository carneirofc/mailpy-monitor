import dataclasses


@dataclasses.dataclass
class EmailEvent:
    """Email event sent by entry to the SMS queue to signal alarms"""

    pvname: str
    specified_value_message: str
    unit: str
    subject: str
    emails: str
    warning: str
    condition: str
    value_measured: str
