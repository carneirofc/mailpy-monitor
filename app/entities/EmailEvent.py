class EmailEvent:
    """Email event sent by entry to the SMS queue to signal alarms"""

    def __init__(
        self,
        pvname: str,
        specified_value_message: str,
        unit: str,
        subject: str,
        emails: str,
        warning: str,
        condition: str,
        value_measured,
    ):
        self.pvname = pvname
        self.specified_value_message = (
            specified_value_message  # String representing the specified value
        )
        self.unit = unit
        self.subject = subject
        self.emails = emails
        self.warning = warning
        self.condition = condition
        self.value_measured = value_measured

    def __str__(self):
        return f"<EmailEvent {self.pvname} {self.value_measured} {self.condition} {self.specified_value_message} {self.subject} {self.emails} {self.warning}>"
