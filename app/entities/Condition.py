import logging
import typing

from ..helpers.exceptions import EntryException

logger = logging.getLogger()


class ConditionEnums(object):
    """Alarm  conditions"""

    OutOfRange = "out of range"
    SuperiorThan = "superior than"
    InferiorThan = "inferior than"
    IncreasingStep = "increasing step"
    DecreasingStep = "decreasing step"

    @staticmethod
    def get_conditions() -> typing.List:
        return [
            {
                "name": ConditionEnums.OutOfRange,
                "desc": "Must remain within the specified range.",
            },
            {"name": ConditionEnums.SuperiorThan, "desc": "Must remain superior than."},
            {"name": ConditionEnums.InferiorThan, "desc": "Must remain inferior than."},
            {
                "name": ConditionEnums.IncreasingStep,
                "desc": "Each increasing step triggers an alarm.",
            },
            {
                "name": ConditionEnums.DecreasingStep,
                "desc": "Each decreasing step triggers an alarm.",
            },
        ]


class ConditionException(EntryException):
    pass


class ConditionCheckResponse(typing.NamedTuple):
    message: str
    extras: dict = {}


class Condition:
    def __init__(self, limits: str) -> None:
        pass

    def check_alarm(self, value: typing.Any) -> typing.Optional[ConditionCheckResponse]:
        raise NotImplementedError("Child class must impplement this method")


class ConditionInferiorThan(Condition):
    def __init__(self, limits: str) -> None:
        super().__init__(limits)
        self.alarm_limit: float
        self._parse_limits(limits)

    def _parse_limits(self, limits):
        if not limits or type(limits) != str:
            raise ConditionException(f"Cannot create condition with limits '{limits}'")

        self.alarm_limit = float(limits)

    def check_alarm(self, value: typing.Any) -> typing.Optional[ConditionCheckResponse]:
        if type(value) != int and type(value) != float:
            raise ConditionException(
                f"Condition {self} requires a numeric input, received {type(value)}"
            )

        if value < self.alarm_limit:
            return ConditionCheckResponse(
                message=f"value required to be higher than {self.alarm_limit}"
            )
        return None


class ConditionSuperiorThan(Condition):
    def __init__(self, limits: str) -> None:
        super().__init__(limits)
        self.alarm_limit: float
        self._parse_limits(limits)

    def _parse_limits(self, limits):
        if not limits or type(limits) != str:
            raise ConditionException(f"Cannot create condition with limits '{limits}'")

        self.alarm_limit = float(limits)

    def check_alarm(self, value: typing.Any) -> typing.Optional[ConditionCheckResponse]:
        if type(value) != int and type(value) != float:
            raise ConditionException(
                f"Condition {self} requires a numeric input, received {type(value)}"
            )

        if value > self.alarm_limit:
            return ConditionCheckResponse(
                message=f"value required to be lower than {self.alarm_limit}"
            )
        return None


class ConditionOutOfRange(Condition):
    def __init__(self, limits: str) -> None:
        super().__init__(limits)
        self.alarm_min: float
        self.alarm_max: float

        self._parse_limits(limits)

    def _parse_limits(self, limits):
        if not limits or type(limits) != str:
            raise ConditionException(f"Cannot create condition with limits '{limits}'")

        _min, _max = limits.split(":")

        if _min >= _max:
            raise ConditionException(
                f"Cannot create condition with limits '{limits}', condition '{_min}<{_max}' must be valid"
            )

        self.alarm_min = float(_min)
        self.alarm_max = float(_max)

    def check_alarm(self, value: float) -> typing.Optional[ConditionCheckResponse]:
        if type(value) != int and type(value) != float:
            raise ConditionException(
                f"Condition {self} requires a numeric input, received {type(value)}"
            )

        if value < self.alarm_min or value > self.alarm_max:
            return ConditionCheckResponse(
                message=f"from {self.alarm_min} to {self.alarm_max}"
            )

        return None


class ConditionIncreasingStep(Condition):
    """
    create two dictionaries, "step_level" and "step":Levels change when its equal to  the limit.
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

    def __init__(self, limits: str) -> None:
        super().__init__(limits)

        if not limits or type(limits) != str:
            raise ConditionException(f"Cannot create condition with limits '{limits}'")

        self.step_values: typing.List[float] = []
        self.step_level: int = 0
        self.min_level = 0
        self.max_level = -1

        self._parse_limits(limits=limits)

    def _parse_limits(self, limits: str):
        self.step_level = 0  # Current step level
        try:
            self.step_values = [
                float(val) for val in limits.split(":")
            ]  # Step level border
        except ValueError as e:
            raise ConditionException(
                f"Cannot create condition, only numeric values are supported. {e}"
            )

        s = self.step_values[0]
        for v in self.step_values[1:]:
            if s >= v:
                raise ConditionException(
                    f"Cannot create condition, step values {limits} are not sorted"
                )
            s = v
        self.min_level = 0
        self.max_level = len(self.step_values)

    def get_level_str(self, level: int):
        if level == self.min_level:
            return f"lowest level ({level}), values lesser than {self.step_values[0]}"
        if level == self.max_level:
            return f"highest level ({level}), values greater than {self.step_values[level - 1]}"

        return f"level ({level}), values between {self.step_values[level -1]}  and {self.step_values[level]}"

    def check_alarm(
        self, value: typing.Union[int, float]
    ) -> typing.Optional[ConditionCheckResponse]:

        if type(value) != int and type(value) != float:
            raise ConditionException(
                f"Condition {self} requires a numeric input, received {type(value)}"
            )

        # specified_value_message: typing.Optional[str] = None

        new_value_level = self.find_level_for_value(value)
        if new_value_level > self.step_level:
            # We are going up levels
            response = ConditionCheckResponse(
                message=self.get_level_str(new_value_level)
            )
            self.step_level = new_value_level
            return response

        if new_value_level < self.step_level:
            logger.info(
                f"{self} going down from level {self.step_level} to {new_value_level}. {self.get_level_str(new_value_level)}"
            )

        self.step_level = new_value_level
        return None

    def find_level_for_value(self, value) -> int:
        level = 0
        for min_value in self.step_values:
            # Locate the next level we belong
            if min_value > value:
                return level
            level += 1
        return level


def create_condition(condition: str, alarm_values: str) -> Condition:
    if condition == ConditionEnums.OutOfRange:
        return ConditionOutOfRange(limits=alarm_values)

    elif condition == ConditionEnums.SuperiorThan:
        return ConditionSuperiorThan(limits=alarm_values)

    elif condition == ConditionEnums.InferiorThan:
        return ConditionInferiorThan(limits=alarm_values)

    elif condition == ConditionEnums.IncreasingStep:
        return ConditionIncreasingStep(limits=alarm_values)

    raise ConditionException(
        f"Invalid condition '{condition}, factory does not support this."
    )
