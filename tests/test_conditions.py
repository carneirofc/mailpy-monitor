import unittest

from app.entities.Condition import (
    Condition,
    ConditionEnums,
    ConditionException,
    ConditionIncreasingStep,
    ConditionInferiorThan,
    ConditionOutOfRange,
    ConditionSuperiorThan,
    create_condition,
)


class AlarmConditionTest(unittest.TestCase):
    def check_condition_inputs(self, condition):
        wrong_inputs = ["!@#", "asf", "123", [], {}, ()]
        for input in wrong_inputs:
            with self.assertRaises(ConditionException):
                condition.check_alarm(input)

    def test_condition_factory(self):
        for condition_str in [
            "out of range",
            "superior than",
            "inferior than",
            "increasing step",
            "decreasing step",
        ]:
            with self.assertRaises(ConditionException):
                create_condition(condition="ASD!@#AS", alarm_values=":!@#")

            with self.assertRaises(ConditionException):
                create_condition(condition=condition_str, alarm_values=None)

    def test_increasing_step(self):
        alarm_values = "0:1:2:3"
        condition: ConditionIncreasingStep = create_condition(
            condition=ConditionEnums.IncreasingStep, alarm_values=alarm_values
        )
        self.assertEqual(condition.alarm_values, alarm_values)

        self.assertIsInstance(condition, Condition)
        self.assertIsInstance(condition, ConditionIncreasingStep)
        self.check_condition_inputs(condition)
        self.assertEqual(condition.name, ConditionEnums.IncreasingStep)

        invalid_values = ["1:2:3:3", "0.12:-2", ":1:2", "1:1:2:3", "3:2.:1", "-2::3"]
        for alarm_value in invalid_values:
            with self.assertRaises(ConditionException):
                create_condition(
                    condition=ConditionEnums.IncreasingStep, alarm_values=alarm_value
                )

        valid_values = ["0:.23:1.4:5", "-0.12:0:54:1000", "300:2555"]
        for alarm_value in valid_values:
            create_condition(
                condition=ConditionEnums.IncreasingStep, alarm_values=alarm_value
            )

        with self.assertRaises(ConditionException):
            condition.check_alarm(None)

        with self.assertRaises(ConditionException):
            condition.check_alarm([])

        self.assertEqual(condition.step_level, 0)
        sequence = [
            (-1, 0, False),
            (-0.12, 0, False),
            (0, 1, True),
            (2, 3, True),
            (1, 2, False),
            (1, 2, False),
            (6, 4, True),
            (60, 4, False),
            (1236, 4, False),
            (3, 4, False),
            (3.23, 4, False),
            (2.23, 3, False),
            (-1.23, 0, False),
        ]
        for input, expected_level, has_alarmed in sequence:
            _alarm_check = condition.check_alarm(input)
            if has_alarmed:
                self.assertIsNotNone(_alarm_check)
            else:
                self.assertIsNone(_alarm_check)
            self.assertEqual(expected_level, condition.step_level)

    def test_inferior_than(self):
        alarm = 10
        condition = create_condition(
            condition=ConditionEnums.InferiorThan, alarm_values=str(alarm)
        )
        self.assertIsInstance(condition, Condition)
        self.assertIsInstance(condition, ConditionInferiorThan)
        self.assertEqual(condition.name, ConditionEnums.InferiorThan)
        self.assertEqual(condition.alarm_values, str(alarm))
        self.check_condition_inputs(condition)

        self.assertIsNone(condition.check_alarm(alarm))
        self.assertIsNone(condition.check_alarm(alarm + 1))

        self.assertIsNotNone(condition.check_alarm(alarm - 1))

    def test_superior_than(self):
        alarm = 10
        condition = create_condition(
            condition=ConditionEnums.SuperiorThan, alarm_values=str(alarm)
        )
        self.assertEqual(condition.alarm_values, str(alarm))
        self.assertIsInstance(condition, Condition)
        self.assertIsInstance(condition, ConditionSuperiorThan)
        self.assertEqual(condition.name, ConditionEnums.SuperiorThan)
        self.check_condition_inputs(condition)

        self.assertIsNone(condition.check_alarm(alarm))
        self.assertIsNone(condition.check_alarm(alarm - 1))
        self.assertIsNotNone(condition.check_alarm(alarm + 1))

    def test_out_of_range(self):
        alarm_min = 123
        alarm_max = 153
        alarm_str = f"{alarm_min}:{alarm_max}"

        condition = create_condition(
            condition=ConditionEnums.OutOfRange, alarm_values=alarm_str
        )
        self.assertEqual(condition.alarm_values, alarm_str)
        self.assertEqual(condition.name, ConditionEnums.OutOfRange)
        self.check_condition_inputs(condition)

        self.assertIsInstance(condition, Condition)
        self.assertIsInstance(condition, ConditionOutOfRange)
        self.assertIsNone(condition.check_alarm(alarm_min))
        self.assertIsNone(condition.check_alarm(alarm_max))
        self.assertIsNone(condition.check_alarm(alarm_min + 1))
        self.assertIsNone(condition.check_alarm(alarm_max - 1))

        self.assertIsNotNone(condition.check_alarm(alarm_min - 1))
        self.assertIsNotNone(condition.check_alarm(alarm_max + 1))
