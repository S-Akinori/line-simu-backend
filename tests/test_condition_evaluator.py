import pytest

from line_simu.engine.condition import (
    ConditionEvaluationError,
    evaluate_conditions,
)


# --- Helper to build a single condition ---
def _cond(
    rules: list[dict],
    next_key: str = "next_q",
    logic: str = "and",
    cond_id: str = "c1",
) -> dict:
    return {
        "id": cond_id,
        "rules": rules,
        "logic": logic,
        "next_question_key": next_key,
    }


def _rule(
    question_key: str,
    operator: str,
    value=None,
) -> dict:
    r: dict = {"question_key": question_key, "operator": operator}
    if value is not None:
        r["value"] = value
    return r


# ========================================================================
# eq operator
# ========================================================================
class TestEqOperator:
    def test_eq_matches(self):
        conditions = [_cond([_rule("color", "eq", "red")], "q2")]
        result = evaluate_conditions(conditions, {"color": "red"})
        assert result == "q2"

    def test_eq_no_match(self):
        conditions = [_cond([_rule("color", "eq", "red")], "q2")]
        result = evaluate_conditions(conditions, {"color": "blue"})
        assert result is None

    def test_eq_numeric_as_string(self):
        conditions = [_cond([_rule("age", "eq", "30")], "q2")]
        result = evaluate_conditions(conditions, {"age": "30"})
        assert result == "q2"

    def test_eq_missing_answer_does_not_match(self):
        conditions = [_cond([_rule("color", "eq", "red")], "q2")]
        result = evaluate_conditions(conditions, {})
        assert result is None


# ========================================================================
# neq operator
# ========================================================================
class TestNeqOperator:
    def test_neq_matches(self):
        conditions = [_cond([_rule("color", "neq", "red")], "q2")]
        result = evaluate_conditions(conditions, {"color": "blue"})
        assert result == "q2"

    def test_neq_no_match_when_equal(self):
        conditions = [_cond([_rule("color", "neq", "red")], "q2")]
        result = evaluate_conditions(conditions, {"color": "red"})
        assert result is None


# ========================================================================
# in operator
# ========================================================================
class TestInOperator:
    def test_in_matches(self):
        conditions = [
            _cond([_rule("grade", "in", ["grade_1", "grade_2", "grade_3"])], "q2")
        ]
        result = evaluate_conditions(conditions, {"grade": "grade_2"})
        assert result == "q2"

    def test_in_no_match(self):
        conditions = [
            _cond([_rule("grade", "in", ["grade_1", "grade_2"])], "q2")
        ]
        result = evaluate_conditions(conditions, {"grade": "grade_5"})
        assert result is None

    def test_in_single_element_list(self):
        conditions = [_cond([_rule("grade", "in", ["grade_1"])], "q2")]
        result = evaluate_conditions(conditions, {"grade": "grade_1"})
        assert result == "q2"


# ========================================================================
# not_in operator
# ========================================================================
class TestNotInOperator:
    def test_not_in_matches(self):
        conditions = [
            _cond([_rule("grade", "not_in", ["grade_12", "grade_13"])], "q2")
        ]
        result = evaluate_conditions(conditions, {"grade": "grade_1"})
        assert result == "q2"

    def test_not_in_no_match_when_in_list(self):
        conditions = [
            _cond([_rule("grade", "not_in", ["grade_12", "grade_13"])], "q2")
        ]
        result = evaluate_conditions(conditions, {"grade": "grade_12"})
        assert result is None


# ========================================================================
# Numeric operators: gt, gte, lt, lte
# ========================================================================
class TestNumericOperators:
    def test_gt_matches(self):
        conditions = [_cond([_rule("days", "gt", 30)], "q2")]
        result = evaluate_conditions(conditions, {"days": "31"})
        assert result == "q2"

    def test_gt_equal_no_match(self):
        conditions = [_cond([_rule("days", "gt", 30)], "q2")]
        result = evaluate_conditions(conditions, {"days": "30"})
        assert result is None

    def test_gte_matches_equal(self):
        conditions = [_cond([_rule("days", "gte", 30)], "q2")]
        result = evaluate_conditions(conditions, {"days": "30"})
        assert result == "q2"

    def test_gte_matches_greater(self):
        conditions = [_cond([_rule("days", "gte", 30)], "q2")]
        result = evaluate_conditions(conditions, {"days": "50"})
        assert result == "q2"

    def test_lt_matches(self):
        conditions = [_cond([_rule("days", "lt", 100)], "q2")]
        result = evaluate_conditions(conditions, {"days": "50"})
        assert result == "q2"

    def test_lt_equal_no_match(self):
        conditions = [_cond([_rule("days", "lt", 100)], "q2")]
        result = evaluate_conditions(conditions, {"days": "100"})
        assert result is None

    def test_lte_matches_equal(self):
        conditions = [_cond([_rule("days", "lte", 65)], "q2")]
        result = evaluate_conditions(conditions, {"days": "65"})
        assert result == "q2"

    def test_lte_matches_less(self):
        conditions = [_cond([_rule("days", "lte", 65)], "q2")]
        result = evaluate_conditions(conditions, {"days": "20"})
        assert result == "q2"

    def test_gt_with_float_answer(self):
        conditions = [_cond([_rule("rate", "gt", 3.5)], "q2")]
        result = evaluate_conditions(conditions, {"rate": "4.2"})
        assert result == "q2"

    def test_lte_with_zero(self):
        conditions = [_cond([_rule("score", "lte", 0)], "q2")]
        result = evaluate_conditions(conditions, {"score": "0"})
        assert result == "q2"


# ========================================================================
# exists / not_exists operators
# ========================================================================
class TestExistsOperators:
    def test_exists_when_present(self):
        conditions = [_cond([_rule("name", "exists")], "q2")]
        result = evaluate_conditions(conditions, {"name": "Alice"})
        assert result == "q2"

    def test_exists_when_missing(self):
        conditions = [_cond([_rule("name", "exists")], "q2")]
        result = evaluate_conditions(conditions, {})
        assert result is None

    def test_not_exists_when_missing(self):
        conditions = [_cond([_rule("name", "not_exists")], "q2")]
        result = evaluate_conditions(conditions, {})
        assert result == "q2"

    def test_not_exists_when_present(self):
        conditions = [_cond([_rule("name", "not_exists")], "q2")]
        result = evaluate_conditions(conditions, {"name": "Alice"})
        assert result is None


# ========================================================================
# Compound conditions (AND / OR logic)
# ========================================================================
class TestCompoundConditions:
    def test_and_logic_all_true(self):
        conditions = [
            _cond(
                [
                    _rule("grade", "in", ["grade_1", "grade_2", "grade_3"]),
                    _rule("days", "gt", 30),
                ],
                "detailed_q",
                logic="and",
            )
        ]
        result = evaluate_conditions(
            conditions, {"grade": "grade_2", "days": "60"}
        )
        assert result == "detailed_q"

    def test_and_logic_partial_match_returns_none(self):
        conditions = [
            _cond(
                [
                    _rule("grade", "in", ["grade_1", "grade_2"]),
                    _rule("days", "gt", 30),
                ],
                "detailed_q",
                logic="and",
            )
        ]
        result = evaluate_conditions(
            conditions, {"grade": "grade_2", "days": "10"}
        )
        assert result is None

    def test_or_logic_one_true(self):
        conditions = [
            _cond(
                [
                    _rule("grade", "eq", "grade_1"),
                    _rule("days", "gt", 100),
                ],
                "special_q",
                logic="or",
            )
        ]
        result = evaluate_conditions(
            conditions, {"grade": "grade_5", "days": "200"}
        )
        assert result == "special_q"

    def test_or_logic_none_true(self):
        conditions = [
            _cond(
                [
                    _rule("grade", "eq", "grade_1"),
                    _rule("days", "gt", 100),
                ],
                "special_q",
                logic="or",
            )
        ]
        result = evaluate_conditions(
            conditions, {"grade": "grade_5", "days": "10"}
        )
        assert result is None

    def test_or_logic_all_true(self):
        conditions = [
            _cond(
                [
                    _rule("grade", "eq", "grade_1"),
                    _rule("days", "gt", 10),
                ],
                "special_q",
                logic="or",
            )
        ]
        result = evaluate_conditions(
            conditions, {"grade": "grade_1", "days": "50"}
        )
        assert result == "special_q"


# ========================================================================
# Condition ordering
# ========================================================================
class TestConditionOrdering:
    def test_first_matching_condition_wins(self):
        conditions = [
            _cond([_rule("color", "eq", "red")], "red_path", cond_id="c1"),
            _cond([_rule("color", "eq", "red")], "alternate_path", cond_id="c2"),
        ]
        result = evaluate_conditions(conditions, {"color": "red"})
        assert result == "red_path"

    def test_second_condition_matches_when_first_does_not(self):
        conditions = [
            _cond([_rule("color", "eq", "red")], "red_path", cond_id="c1"),
            _cond([_rule("color", "eq", "blue")], "blue_path", cond_id="c2"),
        ]
        result = evaluate_conditions(conditions, {"color": "blue"})
        assert result == "blue_path"

    def test_no_conditions_returns_none(self):
        result = evaluate_conditions([], {})
        assert result is None

    def test_empty_answers_dict(self):
        conditions = [_cond([_rule("x", "eq", "y")], "q2")]
        result = evaluate_conditions(conditions, {})
        assert result is None

    def test_default_logic_is_and(self):
        """When logic key is omitted, it defaults to 'and'."""
        conditions = [
            {
                "id": "c1",
                "rules": [
                    _rule("a", "eq", "1"),
                    _rule("b", "eq", "2"),
                ],
                "next_question_key": "q2",
            }
        ]
        # Both match -> should return q2 (and logic)
        result = evaluate_conditions(conditions, {"a": "1", "b": "2"})
        assert result == "q2"

        # Only one matches -> should return None (and logic)
        result = evaluate_conditions(conditions, {"a": "1", "b": "9"})
        assert result is None


# ========================================================================
# Error cases
# ========================================================================
class TestErrorCases:
    def test_invalid_operator_raises(self):
        conditions = [_cond([_rule("x", "invalid_op", "y")], "q2")]
        with pytest.raises(ConditionEvaluationError):
            evaluate_conditions(conditions, {"x": "y"})

    def test_non_numeric_value_for_gt_raises(self):
        conditions = [_cond([_rule("age", "gt", 18)], "q2")]
        with pytest.raises(ConditionEvaluationError):
            evaluate_conditions(conditions, {"age": "not_a_number"})

    def test_non_numeric_value_for_lt_raises(self):
        conditions = [_cond([_rule("age", "lt", 100)], "q2")]
        with pytest.raises(ConditionEvaluationError):
            evaluate_conditions(conditions, {"age": "abc"})

    def test_non_numeric_value_for_gte_raises(self):
        conditions = [_cond([_rule("age", "gte", 18)], "q2")]
        with pytest.raises(ConditionEvaluationError):
            evaluate_conditions(conditions, {"age": "hello"})

    def test_non_numeric_value_for_lte_raises(self):
        conditions = [_cond([_rule("age", "lte", 65)], "q2")]
        with pytest.raises(ConditionEvaluationError):
            evaluate_conditions(conditions, {"age": "world"})
