from typing import Any


class ConditionEvaluationError(Exception):
    pass


def _to_numeric(value: str) -> float:
    try:
        return float(value)
    except (ValueError, TypeError):
        raise ConditionEvaluationError(f"Cannot convert '{value}' to number")


OPERATORS: dict[str, Any] = {
    "eq": lambda answer, value: answer == str(value),
    "neq": lambda answer, value: answer != str(value),
    "in": lambda answer, value: answer in [str(v) for v in value],
    "not_in": lambda answer, value: answer not in [str(v) for v in value],
    "gt": lambda answer, value: _to_numeric(answer) > float(value),
    "gte": lambda answer, value: _to_numeric(answer) >= float(value),
    "lt": lambda answer, value: _to_numeric(answer) < float(value),
    "lte": lambda answer, value: _to_numeric(answer) <= float(value),
    "exists": lambda answer, value: answer is not None,
    "not_exists": lambda answer, value: answer is None,
}


def evaluate_conditions(
    conditions: list[dict],
    session_answers: dict[str, str],
) -> str | None:
    """Evaluate conditions and return next_question_key or None."""
    for condition in conditions:
        rules = condition["rules"]
        logic = condition.get("logic", "and")

        results = []
        for rule in rules:
            question_key = rule["question_key"]
            operator = rule["operator"]
            expected_value = rule.get("value")
            actual_answer = session_answers.get(question_key)

            op_func = OPERATORS.get(operator)
            if op_func is None:
                raise ConditionEvaluationError(f"Unknown operator: {operator}")

            results.append(op_func(actual_answer, expected_value))

        if logic == "and" and all(results):
            return condition["next_question_key"]
        elif logic == "or" and any(results):
            return condition["next_question_key"]

    return None


def check_display_conditions(
    display_conditions: list[dict],
    session_answers: dict[str, str],
) -> bool:
    """Return True if the question should be shown.

    Empty display_conditions means always show.
    Any matching group (AND/OR within group) → show.
    """
    if not display_conditions:
        return True
    if not isinstance(display_conditions, list):
        return False
    for group in display_conditions:
        if not isinstance(group, dict):
            continue
        rules = group.get("rules", [])
        logic = group.get("logic", "and")
        results = []
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            actual = session_answers.get(rule.get("question_key"))
            op_func = OPERATORS.get(rule.get("operator"))
            if op_func is None:
                continue
            try:
                results.append(op_func(actual, rule.get("value")))
            except ConditionEvaluationError:
                results.append(False)
        if not results:
            continue
        if logic == "and" and all(results):
            return True
        if logic == "or" and any(results):
            return True
    return False
