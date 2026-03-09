import asyncio
import logging
import math
from decimal import Decimal
from uuid import UUID

from simpleeval import simple_eval

from line_simu.db.repositories.answer import (
    get_answer_by_question_key,
    get_session_answers_by_key_full,
)
from line_simu.db.repositories.formula import Formula, get_active_formulas, get_formula_by_name
from line_simu.db.repositories.global_constant import get_all_active_global_constants
from line_simu.engine.condition import OPERATORS, check_display_conditions
from line_simu.db.repositories.lookup import find_lookup_entry
from line_simu.schemas.session import Session

logger = logging.getLogger(__name__)

SAFE_FUNCTIONS = {
    "min": min,
    "max": max,
    "round": round,
    "abs": abs,
    "int": int,
    "float": float,
    "floor": math.floor,
    "ceil": math.ceil,
}


class CalculationError(Exception):
    pass


async def evaluate_formula(
    formula: Formula,
    session_id: UUID,
    channel_id: UUID,
    answers_by_key: dict | None = None,
    global_constants: dict | None = None,
) -> Decimal:
    """Evaluate a formula with resolved variables.

    Global constants are loaded first; formula-specific variables take precedence
    over any global constant with the same name.

    Pass pre-loaded answers_by_key / global_constants to avoid redundant DB queries.
    """
    if global_constants is None:
        global_constants = await get_all_active_global_constants()
    resolved_vars: dict[str, float] = dict(global_constants)
    for var_name, var_config in formula.variables.items():
        resolved_vars[var_name] = await resolve_variable(
            var_config, session_id, channel_id,
            answers_by_key=answers_by_key,
            global_constants=global_constants,
        )

    result = simple_eval(
        formula.expression,
        names=resolved_vars,
        functions=SAFE_FUNCTIONS,
    )
    return Decimal(str(result))


async def resolve_variable(
    var_config: dict,
    session_id: UUID,
    channel_id: UUID,
    answers_by_key: dict | None = None,
    global_constants: dict | None = None,
) -> float:
    """Resolve a variable from its source."""
    source = var_config["source"]

    match source:
        case "answer":
            if answers_by_key is not None:
                answer = answers_by_key.get(var_config["question_key"])
            else:
                answer = await get_answer_by_question_key(session_id, var_config["question_key"])
            if answer is None:
                raise CalculationError(f"Missing answer for {var_config['question_key']}")
            return float(answer.answer_numeric or answer.answer_value)

        case "lookup":
            return await resolve_lookup(var_config, session_id, answers_by_key=answers_by_key)

        case "constant":
            return float(var_config["value"])

        case "formula":
            # Nested formula: scoped to same channel
            sub_formula = await get_formula_by_name(var_config["formula_name"], channel_id)
            if sub_formula is None:
                raise CalculationError(f"Formula not found: {var_config['formula_name']}")
            return float(await evaluate_formula(
                sub_formula, session_id, channel_id,
                answers_by_key=answers_by_key,
                global_constants=global_constants,
            ))

        case "conditional":
            # Return different numeric values based on answer conditions.
            # Each case supports multiple conditions (AND logic) via "conditions" list,
            # or a single condition via legacy "question_key"/"operator"/"value" fields.
            # Result and condition comparison values can be literals, answers, or global constants.
            for case_item in var_config.get("cases", []):
                if "conditions" in case_item:
                    matched = await _check_all_conditions(
                        case_item["conditions"], session_id,
                        answers_by_key=answers_by_key,
                        global_constants=global_constants,
                    )
                else:
                    # Legacy single-condition format
                    if answers_by_key is not None:
                        answer = answers_by_key.get(case_item["question_key"])
                    else:
                        answer = await get_answer_by_question_key(session_id, case_item["question_key"])
                    actual = answer.answer_value if answer else None
                    op_func = OPERATORS.get(case_item.get("operator", "eq"))
                    matched = False
                    if op_func is not None:
                        try:
                            matched = bool(op_func(actual, case_item.get("value")))
                        except Exception:
                            pass
                if matched:
                    return await _resolve_case_result(
                        case_item, session_id,
                        answers_by_key=answers_by_key,
                        global_constants=global_constants,
                    )
            default = var_config.get("default")
            if default is not None:
                return float(default)
            raise CalculationError(
                "No matching case and no default for conditional variable"
            )

        case _:
            raise CalculationError(f"Unknown variable source: {source}")


async def _resolve_condition_value(
    cond: dict,
    session_id: UUID,
    answers_by_key: dict | None = None,
    global_constants: dict | None = None,
) -> object:
    """Resolve the right-hand-side comparison value for a condition.

    value_source:
      "literal" (default / absent): use cond["value"] directly
      "answer":  use answer_value of another question
      "global":  use a global constant float
    """
    value_source = cond.get("value_source", "literal")
    if value_source == "answer":
        vq_key = cond.get("value_question_key", "")
        if answers_by_key is not None:
            answer = answers_by_key.get(vq_key)
        else:
            answer = await get_answer_by_question_key(session_id, vq_key)
        return answer.answer_value if answer else None
    if value_source == "global":
        constants = global_constants if global_constants is not None else await get_all_active_global_constants()
        return constants.get(cond.get("value_constant_name", ""))
    return cond.get("value")


async def _resolve_case_result(
    case_item: dict,
    session_id: UUID,
    answers_by_key: dict | None = None,
    global_constants: dict | None = None,
) -> float:
    """Resolve the numeric result value for a matched conditional case.

    result_source:
      "literal" (default / absent): use case_item["result"]
      "answer":  use answer_numeric (or answer_value) of a question
      "global":  use a global constant float
    """
    result_source = case_item.get("result_source", "literal")
    if result_source == "answer":
        rq_key = case_item.get("result_question_key", "")
        if answers_by_key is not None:
            answer = answers_by_key.get(rq_key)
        else:
            answer = await get_answer_by_question_key(session_id, rq_key)
        if answer is None:
            raise CalculationError(f"Missing answer for result key: {rq_key}")
        return float(answer.answer_numeric or answer.answer_value)
    if result_source == "global":
        constants = global_constants if global_constants is not None else await get_all_active_global_constants()
        const_name = case_item.get("result_constant_name", "")
        val = constants.get(const_name)
        if val is None:
            raise CalculationError(f"Global constant not found: {const_name}")
        return val
    return float(case_item["result"])


async def _check_all_conditions(
    conditions: list[dict],
    session_id: UUID,
    answers_by_key: dict | None = None,
    global_constants: dict | None = None,
) -> bool:
    """Return True only if every condition in the list matches (AND logic)."""
    for cond in conditions:
        if answers_by_key is not None:
            answer = answers_by_key.get(cond["question_key"])
        else:
            answer = await get_answer_by_question_key(session_id, cond["question_key"])
        actual = answer.answer_value if answer else None
        op_func = OPERATORS.get(cond.get("operator", "eq"))
        if op_func is None:
            return False
        comparison_value = await _resolve_condition_value(
            cond, session_id,
            answers_by_key=answers_by_key,
            global_constants=global_constants,
        )
        try:
            if not op_func(actual, comparison_value):
                return False
        except Exception:
            return False
    return True


async def resolve_lookup(
    var_config: dict,
    session_id: UUID,
    answers_by_key: dict | None = None,
) -> float:
    """Resolve a value from a lookup table (lookup_tables are global / shared).

    key_mappings values can be either:
    - str: the question_key whose answer_value is used directly
    - dict {"question_key": ..., "transform": "floor(x / 30)"}: answer_numeric is
      evaluated through the expression (x = answer value) before lookup
    """
    key_values: dict[str, str] = {}
    for lookup_key, mapping in var_config["key_mappings"].items():
        if isinstance(mapping, str):
            question_key = mapping
            transform = None
        else:
            question_key = mapping["question_key"]
            transform = mapping.get("transform") or None

        if answers_by_key is not None:
            answer = answers_by_key.get(question_key)
        else:
            answer = await get_answer_by_question_key(session_id, question_key)
        if answer is None:
            raise CalculationError(f"Missing answer for lookup key: {question_key}")

        if transform:
            x = float(answer.answer_numeric or answer.answer_value)
            raw = simple_eval(transform, names={"x": x}, functions=SAFE_FUNCTIONS)
            num = float(raw)
            key_values[lookup_key] = str(int(num)) if num == int(num) else str(num)
        else:
            key_values[lookup_key] = answer.answer_value

    entry = await find_lookup_entry(var_config["table_name"], key_values)
    if entry is None:
        raise CalculationError(
            f"No lookup entry found in '{var_config['table_name']}' for keys: {key_values}"
        )
    return float(entry.result_value)


async def run_calculations(
    session: Session,
    channel_id: UUID,
    filter_names: set[str] | None = None,
) -> dict:
    """Run active formulas and return results.

    Args:
        filter_names: If given, only formulas whose name is in this set are
                      evaluated. Pass None to evaluate all active formulas.
    """
    formulas, global_constants, answers_by_key = await asyncio.gather(
        get_active_formulas(channel_id),
        get_all_active_global_constants(),
        get_session_answers_by_key_full(session.id),
    )

    # check_display_conditions expects dict[str, str]; derive from full Answer objects.
    answers_str = {k: v.answer_value for k, v in answers_by_key.items()}

    active_formulas = [
        f for f in sorted(formulas, key=lambda f: f.display_order)
        if (filter_names is None or f.name in filter_names)
        and not (f.condition and not check_display_conditions(f.condition, answers_str))
    ]

    async def _eval_one(formula: Formula) -> tuple[str, dict]:
        try:
            value = await evaluate_formula(
                formula, session.id, channel_id,
                answers_by_key=answers_by_key,
                global_constants=global_constants,
            )
            formatted = format_value(value, formula.value_unit, formula.value_scale, formula.value_decimals)
            return formula.name, {
                "label": formula.result_label,
                "value": float(value),
                "formatted": formatted,
                "result_format": formula.result_format,
            }
        except CalculationError as e:
            # Debug level: missing answers are expected for formulas that don't
            # apply to the current route (e.g. all-formula mode with partial answers).
            logger.debug("Calculation skipped for %s: %s", formula.name, e)
            return formula.name, {
                "label": formula.result_label,
                "value": None,
                "error": str(e),
                "result_format": formula.result_format,
            }

    pairs = await asyncio.gather(*(_eval_one(f) for f in active_formulas))
    return dict(pairs)


def format_value(value: Decimal, unit: str, scale: float = 1, decimals: int = 0) -> str:
    """Format a numeric value with configurable unit, scale, and decimal places.

    Args:
        value: The calculated result.
        unit: Suffix appended to the number (e.g. '円', '点', 'kg').
        scale: Divisor applied before display (e.g. 10000 for 万円).
        decimals: Number of decimal places. Trailing zeros are trimmed automatically.
    """
    scaled = float(value) / scale
    if decimals == 0:
        return f"{int(round(scaled)):,}{unit}"
    formatted = f"{scaled:,.{decimals}f}"
    if "." in formatted:
        formatted = formatted.rstrip("0").rstrip(".")
    return f"{formatted}{unit}"
