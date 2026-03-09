from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from line_simu.db.repositories.formula import Formula
from line_simu.db.repositories.lookup import LookupEntry
from line_simu.engine.calculation import (
    CalculationError,
    evaluate_formula,
    format_value,
    resolve_variable,
    run_calculations,
)
from line_simu.schemas.answer import Answer
from line_simu.schemas.session import Session


def _make_formula(**overrides) -> Formula:
    defaults = {
        "id": uuid4(),
        "name": "test_formula",
        "description": "Test",
        "expression": "a + b",
        "variables": {
            "a": {"source": "constant", "value": 100},
            "b": {"source": "constant", "value": 200},
        },
        "result_label": "Test Result",
        "result_format": "{label}: {value}",
        "value_unit": "円",
        "value_scale": 1,
        "value_decimals": 0,
        "display_order": 0,
        "is_active": True,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }
    defaults.update(overrides)
    return Formula(**defaults)


def _make_session(**overrides) -> Session:
    defaults = {
        "id": uuid4(),
        "line_user_id": uuid4(),
        "status": "in_progress",
        "current_question_id": None,
        "last_reminder_sent_at": None,
        "reminder_count": 0,
        "result": None,
        "started_at": datetime.now(timezone.utc),
        "completed_at": None,
        "abandoned_at": None,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }
    defaults.update(overrides)
    return Session(**defaults)


def _make_answer(**overrides) -> Answer:
    defaults = {
        "id": uuid4(),
        "session_id": uuid4(),
        "question_id": uuid4(),
        "answer_value": "100",
        "answer_numeric": Decimal("100"),
        "answered_at": datetime.now(timezone.utc),
    }
    defaults.update(overrides)
    return Answer(**defaults)


# ========================================================================
# evaluate_formula tests
# ========================================================================
class TestEvaluateFormula:
    @pytest.mark.asyncio
    async def test_simple_addition_with_constants(self):
        formula = _make_formula(
            expression="a + b",
            variables={
                "a": {"source": "constant", "value": 4300},
                "b": {"source": "constant", "value": 30},
            },
        )
        result = await evaluate_formula(formula, uuid4())
        assert result == Decimal("4330")

    @pytest.mark.asyncio
    async def test_multiplication_with_constants(self):
        formula = _make_formula(
            expression="daily_rate * days",
            variables={
                "daily_rate": {"source": "constant", "value": 4300},
                "days": {"source": "constant", "value": 60},
            },
        )
        result = await evaluate_formula(formula, uuid4())
        assert result == Decimal("258000")

    @pytest.mark.asyncio
    async def test_with_min_function(self):
        formula = _make_formula(
            expression="min(a, b)",
            variables={
                "a": {"source": "constant", "value": 100},
                "b": {"source": "constant", "value": 200},
            },
        )
        result = await evaluate_formula(formula, uuid4())
        assert result == Decimal("100")

    @pytest.mark.asyncio
    async def test_with_max_function(self):
        formula = _make_formula(
            expression="max(a, b)",
            variables={
                "a": {"source": "constant", "value": 100},
                "b": {"source": "constant", "value": 200},
            },
        )
        result = await evaluate_formula(formula, uuid4())
        assert result == Decimal("200")

    @pytest.mark.asyncio
    async def test_complex_expression(self):
        formula = _make_formula(
            expression="(base + extra) * multiplier",
            variables={
                "base": {"source": "constant", "value": 1000},
                "extra": {"source": "constant", "value": 500},
                "multiplier": {"source": "constant", "value": 3},
            },
        )
        result = await evaluate_formula(formula, uuid4())
        assert result == Decimal("4500")

    @pytest.mark.asyncio
    @patch(
        "line_simu.engine.calculation.get_answer_by_question_key",
        new_callable=AsyncMock,
    )
    async def test_answer_variable_uses_numeric(self, mock_get_answer):
        sid = uuid4()
        mock_get_answer.return_value = _make_answer(
            answer_value="30",
            answer_numeric=Decimal("30"),
        )
        formula = _make_formula(
            expression="days * rate",
            variables={
                "days": {"source": "answer", "question_key": "hospitalization_days"},
                "rate": {"source": "constant", "value": 4300},
            },
        )
        result = await evaluate_formula(formula, sid)
        assert result == Decimal("129000")

    @pytest.mark.asyncio
    @patch(
        "line_simu.engine.calculation.get_answer_by_question_key",
        new_callable=AsyncMock,
    )
    async def test_answer_variable_falls_back_to_value_string(self, mock_get_answer):
        sid = uuid4()
        mock_get_answer.return_value = _make_answer(
            answer_value="50",
            answer_numeric=None,
        )
        formula = _make_formula(
            expression="days * 100",
            variables={
                "days": {"source": "answer", "question_key": "outpatient_days"},
            },
        )
        result = await evaluate_formula(formula, sid)
        assert result == Decimal("5000")

    @pytest.mark.asyncio
    @patch(
        "line_simu.engine.calculation.get_answer_by_question_key",
        new_callable=AsyncMock,
    )
    async def test_missing_answer_raises_calculation_error(self, mock_get_answer):
        sid = uuid4()
        mock_get_answer.return_value = None
        formula = _make_formula(
            expression="x * 10",
            variables={
                "x": {"source": "answer", "question_key": "missing_key"},
            },
        )
        with pytest.raises(CalculationError, match="Missing answer"):
            await evaluate_formula(formula, sid)

    @pytest.mark.asyncio
    @patch(
        "line_simu.engine.calculation.find_lookup_entry",
        new_callable=AsyncMock,
    )
    @patch(
        "line_simu.engine.calculation.get_answer_by_question_key",
        new_callable=AsyncMock,
    )
    async def test_lookup_variable(self, mock_get_answer, mock_find_lookup):
        sid = uuid4()
        mock_get_answer.return_value = _make_answer(answer_value="grade_7")
        mock_find_lookup.return_value = LookupEntry(
            id=uuid4(),
            lookup_table_id=uuid4(),
            key_values={"injury_grade": "grade_7"},
            result_value=Decimal("530000"),
            created_at=datetime.now(timezone.utc),
        )
        formula = _make_formula(
            expression="consolation",
            variables={
                "consolation": {
                    "source": "lookup",
                    "table_name": "consolation_money_table",
                    "key_mappings": {"injury_grade": "injury_grade_answer"},
                },
            },
        )
        result = await evaluate_formula(formula, sid)
        assert result == Decimal("530000")

    @pytest.mark.asyncio
    @patch(
        "line_simu.engine.calculation.find_lookup_entry",
        new_callable=AsyncMock,
    )
    @patch(
        "line_simu.engine.calculation.get_answer_by_question_key",
        new_callable=AsyncMock,
    )
    async def test_lookup_entry_not_found_raises(
        self, mock_get_answer, mock_find_lookup
    ):
        sid = uuid4()
        mock_get_answer.return_value = _make_answer(answer_value="grade_99")
        mock_find_lookup.return_value = None
        formula = _make_formula(
            expression="consolation",
            variables={
                "consolation": {
                    "source": "lookup",
                    "table_name": "consolation_money_table",
                    "key_mappings": {"injury_grade": "injury_grade_answer"},
                },
            },
        )
        with pytest.raises(CalculationError, match="No lookup entry found"):
            await evaluate_formula(formula, sid)

    @pytest.mark.asyncio
    async def test_unknown_variable_source_raises(self):
        formula = _make_formula(
            expression="x",
            variables={
                "x": {"source": "unknown_source", "value": 1},
            },
        )
        with pytest.raises(CalculationError, match="Unknown variable source"):
            await evaluate_formula(formula, uuid4())

    @pytest.mark.asyncio
    @patch(
        "line_simu.engine.calculation.get_formula_by_name",
        new_callable=AsyncMock,
    )
    async def test_nested_formula_variable(self, mock_get_formula):
        sid = uuid4()
        sub_formula = _make_formula(
            name="sub_calc",
            expression="a * b",
            variables={
                "a": {"source": "constant", "value": 10},
                "b": {"source": "constant", "value": 5},
            },
        )
        mock_get_formula.return_value = sub_formula

        parent_formula = _make_formula(
            name="parent_calc",
            expression="sub_result + 100",
            variables={
                "sub_result": {"source": "formula", "formula_name": "sub_calc"},
            },
        )
        result = await evaluate_formula(parent_formula, sid)
        assert result == Decimal("150")

    @pytest.mark.asyncio
    @patch(
        "line_simu.engine.calculation.get_formula_by_name",
        new_callable=AsyncMock,
    )
    async def test_nested_formula_not_found_raises(self, mock_get_formula):
        sid = uuid4()
        mock_get_formula.return_value = None
        formula = _make_formula(
            expression="sub",
            variables={
                "sub": {"source": "formula", "formula_name": "nonexistent"},
            },
        )
        with pytest.raises(CalculationError, match="Formula not found"):
            await evaluate_formula(formula, sid)


# ========================================================================
# run_calculations tests
# ========================================================================
class TestRunCalculations:
    @pytest.mark.asyncio
    @patch(
        "line_simu.engine.calculation.get_active_formulas",
        new_callable=AsyncMock,
    )
    async def test_empty_formulas_returns_empty_dict(self, mock_get_formulas):
        mock_get_formulas.return_value = []
        session = _make_session()
        result = await run_calculations(session)
        assert result == {}

    @pytest.mark.asyncio
    @patch(
        "line_simu.engine.calculation.get_active_formulas",
        new_callable=AsyncMock,
    )
    async def test_multiple_formulas_ordered_by_display_order(self, mock_get_formulas):
        f1 = _make_formula(
            name="second",
            expression="10 + 20",
            variables={},
            display_order=2,
            result_label="Second",
        )
        f2 = _make_formula(
            name="first",
            expression="1 + 2",
            variables={},
            display_order=1,
            result_label="First",
        )
        mock_get_formulas.return_value = [f1, f2]

        session = _make_session()
        result = await run_calculations(session)

        assert "first" in result
        assert "second" in result
        assert result["first"]["value"] == 3.0
        assert result["second"]["value"] == 30.0

    @pytest.mark.asyncio
    @patch(
        "line_simu.engine.calculation.get_active_formulas",
        new_callable=AsyncMock,
    )
    async def test_failed_formula_continues_others(self, mock_get_formulas):
        good_formula = _make_formula(
            name="good",
            expression="100 + 200",
            variables={},
            display_order=1,
            result_label="Good",
        )
        bad_formula = _make_formula(
            name="bad",
            expression="missing_var",
            variables={
                "missing_var": {"source": "answer", "question_key": "nonexistent"},
            },
            display_order=2,
            result_label="Bad",
        )
        mock_get_formulas.return_value = [good_formula, bad_formula]

        session = _make_session()

        with patch(
            "line_simu.engine.calculation.get_answer_by_question_key",
            new_callable=AsyncMock,
            return_value=None,
        ):
            result = await run_calculations(session)

        assert result["good"]["value"] == 300.0
        assert result["good"]["formatted"] == "300円"
        assert result["bad"]["value"] is None
        assert "error" in result["bad"]

    @pytest.mark.asyncio
    @patch(
        "line_simu.engine.calculation.get_active_formulas",
        new_callable=AsyncMock,
    )
    async def test_result_includes_formatted_currency(self, mock_get_formulas):
        formula = _make_formula(
            name="compensation",
            expression="4300 * 60",
            variables={},
            display_order=1,
            result_label="Compensation",
        )
        mock_get_formulas.return_value = [formula]
        session = _make_session()
        result = await run_calculations(session)

        assert result["compensation"]["value"] == 258000.0
        assert result["compensation"]["formatted"] == "258,000円"
        assert result["compensation"]["label"] == "Compensation"


# ========================================================================
# format_value tests
# ========================================================================
class TestFormatValue:
    def test_integer_yen(self):
        assert format_value(Decimal("258000"), "円") == "258,000円"

    def test_zero(self):
        assert format_value(Decimal("0"), "円") == "0円"

    def test_large_value(self):
        assert format_value(Decimal("15000000"), "円") == "15,000,000円"

    def test_truncates_to_integer(self):
        assert format_value(Decimal("1234.56"), "円") == "1,235円"

    def test_man_yen_integer(self):
        assert format_value(Decimal("500000"), "万円", scale=10000, decimals=1) == "50万円"

    def test_man_yen_decimal(self):
        assert format_value(Decimal("503000"), "万円", scale=10000, decimals=1) == "50.3万円"

    def test_custom_unit_points(self):
        assert format_value(Decimal("850"), "点") == "850点"

    def test_custom_unit_kg_with_decimals(self):
        assert format_value(Decimal("725"), "kg", scale=10, decimals=1) == "72.5kg"

    def test_trailing_zeros_trimmed(self):
        assert format_value(Decimal("100"), "万円", scale=10000, decimals=2) == "1万円"


# ========================================================================
# resolve_variable tests
# ========================================================================
class TestResolveVariable:
    @pytest.mark.asyncio
    async def test_constant_source(self):
        result = await resolve_variable(
            {"source": "constant", "value": 4300}, uuid4()
        )
        assert result == 4300.0

    @pytest.mark.asyncio
    async def test_unknown_source_raises(self):
        with pytest.raises(CalculationError, match="Unknown variable source"):
            await resolve_variable({"source": "magic"}, uuid4())
