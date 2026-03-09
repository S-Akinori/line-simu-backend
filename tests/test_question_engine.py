from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from line_simu.engine.question import resolve_next_question
from line_simu.schemas.question import Question
from line_simu.schemas.session import Session


def _make_session(**overrides) -> Session:
    defaults = {
        "id": uuid4(),
        "line_user_id": uuid4(),
        "status": "in_progress",
        "current_question_id": uuid4(),
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


def _make_question(**overrides) -> Question:
    defaults = {
        "id": uuid4(),
        "question_key": "test_question",
        "question_type": "button",
        "content": "Test question?",
        "sort_order": 10,
        "conditions": [],
        "validation": {},
        "is_active": True,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }
    defaults.update(overrides)
    return Question(**defaults)


class TestResolveNextQuestion:
    @pytest.mark.asyncio
    @patch("line_simu.engine.question.get_session_answers_by_key", new_callable=AsyncMock)
    @patch("line_simu.engine.question.get_question_by_key", new_callable=AsyncMock)
    async def test_condition_match_overrides_sort_order(
        self, mock_get_by_key, mock_get_answers
    ):
        session = _make_session()
        current_q = _make_question(
            question_key="was_hospitalized",
            sort_order=10,
            conditions=[
                {
                    "id": "c1",
                    "rules": [
                        {
                            "question_key": "was_hospitalized",
                            "operator": "eq",
                            "value": "yes",
                        }
                    ],
                    "logic": "and",
                    "next_question_key": "hospitalization_days",
                }
            ],
        )
        next_q = _make_question(question_key="hospitalization_days", sort_order=20)

        mock_get_answers.return_value = {"was_hospitalized": "yes"}
        mock_get_by_key.return_value = next_q

        result = await resolve_next_question(session, current_q)

        assert result == next_q
        mock_get_by_key.assert_called_once_with("hospitalization_days")

    @pytest.mark.asyncio
    @patch("line_simu.engine.question.get_session_answers_by_key", new_callable=AsyncMock)
    @patch("line_simu.engine.question.get_next_question_by_order", new_callable=AsyncMock)
    async def test_no_condition_falls_back_to_sort_order(
        self, mock_get_next, mock_get_answers
    ):
        session = _make_session()
        current_q = _make_question(
            question_key="simple_q",
            sort_order=10,
            conditions=[
                {
                    "id": "c1",
                    "rules": [
                        {
                            "question_key": "color",
                            "operator": "eq",
                            "value": "red",
                        }
                    ],
                    "logic": "and",
                    "next_question_key": "red_path",
                }
            ],
        )
        fallback_q = _make_question(question_key="next_by_order", sort_order=20)

        mock_get_answers.return_value = {"color": "blue"}
        mock_get_next.return_value = fallback_q

        result = await resolve_next_question(session, current_q)

        assert result == fallback_q
        mock_get_next.assert_called_once_with(10)

    @pytest.mark.asyncio
    @patch("line_simu.engine.question.get_session_answers_by_key", new_callable=AsyncMock)
    @patch("line_simu.engine.question.get_next_question_by_order", new_callable=AsyncMock)
    async def test_empty_conditions_uses_sort_order(
        self, mock_get_next, mock_get_answers
    ):
        session = _make_session()
        current_q = _make_question(
            question_key="no_cond_q",
            sort_order=5,
            conditions=[],
        )
        next_q = _make_question(question_key="next_q", sort_order=10)

        mock_get_answers.return_value = {}
        mock_get_next.return_value = next_q

        result = await resolve_next_question(session, current_q)

        assert result == next_q
        mock_get_next.assert_called_once_with(5)

    @pytest.mark.asyncio
    @patch("line_simu.engine.question.get_session_answers_by_key", new_callable=AsyncMock)
    @patch("line_simu.engine.question.get_next_question_by_order", new_callable=AsyncMock)
    async def test_no_next_question_returns_none(
        self, mock_get_next, mock_get_answers
    ):
        session = _make_session()
        current_q = _make_question(
            question_key="last_q",
            sort_order=100,
            conditions=[],
        )

        mock_get_answers.return_value = {}
        mock_get_next.return_value = None

        result = await resolve_next_question(session, current_q)

        assert result is None

    @pytest.mark.asyncio
    @patch("line_simu.engine.question.get_session_answers_by_key", new_callable=AsyncMock)
    @patch("line_simu.engine.question.get_next_question_by_order", new_callable=AsyncMock)
    async def test_condition_evaluation_error_falls_back_to_sort_order(
        self, mock_get_next, mock_get_answers
    ):
        """If condition evaluation raises, log warning and fall back to sort_order."""
        session = _make_session()
        current_q = _make_question(
            question_key="bad_cond_q",
            sort_order=10,
            conditions=[
                {
                    "id": "c1",
                    "rules": [
                        {
                            "question_key": "age",
                            "operator": "gt",
                            "value": 18,
                        }
                    ],
                    "logic": "and",
                    "next_question_key": "adult_q",
                }
            ],
        )
        fallback_q = _make_question(question_key="fallback_q", sort_order=20)

        # "not_a_number" will cause ConditionEvaluationError in gt
        mock_get_answers.return_value = {"age": "not_a_number"}
        mock_get_next.return_value = fallback_q

        result = await resolve_next_question(session, current_q)

        assert result == fallback_q
        mock_get_next.assert_called_once_with(10)

    @pytest.mark.asyncio
    @patch("line_simu.engine.question.get_session_answers_by_key", new_callable=AsyncMock)
    @patch("line_simu.engine.question.get_question_by_key", new_callable=AsyncMock)
    async def test_condition_match_returns_none_question_key_not_found(
        self, mock_get_by_key, mock_get_answers
    ):
        """If condition matches but target question doesn't exist, returns None."""
        session = _make_session()
        current_q = _make_question(
            question_key="q1",
            sort_order=10,
            conditions=[
                {
                    "id": "c1",
                    "rules": [
                        {
                            "question_key": "choice",
                            "operator": "eq",
                            "value": "a",
                        }
                    ],
                    "logic": "and",
                    "next_question_key": "nonexistent_key",
                }
            ],
        )

        mock_get_answers.return_value = {"choice": "a"}
        mock_get_by_key.return_value = None

        result = await resolve_next_question(session, current_q)

        assert result is None
