from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel


class Answer(BaseModel):
    id: UUID
    session_id: UUID
    question_id: UUID
    answer_value: str
    answer_numeric: Decimal | None = None
    answered_at: datetime
