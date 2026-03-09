from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class QuestionOption(BaseModel):
    id: UUID
    question_id: UUID
    label: str
    value: str
    image_url: str | None = None
    sort_order: int = 0
    error_message: str | None = None
    created_at: datetime


class Question(BaseModel):
    id: UUID
    question_key: str
    question_type: str
    content: str
    description: str | None = None
    sort_order: int = 0
    group_name: str | None = None
    parent_question_id: UUID | None = None
    conditions: list[dict] = []
    display_conditions: list[dict] = []
    validation: dict = {}
    image_url: str | None = None
    is_active: bool = True
    created_at: datetime
    updated_at: datetime
