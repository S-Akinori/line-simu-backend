from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class Session(BaseModel):
    id: UUID
    line_user_id: UUID
    status: str
    current_question_id: UUID | None = None
    current_route_id: UUID | None = None
    last_reminder_sent_at: datetime | None = None
    reminder_count: int = 0
    result: dict | None = None
    started_at: datetime
    completed_at: datetime | None = None
    abandoned_at: datetime | None = None
    created_at: datetime
    updated_at: datetime
