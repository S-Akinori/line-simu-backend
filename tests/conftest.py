import os

# Set test environment variables BEFORE any line_simu imports
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost:5432/test")
os.environ.setdefault(
    "DATABASE_URL_SYNC", "postgresql+psycopg2://test:test@localhost:5432/test"
)
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-key")
os.environ.setdefault("LINE_CHANNEL_SECRET", "test-channel-secret")
os.environ.setdefault("LINE_CHANNEL_ACCESS_TOKEN", "test-channel-access-token")

import pytest
from uuid import uuid4


@pytest.fixture
def session_id():
    return uuid4()


@pytest.fixture
def sample_answers():
    return {
        "was_hospitalized": "yes",
        "hospitalization_days": "30",
        "injury_grade": "grade_7",
        "daily_income": "10000",
    }
