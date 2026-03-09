import logging
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)


async def push_answers_to_spreadsheet(
    gas_webhook_url: str,
    line_user_id: str,
    display_name: str | None,
    answers: dict[str, dict],
) -> None:
    """Push session answers to Google Spreadsheet via GAS webhook (best-effort).

    Args:
        gas_webhook_url: GAS web app URL for this channel
        line_user_id: LINE user ID string (e.g. "U1234abc...")
        display_name: LINE display name, or None if unknown
        answers: {question_key: {"label": str, "value": str}, ...}
    """
    if not gas_webhook_url:
        return

    payload = {
        "line_user_id": line_user_id,
        "display_name": display_name or "",
        "answered_at": datetime.now(timezone.utc).astimezone().isoformat(),
        "answers": answers,
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(gas_webhook_url, json=payload)
            if resp.status_code != 200:
                logger.warning("GAS webhook returned status %s", resp.status_code)
    except Exception as exc:
        logger.warning("Failed to push answers to spreadsheet: %s", exc)
