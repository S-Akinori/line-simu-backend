import asyncio
import logging
import re
from uuid import UUID

from linebot.v3.messaging import PushMessageRequest, ReplyMessageRequest

from line_simu.db.repositories.answer import (
    get_session_answers_by_key,
    get_session_answers_with_labels,
    save_answer,
)
from line_simu.db.repositories.channel import LineChannel
from line_simu.db.repositories.question import (
    get_option_by_value,
    get_question_by_id,
    get_question_options,
)
from line_simu.db.repositories.route import (
    get_first_route,
    get_first_route_question,
)
from line_simu.db.repositories.session import (
    complete_session,
    get_or_create_session,
    reset_and_create_session,
    update_session_route_and_question,
)
from line_simu.db.repositories.result_config import (
    get_configs_triggered_by_route,
    get_end_configs,
)
from line_simu.engine.calculation import run_calculations
from line_simu.engine.condition import check_display_conditions
from line_simu.engine.question import resolve_next
from line_simu.line.client import get_messaging_api
from line_simu.line.messages import (
    build_button_message,
    build_carousel_message,
    build_text_message,
)
from line_simu.schemas.question import Question
from line_simu.services.notification import notify_admin_completion
from line_simu.services.spreadsheet import push_answers_to_spreadsheet
from line_simu.services.user import get_line_user_display_name, get_line_user_uuid

logger = logging.getLogger(__name__)


async def start_session(
    line_user_id: str,
    channel: LineChannel,
    reply_token: str | None = None,
) -> None:
    """Create a new session and send the first question."""
    user_uuid = await get_line_user_uuid(line_user_id, channel.id)
    if user_uuid is None:
        logger.error("User not found: %s (channel: %s)", line_user_id, channel.name)
        return

    session = await get_or_create_session(user_uuid)

    route = await get_first_route(channel.id)
    if route is None:
        logger.error("No routes configured for channel: %s", channel.name)
        return
    first_question = await get_first_route_question(route.id)
    if first_question is None:
        logger.error("No active questions in first route for channel: %s", channel.name)
        return

    await update_session_route_and_question(session.id, route.id, first_question.id)
    api = get_messaging_api(channel.channel_access_token)
    await _send_all(api, line_user_id, await _build_question_message(first_question), reply_token)


async def restart_session(
    line_user_id: str,
    channel: LineChannel,
    start_route_id: UUID | None = None,
    reply_token: str | None = None,
) -> None:
    """Abandon any in-progress session and start fresh from the specified (or first) route."""
    user_uuid = await get_line_user_uuid(line_user_id, channel.id)
    if user_uuid is None:
        logger.error("User not found: %s (channel: %s)", line_user_id, channel.name)
        return

    session = await reset_and_create_session(user_uuid)

    if start_route_id is not None:
        route_id = start_route_id
    else:
        route = await get_first_route(channel.id)
        if route is None:
            logger.error("No routes configured for channel: %s", channel.name)
            return
        route_id = route.id

    first_question = await get_first_route_question(route_id)
    if first_question is None:
        logger.error("No active questions in route %s", route_id)
        return

    await update_session_route_and_question(session.id, route_id, first_question.id)
    api = get_messaging_api(channel.channel_access_token)
    await _send_all(api, line_user_id, await _build_question_message(first_question), reply_token)


async def process_answer(
    line_user_id: str,
    answer_value: str,
    channel: LineChannel,
    reply_token: str | None = None,
) -> None:
    """Core flow: receive answer -> save -> determine next -> send."""
    user_uuid = await get_line_user_uuid(line_user_id, channel.id)
    if user_uuid is None:
        logger.error("User not found: %s (channel: %s)", line_user_id, channel.name)
        return

    session = await get_or_create_session(user_uuid)
    if session.current_question_id is None:
        await start_session(line_user_id, channel, reply_token=reply_token)
        return

    current_question = await get_question_by_id(session.current_question_id)
    if current_question is None:
        logger.error("Current question not found: %s", session.current_question_id)
        return

    api = get_messaging_api(channel.channel_access_token)

    # 1. Validate free_text answer against question.validation rules
    if current_question.question_type == "free_text" and current_question.validation:
        error_msg = _validate_free_text(answer_value, current_question.validation)
        if error_msg:
            await _send_all(
                api, line_user_id,
                [build_text_message(error_msg), *await _build_question_message(current_question)],
                reply_token,
            )
            return

    # 2. Check for error message on selected option (carousel/button only)
    if current_question.question_type in ("image_carousel", "button"):
        selected_option = await get_option_by_value(current_question.id, answer_value)
        if selected_option and selected_option.error_message:
            await _send_all(
                api, line_user_id,
                [build_text_message(selected_option.error_message), *await _build_question_message(current_question)],
                reply_token,
            )
            return

    # 3. Parse numeric value if possible
    numeric_value = _try_parse_numeric(answer_value)

    # 4. Save answer
    await save_answer(session.id, current_question.id, answer_value, numeric_value)

    # 4b. Push current answers to spreadsheet (fire-and-forget)
    asyncio.create_task(
        _push_to_spreadsheet(line_user_id, session.id, channel)
    )

    # 5. Determine next (route_id, question)
    next_result = await resolve_next(session, current_question, session.current_route_id, channel.id)

    # Collect all outgoing messages, then send together via reply_message (up to 5)
    messages: list = []

    if next_result is None:
        # No more questions → determine which configs to show first,
        # then calculate only the formulas those templates reference.
        # Pre-load session answers once; reused by config filtering and calculations.
        session_answers = await get_session_answers_by_key(session.id)

        # Route-triggered result (highest priority)
        active_configs: list = []
        if session.current_route_id:
            route_configs = await get_configs_triggered_by_route(session.current_route_id)
            route_configs = await _filter_configs_by_condition(route_configs, session.id, session_answers)
            active_configs = route_configs

        # End-of-flow result (fallback)
        if not active_configs:
            end_configs = await get_end_configs(channel.id)
            end_configs = await _filter_configs_by_condition(end_configs, session.id, session_answers)
            active_configs = end_configs

        filter_names = _extract_formula_names(active_configs)
        logger.debug(
            "Session %s end: %d active configs, filter_names=%s",
            session.id, len(active_configs),
            sorted(filter_names) if filter_names is not None else "ALL (no template set)",
        )
        # Only run calculations when there are configs to display.
        # When active_configs is empty, no result needs to be shown.
        result = await run_calculations(session, channel.id, filter_names=filter_names) if active_configs else {}
        await complete_session(session.id, result)

        for cfg in active_configs:
            messages.extend(_build_result_messages(
                result,
                intro_message=cfg.intro_message,
                closing_message=cfg.closing_message,
                body_template=cfg.body_template,
            ))

        await _send_all(api, line_user_id, messages, reply_token)
        await notify_admin_completion(session, result, channel)
    else:
        next_route_id, next_question = next_result

        # Route completed → collect route-triggered result messages
        if session.current_route_id and next_route_id != session.current_route_id:
            route_configs = await get_configs_triggered_by_route(session.current_route_id)
            # Pre-load session answers once; reused by config filtering and calculations.
            session_answers = await get_session_answers_by_key(session.id)
            route_configs = await _filter_configs_by_condition(route_configs, session.id, session_answers)
            if route_configs:
                filter_names = _extract_formula_names(route_configs)
                logger.debug(
                    "Session %s route transition: %d configs, filter_names=%s",
                    session.id, len(route_configs),
                    sorted(filter_names) if filter_names is not None else "ALL (no template set)",
                )
                result = await run_calculations(session, channel.id, filter_names=filter_names)
                for cfg in route_configs:
                    messages.extend(_build_result_messages(
                        result,
                        intro_message=cfg.intro_message,
                        closing_message=cfg.closing_message,
                        body_template=cfg.body_template,
                    ))

        messages.extend(await _build_question_message(next_question))
        await update_session_route_and_question(session.id, next_route_id, next_question.id)
        await _send_all(api, line_user_id, messages, reply_token)


# ---------------------------------------------------------------------------
# Public helpers (may be used by scheduler etc.)
# ---------------------------------------------------------------------------

async def send_question_message(
    line_user_id: str,
    question: Question,
    channel: LineChannel,
    reply_token: str | None = None,
) -> None:
    """Build and send a LINE message for a question."""
    api = get_messaging_api(channel.channel_access_token)
    await _send_all(api, line_user_id, await _build_question_message(question), reply_token)


async def send_result_message(
    line_user_id: str,
    result: dict,
    channel: LineChannel,
    intro_message: str = "シミュレーション結果をお知らせします。",
    closing_message: str | None = None,
    body_template: str | None = None,
    reply_token: str | None = None,
) -> None:
    """Send calculation results to the user."""
    api = get_messaging_api(channel.channel_access_token)
    messages = _build_result_messages(result, intro_message, closing_message, body_template)
    await _send_all(api, line_user_id, messages, reply_token)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

async def _build_question_message(question: Question) -> list[object]:
    """Build LINE message objects for a question (without sending)."""
    options = await get_question_options(question.id)
    option_dicts = [
        {"label": opt.label, "value": opt.value, "image_url": opt.image_url}
        for opt in options
    ]
    if question.question_type == "image_carousel" and option_dicts:
        return [build_text_message(question.content), build_carousel_message(option_dicts)]
    if question.question_type == "button" and option_dicts:
        return [build_button_message(question.content, option_dicts, question.image_url)]
    return [build_text_message(question.content)]


def _build_result_messages(
    result: dict,
    intro_message: str = "シミュレーション結果をお知らせします。",
    closing_message: str | None = None,
    body_template: str | None = None,
) -> list:
    """Build LINE message objects for a result config (without sending)."""
    if body_template is not None:
        body = body_template
        for formula_name, item in result.items():
            if item.get("error"):
                replacement = "計算できませんでした"
            else:
                replacement = item.get("formatted", str(item.get("value", "N/A")))
            body = body.replace(f"{{{formula_name}}}", replacement)
        text = intro_message + "\n\n" + body
    else:
        lines = [intro_message + "\n"]
        for _name, item in result.items():
            if item.get("error"):
                # Skip formulas that could not be calculated — they are likely
                # not relevant to the current route and should not be displayed.
                continue
            label = item.get("label", _name)
            fmt = item.get("result_format", "{label}: {value}")
            formatted_value = item.get("formatted", str(item.get("value", "N/A")))
            lines.append(fmt.replace("{label}", str(label)).replace("{value}", formatted_value))
        text = "\n".join(lines)

    messages = [build_text_message(text)]
    if closing_message is not None:
        messages.append(build_text_message(closing_message))
    return messages


async def _filter_configs_by_condition(
    configs: list,
    session_id: UUID,
    answers_by_key: dict | None = None,
) -> list:
    """Return only configs whose condition matches the session answers (or have no condition)."""
    if not any(getattr(cfg, "condition", None) for cfg in configs):
        return configs
    if answers_by_key is None:
        answers_by_key = await get_session_answers_by_key(session_id)
    return [
        cfg for cfg in configs
        if not cfg.condition or check_display_conditions(cfg.condition, answers_by_key)
    ]


async def _send_all(
    api: object,
    line_user_id: str,
    messages: list,
    reply_token: str | None,
) -> None:
    """Send messages: first batch via reply_message (if token available), rest via push_message."""
    if not messages:
        return
    if reply_token:
        await api.reply_message(
            ReplyMessageRequest(reply_token=reply_token, messages=messages[:5])
        )
        remaining = messages[5:]
    else:
        remaining = messages
    for i in range(0, len(remaining), 5):
        await api.push_message(
            PushMessageRequest(to=line_user_id, messages=remaining[i : i + 5])
        )


def _try_parse_numeric(value: str) -> float | None:
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _extract_formula_names(configs: list) -> set[str] | None:
    """Extract formula names referenced as {name} in body_templates.

    Only configs that have body_template set contribute formula names.
    If NO config has body_template (all NULL), returns None so all active
    formulas are calculated for the default full-list display.
    """
    templates = [
        cfg.body_template
        for cfg in configs
        if cfg.body_template is not None
    ]
    if not templates:
        return None  # no templates set → need all formulas for default display

    names: set[str] = set()
    for template in templates:
        names.update(re.findall(r"\{(\w+)\}", template))
    return names


async def _push_to_spreadsheet(
    line_user_id: str,
    session_id: UUID,
    channel: LineChannel,
) -> None:
    """Fetch current session answers and push to spreadsheet (best-effort)."""
    if not channel.gas_webhook_url:
        return
    try:
        answers = await get_session_answers_with_labels(session_id)
        display_name = await get_line_user_display_name(line_user_id, channel.id)
        await push_answers_to_spreadsheet(channel.gas_webhook_url, line_user_id, display_name, answers)
    except Exception:
        logger.warning("Spreadsheet push failed for session %s", session_id, exc_info=True)


def _validate_free_text(value: str, validation: dict) -> str | None:
    """Return error message string if invalid, None if valid."""
    if validation.get("type") != "numeric":
        return None

    try:
        num = float(value)
    except (ValueError, TypeError):
        return "数字を入力してください。"

    min_val = validation.get("min")
    max_val = validation.get("max")

    if min_val is not None and num < float(min_val):
        label = int(min_val) if float(min_val) == int(min_val) else min_val
        return f"{label}以上の数字を入力してください。"

    if max_val is not None and num > float(max_val):
        label = int(max_val) if float(max_val) == int(max_val) else max_val
        return f"{label}以下の数字を入力してください。"

    return None
