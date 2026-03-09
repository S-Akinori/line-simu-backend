import logging
import urllib.parse

from linebot.v3.webhooks import (
    FollowEvent,
    MessageEvent,
    PostbackEvent,
    UnfollowEvent,
)

from line_simu.db.repositories.channel import LineChannel
from line_simu.engine.session import process_answer, restart_session, start_session
from line_simu.line.client import get_messaging_api
from line_simu.services.user import mark_user_unfollowed, upsert_line_user

logger = logging.getLogger(__name__)


async def _fetch_display_name(line_user_id: str, channel: LineChannel) -> str | None:
    """Fetch the display name from LINE Profile API. Returns None on failure."""
    try:
        api = get_messaging_api(channel.channel_access_token)
        profile = await api.get_profile(line_user_id)
        return profile.display_name
    except Exception:
        logger.debug("Could not fetch LINE profile for %s", line_user_id)
        return None


async def handle_message_event(event: MessageEvent, channel: LineChannel) -> None:
    line_user_id = event.source.user_id
    text = event.message.text
    display_name = await _fetch_display_name(line_user_id, channel)
    await upsert_line_user(line_user_id, channel.id, display_name=display_name)
    if text.strip() in channel.start_keywords:
        start_route_id = channel.start_keyword_routes.get(text.strip())
        await restart_session(
            line_user_id, channel,
            start_route_id=start_route_id,
            reply_token=event.reply_token,
        )
        return
    await process_answer(line_user_id, text, channel, reply_token=event.reply_token)


async def handle_postback_event(event: PostbackEvent, channel: LineChannel) -> None:
    line_user_id = event.source.user_id
    data = dict(urllib.parse.parse_qsl(event.postback.data))
    answer_value = data.get("answer", event.postback.data)
    display_name = await _fetch_display_name(line_user_id, channel)
    await upsert_line_user(line_user_id, channel.id, display_name=display_name)
    await process_answer(line_user_id, answer_value, channel, reply_token=event.reply_token)


async def handle_follow_event(event: FollowEvent, channel: LineChannel) -> None:
    line_user_id = event.source.user_id
    display_name = await _fetch_display_name(line_user_id, channel)
    await upsert_line_user(line_user_id, channel.id, display_name=display_name)
    await start_session(line_user_id, channel, reply_token=event.reply_token)


async def handle_unfollow_event(event: UnfollowEvent, channel: LineChannel) -> None:
    await mark_user_unfollowed(event.source.user_id, channel.id)
