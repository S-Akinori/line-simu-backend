from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.webhook import WebhookParser
from linebot.v3.webhooks import (
    FollowEvent,
    MessageEvent,
    PostbackEvent,
    TextMessageContent,
    UnfollowEvent,
)

from line_simu.db.repositories.channel import LineChannel, get_channel_by_webhook_path
from line_simu.line.events import (
    handle_follow_event,
    handle_message_event,
    handle_postback_event,
    handle_unfollow_event,
)

router = APIRouter()


@router.post("/webhook/{channel_path}")
async def webhook_handler(
    channel_path: str,
    request: Request,
    background_tasks: BackgroundTasks,
) -> dict[str, str]:
    # 1. Read body immediately before any async DB calls to avoid ClientDisconnect
    # if LINE closes the connection while we're waiting for the DB.
    signature = request.headers.get("X-Line-Signature", "")
    body_bytes = await request.body()
    body_str = body_bytes.decode("utf-8")

    # 2. Load channel by webhook_path
    channel = await get_channel_by_webhook_path(channel_path)
    if channel is None:
        raise HTTPException(status_code=404, detail="Channel not found")

    parser = WebhookParser(channel.channel_secret)
    try:
        events = parser.parse(body_str, signature)
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    # 3. Dispatch events with channel context
    for event in events:
        if isinstance(event, MessageEvent) and isinstance(
            event.message, TextMessageContent
        ):
            background_tasks.add_task(handle_message_event, event, channel)
        elif isinstance(event, PostbackEvent):
            background_tasks.add_task(handle_postback_event, event, channel)
        elif isinstance(event, FollowEvent):
            background_tasks.add_task(handle_follow_event, event, channel)
        elif isinstance(event, UnfollowEvent):
            background_tasks.add_task(handle_unfollow_event, event, channel)

    return {"status": "ok"}
