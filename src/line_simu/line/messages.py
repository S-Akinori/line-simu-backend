from linebot.v3.messaging import (
    ButtonsTemplate,
    ImageCarouselColumn,
    ImageCarouselTemplate,
    PostbackAction,
    QuickReply,
    QuickReplyItem,
    TemplateMessage,
    TextMessage,
)


def build_text_message(text: str) -> TextMessage:
    return TextMessage(text=text)


def build_carousel_message(
    options: list[dict],
) -> TemplateMessage:
    """Build an image carousel template message from options.

    Each option dict should have: label, value, image_url.
    Image carousel shows only images (no title/text). Supports max 10 columns.
    """
    columns = []
    for opt in options[:10]:
        columns.append(
            ImageCarouselColumn(
                image_url=opt.get("image_url"),
                action=PostbackAction(
                    label=opt["label"][:20],
                    data=f"answer={opt['value']}",
                    display_text=opt["label"][:300],
                ),
            )
        )
    return TemplateMessage(
        alt_text="選択してください",
        template=ImageCarouselTemplate(columns=columns),
    )


def build_button_message(
    question_text: str,
    options: list[dict],
    image_url: str | None = None,
) -> TemplateMessage:
    """Build a button template message. Max 4 options."""
    actions = []
    for opt in options[:4]:
        actions.append(
            PostbackAction(
                label=opt["label"][:20],
                data=f"answer={opt['value']}",
                display_text=opt["label"][:300],
            )
        )
    return TemplateMessage(
        alt_text=question_text[:400],
        template=ButtonsTemplate(
            thumbnail_image_url=image_url,
            text=question_text[:160],
            actions=actions,
        ),
    )


def build_quick_reply_message(
    question_text: str,
    options: list[dict],
) -> TextMessage:
    """Build a text message with quick reply buttons."""
    items = []
    for opt in options[:13]:
        items.append(
            QuickReplyItem(
                action=PostbackAction(
                    label=opt["label"][:20],
                    data=f"answer={opt['value']}",
                )
            )
        )
    return TextMessage(
        text=question_text,
        quick_reply=QuickReply(items=items),
    )
