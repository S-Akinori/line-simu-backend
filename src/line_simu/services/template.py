import re

_VARIABLE_PATTERN = re.compile(r"\{\{(\w+)\}\}")


def render_template(template: str, variables: dict[str, str]) -> str:
    """Replace {{variable}} placeholders with actual values.

    Unknown placeholders are left as-is.
    Supported variables: display_name, channel_name
    """
    def replace(match: re.Match) -> str:
        return variables.get(match.group(1), match.group(0))

    return _VARIABLE_PATTERN.sub(replace, template)
