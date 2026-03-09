from linebot.v3.messaging import (
    AsyncApiClient,
    AsyncMessagingApi,
    Configuration,
)

# Per-channel client cache: {channel_id: AsyncMessagingApi}
_client_cache: dict[str, AsyncMessagingApi] = {}


def get_messaging_api(channel_access_token: str) -> AsyncMessagingApi:
    """Return a cached AsyncMessagingApi for the given channel access token."""
    if channel_access_token not in _client_cache:
        config = Configuration(access_token=channel_access_token)
        api_client = AsyncApiClient(config)
        _client_cache[channel_access_token] = AsyncMessagingApi(api_client)
    return _client_cache[channel_access_token]
