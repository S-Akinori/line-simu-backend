from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    database_url_sync: str
    supabase_url: str
    supabase_service_role_key: str
    # LINE channel credentials are stored per-channel in the line_channels table.
    # No global LINE credentials needed here.

    # SMTP email notification (all optional; email is disabled if smtp_host is not set)
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_user: str | None = None
    smtp_password: str | None = None
    smtp_from: str | None = None
    admin_email: str | None = None


settings = Settings()
