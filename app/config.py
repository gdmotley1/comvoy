from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    supabase_url: str = ""
    supabase_key: str = ""  # anon key (for RLS-gated access)
    supabase_service_key: str = ""  # service role key (for ETL / admin ops)
    anthropic_api_key: str = ""
    google_maps_api_key: str = ""
    reports_dir: str = "./data/reports"

    # --- Token budget guardrails ---
    agent_model: str = "claude-sonnet-4-20250514"
    agent_max_tokens: int = 2048          # max output tokens per API call
    agent_max_loop: int = 5               # max tool-use iterations per request
    agent_max_history: int = 20           # max messages kept in conversation
    agent_tool_result_cap: int = 8000     # max chars per tool result
    agent_search_limit: int = 10          # default dealer search results

    # --- SMTP email (for auto-briefing notifications) ---
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from: str = "Otto <otto@comvoy.com>"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
