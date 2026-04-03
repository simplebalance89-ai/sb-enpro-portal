"""
Enpro Filtration Mastermind Portal — Configuration
Pydantic Settings with env var loading.
"""

from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache


class Settings(BaseSettings):
    # Azure OpenAI
    AZURE_OPENAI_ENDPOINT: str = Field(default="", description="Azure OpenAI endpoint URL")
    AZURE_OPENAI_KEY: str = Field(default="", description="Azure OpenAI API key")
    AZURE_OPENAI_API_VERSION: str = Field(default="2024-12-01-preview", description="Azure OpenAI API version")

    # Deployments
    AZURE_DEPLOYMENT_ROUTER: str = Field(default="gpt-4.1-mini", description="Router model deployment name")
    AZURE_DEPLOYMENT_REASONING: str = Field(default="gpt-4.1", description="Reasoning model deployment name")

    # Azure Blob Storage
    AZURE_BLOB_SAS: str = Field(default="", description="SAS token for Azure Blob access")

    # Azure Whisper STT (separate resource — northcentralus)
    AZURE_WHISPER_ENDPOINT: str = Field(default="", description="Azure Whisper endpoint URL (separate from main OpenAI)")
    AZURE_WHISPER_KEY: str = Field(default="", description="Azure Whisper API key")
    AZURE_WHISPER_DEPLOYMENT: str = Field(default="whisper", description="Azure Whisper deployment name")
    AZURE_WHISPER_API_VERSION: str = Field(default="2024-12-01-preview", description="Azure Whisper API version")

    # Local paths
    SESSION_DIR: str = Field(default="data/sessions", description="Session storage directory")
    AUDIT_LOG: str = Field(default="data/audit.jsonl", description="Audit log file path")

    # Server
    HOST: str = Field(default="0.0.0.0", description="Server bind host")
    PORT: int = Field(default=8000, description="Server bind port")

    # Security
    ADMIN_TOKEN: str = Field(default="", description="Admin API token for sensitive endpoints")

    # SMTP / Email
    smtp_host: str = Field(default="", alias="SMTP_HOST", description="SMTP server host")
    smtp_port: int = Field(default=587, alias="SMTP_PORT", description="SMTP server port")
    smtp_user: str = Field(default="", alias="SMTP_USER", description="SMTP username")
    smtp_pass: str = Field(default="", alias="SMTP_PASS", description="SMTP password")
    report_email: str = Field(default="pwnetsuite@outlook.com", alias="REPORT_EMAIL", description="Report recipient email")

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "populate_by_name": True}


@lru_cache()
def get_settings() -> Settings:
    return Settings()


# Module-level instance for direct import
settings = get_settings()
