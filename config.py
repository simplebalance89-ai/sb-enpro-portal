"""
EnPro Filtration Mastermind Portal — Configuration
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

    # Local paths
    SESSION_DIR: str = Field(default="data/sessions", description="Session storage directory")
    AUDIT_LOG: str = Field(default="data/audit.jsonl", description="Audit log file path")

    # Server
    HOST: str = Field(default="0.0.0.0", description="Server bind host")
    PORT: int = Field(default=8000, description="Server bind port")

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


@lru_cache()
def get_settings() -> Settings:
    return Settings()


# Module-level instance for direct import
settings = get_settings()
