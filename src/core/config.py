from pathlib import Path
from logging import config as logging_config

from core.logger import LOGGING
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


logging_config.dictConfig(LOGGING)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).parent.parent.parent / '.env'),
        env_file_encoding='utf-8',
    )

    project_name: str = Field(..., alias='PROJECT_NAME')

    redis_host: str = Field(..., alias='REDIS_HOST')
    redis_port: int = Field(..., alias='REDIS_PORT')
    redis_max_connections: int = Field(..., alias='REDIS_MAX_CONNECTIONS')
    redis_connect_timeout: int = Field(..., alias='REDIS_CONNECT_TIMEOUT')

    elastic_host: str = Field(..., alias='ELASTIC_HOST')
    elastic_port: int = Field(..., alias='ELASTIC_PORT')
    elastic_max_connections: int = Field(..., alias='ELASTIC_MAX_CONNECTIONS')
    elastic_timeout: int = Field(..., alias='ELASTIC_TIMEOUT')

    base_dir: Path = Field(default_factory=lambda: Path(__file__).parent.parent)


settings = Settings()
