import dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file_encoding='utf-8',
        env_file=dotenv.find_dotenv(),
        env_prefix='mads_capstone_',
        extra='ignore',
    )

    env: str
    version: str


def load_settings() -> Settings:
    return Settings()
