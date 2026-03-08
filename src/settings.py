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

    augmented_data_base_path: str = (
        '/workspaces/mads-siads-699-winter-2026-capstone/notebooks/data/source_data/initial_dataset'
    )


def load_settings() -> Settings:
    return Settings()
