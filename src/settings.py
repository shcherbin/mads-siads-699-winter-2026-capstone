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

    data_base_path: str = '/workspaces/mads-siads-699-winter-2026-capstone/notebooks/data'

    @property
    def source_data_path(self) -> str:
        return f"{self.data_base_path}/source_data"

    @property
    def augmented_data_path(self) -> str:
        return f"{self.data_base_path}/augmented_data"

    @property
    def features_data_path(self) -> str:
        return f"{self.augmented_data_path}/features"

    @property
    def initial_dataset_path(self) -> str:
        return f'{self.source_data_path}/initial_dataset'

    @property
    def dependency_edges_path(self) -> str:
        return f'{self.augmented_data_path}/package_dependency_edges.parquet'

    @property
    def feature_dependency_count_with_version_path(self) -> str:
        return f'{self.features_data_path}/feature_dependency_count_with_version.parquet'

    @property
    def feature_dependency_count_without_version_path(self) -> str:
        return f'{self.features_data_path}/feature_dependency_count_without_version.parquet'

    @property
    def feature_total_downloads_path(self) -> str:
        return f'{self.source_data_path}/pypi_file_downloads'

    @property
    def feature_repo_age_and_staleness_path(self) -> str:
        return f'{self.features_data_path}/feature_repo_age_and_staleness.parquet'


def load_settings() -> Settings:
    return Settings()
