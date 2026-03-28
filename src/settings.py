from pathlib import Path

import dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict


def _repo_root() -> Path:
    """Resolves the repository root from this file's location, regardless of cwd."""
    return Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file_encoding='utf-8',
        env_file=dotenv.find_dotenv(),
        env_prefix='mads_capstone_',
        extra='ignore',
    )

    env: str
    version: str

    data_base_path: str = str(_repo_root() / 'notebooks' / 'data')

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
    def visualizations_path(self) -> str:
        return f"{self.augmented_data_path}/visualizations"

    @property
    def initial_dataset_path(self) -> str:
        return f'{self.source_data_path}/initial_dataset'

    @property
    def librariesio_path(self) -> str:
        return f'{self.source_data_path}/librariesio'

    @property
    def librariesio_parquet_path(self) -> str:
        return f'{self.librariesio_path}/librariesio.parquet'

    @property
    def mttu_mttr_path(self) -> str:
        return f'{self.augmented_data_path}/mttu_mttr_data'

    @property
    def pypi_scorecards_path(self) -> str:
        return f'{self.source_data_path}/pypi_scorecards'

    @property
    def scorecards_cli_results_path(self) -> str:
        return f'{self.source_data_path}/scorecards_cli/scorecard_results.jsonl'

    @property
    def unique_packages_path(self) -> str:
        return f'{self.augmented_data_path}/unique_packages.parquet'

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
    def feature_libraries_io_path(self) -> str:
        return f'{self.features_data_path}/feature_libraries_io.parquet'

    @property
    def feature_pypi_downloads_path(self) -> str:
        return f'{self.features_data_path}/feature_pypi_downloads.parquet'

    @property
    def feature_ossf_scorecard_path(self) -> str:
        return f'{self.features_data_path}/feature_ossf_scorecard.parquet'

    @property
    def feature_repo_contributions_and_size_path(self) -> str:
        return f'{self.features_data_path}/feature_repo_contributions_and_size'

    @property
    def feature_repo_age_and_staleness_path(self) -> str:
        return f'{self.features_data_path}/feature_repo_age_and_staleness.parquet'

    @property
    def final_dataset_path(self) -> str:
        return f'{self.augmented_data_path}/final_dataset.parquet'


def load_settings() -> Settings:
    return Settings()
