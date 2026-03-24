import polars as pl
from functools import cached_property

from settings import load_settings

SETTINGS = load_settings()


class FinalDatasetConstructor:

    @cached_property
    def df_initial_dataset(self) -> pl.DataFrame:
        init_df = pl.scan_parquet(SETTINGS.initial_dataset_path)
        unique_repos = (
            init_df
                .select(
                    pl.col("package_name"),
                    pl.col("github_repo"),
                )
                .unique()
                .collect()
        )
        return unique_repos

    @cached_property
    def df_feature_repo_age_and_commit_staleness(self) -> pl.DataFrame:
        return pl.read_parquet(f"{SETTINGS.feature_repo_age_and_staleness_path}")

    @cached_property
    def df_feature_dependency_count(self) -> pl.DataFrame:
        return pl.read_parquet(f"{SETTINGS.feature_dependency_count_without_version_path}")

    @cached_property
    def df_feature_total_downloads(self) -> pl.DataFrame:
        """Load the total downloads feature dataset and compute the mean total downloads per package.

        We compute the mean here since there are multiple entries for the same package (e.g., different versions),
        and we want a single value per package for the final dataset.
        """
        total_download_df = pl.scan_parquet(SETTINGS.feature_total_downloads_path)
        return (
            total_download_df
                .group_by("package_name")
                # TODO: kshcherb: check if computing the mean is the best approach here
                .agg(pl.col("total_downloads").mean().alias("total_downloads"))
                .collect()
        )

    def __call__(self) -> pl.DataFrame:
        """Merge the initial dataset with the feature datasets to create the final dataset.
        """
        merged_df = (
            self.df_initial_dataset
            .join(self.df_feature_repo_age_and_commit_staleness, on=["package_name", "github_repo"], how="left")
            .join(self.df_feature_dependency_count, on="package_name", how="left")
            .join(self.df_feature_total_downloads, on="package_name", how="left")
        )
        return merged_df




def main():
    construct = FinalDatasetConstructor()
    construct()



if __name__ == "__main__":
    main()
