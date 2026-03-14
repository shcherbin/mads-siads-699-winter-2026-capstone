"""This script computes the dependency count feature for each package across all snapshots."""

import polars as pl
from loguru import logger

from settings import load_settings

SETTINGS = load_settings()


def load_initial_dataset() -> pl.LazyFrame:
    """Load the initial dataset.

    The initial dataset is the augmented data that contains all the dependencies of all packages across all snapshots.

    The dataset was constructed by running SQL scripts on the original data on Google BigQuery,
    and then exporting the results to Parquet files. You can find the SQL scripts in the `sql/initial_dataset` directory.

    Output DataFrame schema:
    Schema([
        ('SnapshotAt', Datetime(time_unit='us', time_zone=None)),
        ('package_name', String),
        ('package_version', String),
        ('package_published_at', Datetime(time_unit='us', time_zone=None)),
        ('dep_name', String),
        ('dep_version', String),
        ('MinimumDepth', Int64),
        ('github_repo', String)
    ])
    """
    df = pl.scan_parquet(SETTINGS.initial_dataset_path)
    return df


def compute_dependency_edges(frame: pl.LazyFrame) -> pl.LazyFrame:
    """Compute the dependency edges from the augmented data.
    A dependency edge is a direct dependency between a package and its dependency.

    Output DataFrame schema:
    Schema([
        ('package_name', String),
        ('package_version', String),
        ('dep_name', String),
        ('dep_version', String)
    ])
    """
    return (
        frame.filter(pl.col("MinimumDepth") == 1)  # only direct dependencies
        .select(["package_name", "package_version", "dep_name", "dep_version"])
        .drop_nulls(["package_name", "package_version", "dep_name", "dep_version"])
        .unique()  # removes duplicates across snapshots
    )


def compute_dependency_count_without_version(adopter_edges_frame: pl.LazyFrame) -> pl.LazyFrame:
    """Compute the dependency count of each package without considering the version.

    The dependency count of a package is defined as the number of direct dependencies it has across all snapshots.

    Output DataFrame schema:
    Schema([
        ('package_name', String),
        ('dependency_count', Int64)
    ])
    """

    return (
        adopter_edges_frame.select(
            [
                "package_name",
                "dep_name",
            ]
        )
        .group_by("dep_name")
        .agg(adopting_packages=pl.n_unique("package_name"))
        .sort("adopting_packages", descending=True)
        .rename({"dep_name": "package_name", "adopting_packages": "dependency_count"})
    )


def compute_dependency_count_with_version(adopter_edges_frame: pl.LazyFrame) -> pl.LazyFrame:
    """Compute the dependency count of each package considering the version.

    The dependency count of a package is defined as the number of direct dependencies it has across all snapshots.

    Output DataFrame schema:
    Schema([
        ('package_name', String),
        ('package_version', String),
        ('dependency_count', Int64)
    ])
    """
    return (
        adopter_edges_frame.select(
            [
                "package_name",
                "package_version",
                "dep_name",
                "dep_version",
            ]
        )
        .group_by(["dep_name", "dep_version"])
        .agg(adopting_packages=pl.n_unique("package_name"))
        .sort("adopting_packages", descending=True)
        .rename(
            {
                "dep_name": "package_name",
                "dep_version": "package_version",
                "adopting_packages": "dependency_count",
            }
        )
    )


def main():
    logger.info("Starting the computation of the `dependency_count` feature...")
    init_df = load_initial_dataset()
    dep_edges_df = compute_dependency_edges(init_df)
    dependency_count_with_version_df = compute_dependency_count_with_version(dep_edges_df)
    dependency_count_without_version_df = compute_dependency_count_without_version(dep_edges_df)

    logger.info("Computing dependency edges...")
    dep_edges_df.sink_parquet(SETTINGS.dependency_edges_path)
    logger.info(f"Saved dependency edges to {SETTINGS.dependency_edges_path}")

    logger.info("Computing dependency count considering version...")
    dependency_count_with_version_df.sink_parquet(SETTINGS.feature_dependency_count_with_version_path)
    logger.info(f"Saved dependency count with version to {SETTINGS.feature_dependency_count_with_version_path}")

    logger.info("Computing dependency count without considering version...")
    dependency_count_without_version_df.sink_parquet(SETTINGS.feature_dependency_count_without_version_path)
    logger.info(f"Saved dependency count without version to {SETTINGS.feature_dependency_count_without_version_path}")


if __name__ == "__main__":
    main()
