"""This script computes the contributions_count and size_in_kb features for each repository."""

import argparse
import os
from pathlib import Path
import polars as pl
from github import Github, Auth, UnknownObjectException, GithubException
from loguru import logger

from settings import load_settings

SETTINGS = load_settings()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GH = Github(auth=Auth.Token(token=GITHUB_TOKEN))


def load_initial_dataset() -> pl.DataFrame:
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


def get_contributions_and_size(full_name: str):
    try:
        repo = GH.get_repo(full_name)

        # Get total number of unique contributors
        contributors_count = repo.get_contributors().totalCount

        # Get repository size in KB (GitHub API returns size in KB)
        size_in_kb = repo.size

        return {"contributions_count": contributors_count, "size_in_kb": size_in_kb}
    except UnknownObjectException as e:
        logger.warning(f"Repository not found for {full_name}: {e}")
        return {"contributions_count": None, "size_in_kb": None}
    except GithubException as e:
        logger.error(f"GitHub error for {full_name}: {e}")
        return {"contributions_count": None, "size_in_kb": None}


def main(start_partition: int = 0):
    PARTITION_SIZE = 2500

    unique_repos = load_initial_dataset()
    total_rows = len(unique_repos)
    num_partitions = (total_rows + PARTITION_SIZE - 1) // PARTITION_SIZE

    output_dir = Path(SETTINGS.feature_repo_contributions_and_size_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Total rows: {total_rows}, partitions: {num_partitions}, starting from partition {start_partition}")

    for partition_idx in range(start_partition, num_partitions):
        partition = unique_repos.slice(partition_idx * PARTITION_SIZE, PARTITION_SIZE)
        logger.info(f"Processing partition {partition_idx} ({len(partition)} rows)...")

        enriched = (
            partition
            .with_columns(
                pl.col("github_repo")
                .map_elements(
                    lambda repo: get_contributions_and_size(repo),
                    return_dtype=pl.Struct(
                        [
                            pl.Field("contributions_count", pl.Int64),
                            pl.Field("size_in_kb", pl.Int64),
                        ]
                    )
                )
                .alias("contribution_size_stats")
            )
            .with_columns(
                pl.col("contribution_size_stats").struct.field("contributions_count"),
                pl.col("contribution_size_stats").struct.field("size_in_kb"),
            )
            .drop("contribution_size_stats")
        )

        out_path = output_dir / f"{partition_idx}.parquet"
        enriched.write_parquet(out_path)
        logger.info(f"Partition {partition_idx} written to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute repo contributions and size features.")
    parser.add_argument(
        "--start-partition",
        type=int,
        default=0,
        help="Partition index to start (or resume) from (default: 0).",
    )
    args = parser.parse_args()
    main(start_partition=args.start_partition)
