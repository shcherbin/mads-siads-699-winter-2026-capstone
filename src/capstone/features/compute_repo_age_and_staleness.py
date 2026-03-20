"""This script computes the dependency count feature for each package across all snapshots."""

import os
import polars as pl
from github import Github, Auth, UnknownObjectException, GithubException
from datetime import datetime, timezone
from loguru import logger

from settings import load_settings

SETTINGS = load_settings()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GH = Github(auth=Auth.Token(token=GITHUB_TOKEN))


def load_initial_dataset() -> pl.LazyFrame:
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


def get_repo_age_and_staleness(full_name: str):
    try:
        repo = GH.get_repo(full_name)
        now = datetime.now(timezone.utc)

        created_at = repo.created_at
        pushed_at = repo.pushed_at

        repo_age_years = round((now - created_at).days / 365.2425, 2)
        commit_staleness_days = round((now - pushed_at).days, 2)

        return {"repo_age_years": repo_age_years, "commit_staleness_days": commit_staleness_days}
    except UnknownObjectException as e:
        logger.warning(f"Repository not found for {full_name}: {e}")
        return {"repo_age_years": None, "commit_staleness_days": None}
    except GithubException as e:
        logger.error(f"GitHub error for {full_name}: {e}")
        return {"repo_age_years": None, "commit_staleness_days": None}


def main():
    unique_repos = load_initial_dataset()
    enriched_unique_repos = (
        unique_repos
            #.head()
            .with_columns(pl.col("github_repo")
            .map_elements(
                lambda repo: get_repo_age_and_staleness(repo),
                return_dtype=pl.Struct(
                    [
                        pl.Field("repo_age_years", pl.Float64),
                        pl.Field("commit_staleness_days", pl.Int64),
                    ]
                )
            )
            .alias("repo_stats")
        )
        .with_columns(
            pl.col("repo_stats").struct.field("repo_age_years"),
            pl.col("repo_stats").struct.field("commit_staleness_days"),
        )
        .drop("repo_stats")
    )

    enriched_unique_repos.write_parquet(SETTINGS.feature_repo_age_and_staleness_path)


if __name__ == "__main__":
    main()
