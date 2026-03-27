"""Publish OSSF Scorecard data as a feature dataset."""

from pathlib import Path

import polars as pl
from loguru import logger

from settings import load_settings

SETTINGS = load_settings()
SCORECARD_DATE = pl.date(2024, 1, 1)


def load_pypi_scorecards_dataset() -> pl.DataFrame:
    """Load the OSSF Scorecard source dataset."""
    source_path = Path(SETTINGS.pypi_scorecards_path)
    logger.info(f"Loading OSSF Scorecard data from {source_path}")
    return pl.scan_parquet(str(source_path / "*.parquet")).collect()


def publish_ossf_scorecard(df_pypi_scorecards: pl.DataFrame, scorecard_date: pl.Expr) -> pl.DataFrame:
    """Filter scorecard rows for one date and pivot checks into feature columns."""
    normalized = df_pypi_scorecards.filter(pl.col("scorecard_date") == scorecard_date).with_columns(
        pl.col("repo_name").str.to_lowercase().str.replace_all("github.com/", "").alias("repo_name")
    )
    return normalized.pivot(
        index=["scorecard_date", "repo_name", "aggregate_score"],
        on="check_name",
        values="check_score",
    )


def main() -> None:
    logger.info("Starting OSSF Scorecard feature publishing...")
    df_pypi_scorecards = load_pypi_scorecards_dataset()
    feature_frame = publish_ossf_scorecard(df_pypi_scorecards, SCORECARD_DATE)

    output_path = Path(SETTINGS.feature_ossf_scorecard_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Writing OSSF Scorecard features to {output_path}")
    feature_frame.write_parquet(str(output_path))
    logger.info(f"Saved OSSF Scorecard features to {output_path}")


if __name__ == "__main__":
    main()

