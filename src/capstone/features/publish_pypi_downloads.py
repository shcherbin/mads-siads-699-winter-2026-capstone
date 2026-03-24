"""Publish PyPI file download counts as a feature dataset."""

from pathlib import Path

import polars as pl
from loguru import logger

from settings import load_settings

SETTINGS = load_settings()


def load_pypi_downloads_dataset() -> pl.LazyFrame:
    """Load the PyPI file downloads source dataset."""
    source_path = SETTINGS.feature_total_downloads_path
    logger.info(f"Loading PyPI downloads data from {source_path}")
    return pl.scan_parquet(f"{source_path}/*.parquet")


def select_feature_columns(frame: pl.LazyFrame) -> pl.LazyFrame:
    """Select all PyPI downloads columns for feature publishing."""
    return frame.select(
        [
            pl.col("package_name"),
            pl.col("package_version"),
            pl.col("total_downloads"),
        ]
    )


def main() -> None:
    logger.info("Starting PyPI downloads feature publishing...")
    feature_frame = select_feature_columns(load_pypi_downloads_dataset())

    output_path = Path(SETTINGS.feature_pypi_downloads_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Writing PyPI downloads features to {output_path}")
    feature_frame.sink_parquet(str(output_path))
    logger.info(f"Saved PyPI downloads features to {output_path}")


if __name__ == "__main__":
    main()
