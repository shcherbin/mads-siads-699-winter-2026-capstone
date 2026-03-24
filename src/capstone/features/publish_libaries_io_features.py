"""Publish selected Libraries.io columns as a feature dataset."""

from pathlib import Path

import polars as pl
from loguru import logger

from settings import load_settings

SETTINGS = load_settings()


def load_libraries_io_dataset() -> pl.LazyFrame:
    """Load the Libraries.io source dataset."""
    logger.info(f"Loading Libraries.io data from {SETTINGS.librariesio_parquet_path}")
    return pl.scan_parquet(SETTINGS.librariesio_parquet_path)


def select_feature_columns(frame: pl.LazyFrame) -> pl.LazyFrame:
    """Select the Libraries.io columns needed for feature publishing."""
    return frame.select(
        [
            pl.col("full_name"),
            pl.col("contributions_count").alias("contributions_cnt"),
            pl.col("size"),
        ]
    )


def main() -> None:
    logger.info("Starting Libraries.io feature publishing...")
    feature_frame = select_feature_columns(load_libraries_io_dataset())

    output_path = Path(SETTINGS.feature_libraries_io_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Writing Libraries.io features to {output_path}")
    feature_frame.sink_parquet(str(output_path))
    logger.info(f"Saved Libraries.io features to {output_path}")


if __name__ == "__main__":
    main()
