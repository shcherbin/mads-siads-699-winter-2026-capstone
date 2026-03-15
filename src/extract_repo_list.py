import polars as pl
import os
from pathlib import Path

_BASE = Path(__file__).resolve().parent.parent / "notebooks" / "data"
INPUT_DIR = str(_BASE / "source_data" / "initial_dataset")
OUTPUT_PATH = str(_BASE / "augmented_data" / "unique_packages.parquet")


def extract_unique_packages(input_dir: str, output_path: str) -> pl.DataFrame:
    """Iterate through all parquet files to get unique package_name and github repo with min/max snapshot dates."""
    frames = []
    parquet_files = [f for f in os.listdir(input_dir) if f.endswith(".parquet")]
    for parquet_file in parquet_files:
        df = pl.read_parquet(f"{input_dir}/{parquet_file}")
        agg = df.group_by(["package_name", "github_repo"]).agg(
            pl.col("SnapshotAt").min().alias("min_snapshot_date"),
            pl.col("SnapshotAt").max().alias("max_snapshot_date"),
        )
        frames.append(agg)
    unique_packages = pl.concat(frames).group_by(["package_name", "github_repo"]).agg(
        pl.col("min_snapshot_date").min(),
        pl.col("max_snapshot_date").max(),
    )
    unique_packages = unique_packages.with_columns(
        ("github.com/" + pl.col("github_repo")).alias("github_repo")
    )
    unique_packages.write_parquet(output_path)
    return unique_packages

unique_packages = extract_unique_packages(
    input_dir=INPUT_DIR,
    output_path=OUTPUT_PATH,
)


