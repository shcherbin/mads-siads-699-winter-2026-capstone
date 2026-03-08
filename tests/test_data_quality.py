"""Data quality tests to verify top packages flow through all datasets."""

from pathlib import Path

import polars as pl
import pytest

DATA_DIR = Path(__file__).resolve().parent.parent / "notebooks" / "data"

# Canonical list in github.com/owner/repo format
TOP_10_PACKAGES = [
    "github.com/psf/requests",
    "github.com/numpy/numpy",
    "github.com/pandas-dev/pandas",
    "github.com/pypa/setuptools",
    "github.com/urllib3/urllib3",
    "github.com/yaml/pyyaml",
    "github.com/dateutil/dateutil",
    "github.com/pallets/click",
    "github.com/pydantic/pydantic",
    "github.com/pytest-dev/pytest",
]

# initial_dataset uses owner/repo format (no "github.com/" prefix)
TOP_10_OWNER_REPO = [p.removeprefix("github.com/") for p in TOP_10_PACKAGES]


@pytest.fixture(scope="module")
def initial_dataset() -> pl.LazyFrame:
    """Load all shards of the initial dataset."""
    path = DATA_DIR / "source_data" / "initial_dataset"
    return pl.scan_parquet(path / "*.parquet")


@pytest.fixture(scope="module")
def unique_packages() -> pl.LazyFrame:
    """Load the unique packages augmented dataset."""
    path = DATA_DIR / "augmented_data" / "unique_packages.parquet"
    return pl.scan_parquet(path)


@pytest.fixture(scope="module")
def pypi_scorecards() -> pl.LazyFrame:
    """Load all shards of the pypi scorecards dataset."""
    path = DATA_DIR / "source_data" / "pypi_scorecards"
    return pl.scan_parquet(path / "*.parquet")


# ── initial_dataset tests (github_repo column, owner/repo format) ────────────


@pytest.mark.parametrize("repo", TOP_10_OWNER_REPO)
def test_initial_dataset_contains_package(initial_dataset: pl.LazyFrame, repo: str):
    """Each top-10 package should have at least one row in the initial dataset."""
    count = (
        initial_dataset.filter(pl.col("github_repo") == repo)
        .select(pl.len())
        .collect()
        .item()
    )
    assert count > 0, f"Package '{repo}' not found in initial_dataset (github_repo column)"


# ── unique_packages tests (github_repo column, github.com/owner/repo format) ─


@pytest.mark.parametrize("repo", TOP_10_PACKAGES)
def test_unique_packages_contains_package(unique_packages: pl.LazyFrame, repo: str):
    """Each top-10 package should have at least one row in unique_packages."""
    count = (
        unique_packages.filter(pl.col("github_repo") == repo)
        .select(pl.len())
        .collect()
        .item()
    )
    assert count > 0, f"Package '{repo}' not found in unique_packages (github_repo column)"


# ── pypi_scorecards tests (repo_name column, github.com/owner/repo format) ───


@pytest.mark.parametrize("repo", TOP_10_PACKAGES)
def test_pypi_scorecards_contains_package(pypi_scorecards: pl.LazyFrame, repo: str):
    """Each top-10 package should have at least one row in pypi_scorecards."""
    count = (
        pypi_scorecards.filter(pl.col("repo_name") == repo)
        .select(pl.len())
        .collect()
        .item()
    )
    assert count > 0, f"Package '{repo}' not found in pypi_scorecards (repo_name column)"
