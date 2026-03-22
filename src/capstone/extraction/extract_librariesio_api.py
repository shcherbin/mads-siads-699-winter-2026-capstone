"""Extract repository metadata from the Libraries.io API.
Reads unique packages from notebooks/data/source_data/pypi_scorecards/*.parquet,
calls the Libraries.io GitHub repository endpoint for each one, and saves results to a parquet file.
"""

import argparse
import os
import time
from pathlib import Path

import dotenv
import polars as pl
import requests
from loguru import logger

from settings import load_settings

SETTINGS = load_settings()

API_BASE_URL = "https://libraries.io/api/github"
RATE_LIMIT_SECONDS = 1.0

# Columns to keep from the API response
KEEP_COLUMNS = [
    "full_name",
    "description",
    "fork",
    "created_at",
    "updated_at",
    "pushed_at",
    "size",
    "stargazers_count",
    "language",
    "has_issues",
    "has_wiki",
    "has_pages",
    "forks_count",
    "open_issues_count",
    "default_branch",
    "subscribers_count",
    "license",
    "contributions_count",
    "has_readme",
    "has_changelog",
    "has_contributing",
    "has_license",
    "has_coc",
    "has_threat_model",
    "has_audit",
    "status",
    "rank",
    "github_contributions_count",
    "github_id",
]


def load_api_key() -> str:
    """Load the Libraries.io API key from the .env file."""
    dotenv.load_dotenv()
    api_key = os.getenv("LIBRARIESIO_API_KEY")
    if not api_key:
        raise RuntimeError("LIBRARIESIO_API_KEY not set in environment or .env file")
    return api_key


def load_repos(input_dir: str) -> list[str]:
    """Load unique repo names from all scorecard parquet files in the directory."""
    parquet_files = sorted(Path(input_dir).glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {input_dir}")
    frames = [pl.read_parquet(f, columns=["repo_name"]) for f in parquet_files]
    all_repos = pl.concat(frames)["repo_name"].unique().to_list()
    logger.info(f"Loaded {len(all_repos)} unique repos from {len(parquet_files)} files in {input_dir}")
    return all_repos


def parse_owner_repo(github_repo: str) -> tuple[str, str]:
    """Extract owner and repo name from a 'github.com/owner/repo' string."""

    parts = github_repo.removeprefix("github.com/").split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Invalid github_repo format: {github_repo}")
    return parts[0], parts[1]


def fetch_repo_metadata(owner: str, repo: str, api_key: str, max_retries: int = 10) -> dict | None:
    """Call the Libraries.io API for a single GitHub repository.

       Returns the JSON response dict, or None on failure.
       Retries up to *max_retries* times on timeout errors with increasing delay.
    """
    url = f"{API_BASE_URL}/{owner}/{repo}"
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params={"api_key": api_key}, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"HTTP {resp.status_code} for {owner}/{repo}: {resp.text[:200]}")
            return None
        except requests.exceptions.Timeout:
            delay = 5 * attempt
            if attempt < max_retries:
                logger.warning(f"Timeout for {owner}/{repo} (attempt {attempt}/{max_retries}), retrying in {delay}s")
                time.sleep(delay)
            else:
                logger.warning(f"Timeout for {owner}/{repo} (attempt {attempt}/{max_retries})")
    logger.error(f"Failed to fetch {owner}/{repo} after {max_retries} timeout retries")
    return None


def load_already_fetched(output_path: str) -> set[str]:
    """Load previously fetched repo full_names to support incremental runs.
       Returns a set of lowercased full_names for case-insensitive matching.
    """
    if not os.path.exists(output_path):
        return set()
    df = pl.read_parquet(output_path)
    return set(name.lower() for name in df["full_name"].to_list())


def extract_all(
    input_dir: str = SETTINGS.pypi_scorecards_path,
    output_path: str = SETTINGS.librariesio_parquet_path,
    limit: int | None = None,
) -> pl.DataFrame:
    """Fetch Libraries.io metadata for all repos and save to parquet.

    Args:
        limit: If set, only fetch this many repos (useful for testing).
    """
    api_key = load_api_key()
    repos = load_repos(input_dir)

    # Resume support: skip repos already fetched
    already_fetched = load_already_fetched(output_path)
    if already_fetched:
        logger.info(f"Resuming — {len(already_fetched)} repos already fetched, skipping them")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    results: list[dict] = []
    total = len(repos)
    fetched_count = 0
    progress = len(already_fetched)

    for github_repo in repos:
        if limit is not None and fetched_count >= limit:
            logger.info(f"Reached limit of {limit} repos, stopping.")
            break

        try:
            owner, name = parse_owner_repo(github_repo)
        except ValueError:
            logger.warning(f"Skipping malformed repo: {github_repo}")
            continue

        full_name = f"{owner}/{name}"
        if full_name.lower() in already_fetched:
            continue

        progress += 1
        logger.info(f"[{progress}/{total}] Fetching {owner}/{name}")
        data = fetch_repo_metadata(owner, name, api_key)
        if data is not None:
            row = {col: data.get(col) for col in KEEP_COLUMNS}
            results.append(row)
            fetched_count += 1

        # Save progress every 25 repos
        if len(results) > 0 and len(results) % 25 == 0:
            logger.info(f"Checkpoint: saving {len(results)} new repos (total fetched this run: {fetched_count})")
            _save_batch(results, output_path, already_fetched)
            already_fetched.update(row["full_name"].lower() for row in results)
            results.clear()

        time.sleep(RATE_LIMIT_SECONDS)

    # Final save
    if results:
        _save_batch(results, output_path, already_fetched)

    if os.path.exists(output_path):
        result_df = pl.read_parquet(output_path)
    else:
        result_df = pl.DataFrame()
    logger.info(f"Done. Total repos in output: {len(result_df)}")
    return result_df


def _save_batch(
    new_results: list[dict],
    output_path: str,
    already_fetched: set[str],
) -> pl.DataFrame:
    """Append new results to the existing parquet file (if any)."""
    new_df = pl.DataFrame(new_results, infer_schema_length=None)

    if already_fetched and os.path.exists(output_path):
        existing_df = pl.read_parquet(output_path)
        combined = pl.concat([existing_df, new_df], how="vertical_relaxed")
    else:
        combined = new_df

    combined.write_parquet(output_path)
    logger.info(f"Saved {len(combined)} rows to {output_path}")
    return combined


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract repo metadata from Libraries.io")
    parser.add_argument("--limit", type=int, default=None, help="Max repos to fetch (e.g. --limit 5 for testing)")
    args = parser.parse_args()
    extract_all(limit=args.limit)
