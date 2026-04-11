"""
Publishes supplemental scorecard data that was gathered via the OSSF Scorecard CLI
for repositories that were not available in the bigquery dataset.

"""

import json
import re
from datetime import date as Date
from pathlib import Path

import polars as pl
from loguru import logger

from settings import load_settings

SETTINGS = load_settings()

EXPECTED_CHECKS = (
    "Binary-Artifacts",
    "Branch-Protection",
    "CI-Tests",
    "CII-Best-Practices",
    "Code-Review",
    "Contributors",
    "Dangerous-Workflow",
    "Dependency-Update-Tool",
    "Fuzzing",
    "License",
    "Maintained",
    "Packaging",
    "Pinned-Dependencies",
    "SAST",
    "Security-Policy",
    "Signed-Releases",
    "Token-Permissions",
    "Vulnerabilities",
)


def load_cli_scorecards() -> pl.DataFrame:
    """Read CLI JSONL, deduplicate per repo, and return a long-format DataFrame."""
    source_path = Path(SETTINGS.scorecards_cli_results_path)
    logger.info(f"Loading CLI scorecard results from {source_path}")

    # Deduplicate in Python: keep the entry with the latest timestamp per repo.
    # Some lines contain multiple concatenated JSON objects, so we use raw_decode
    # to consume them one at a time.
    decoder = json.JSONDecoder()
    latest: dict[str, dict] = {}
    with open(source_path) as f:
        for line in f:
            text = line.strip()
            pos = 0
            while pos < len(text):
                try:
                    entry, offset = decoder.raw_decode(text, pos)
                except json.JSONDecodeError:
                    break
                pos += offset
                # Skip whitespace between objects
                while pos < len(text) and text[pos] in " \t":
                    pos += 1
                if entry["status"] != "success":
                    continue
                repo = entry["repo"]
                if repo not in latest or entry["timestamp"] > latest[repo]["timestamp"]:
                    latest[repo] = entry

    logger.info(f"Found {len(latest)} unique repos with successful scorecard results")

    rows = []
    for entry in latest.values():
        sc = entry["scorecard"]
        if not sc or not sc.get("checks"):
            continue
        scorecard_date: Date = Date.fromisoformat(sc["date"][:10])
        repo_name: str = sc["repo"]["name"]
        aggregate_score: float = sc["score"]
        vuln_detected: int | None = None
        for check in sc["checks"]:
            if check["name"] == "Vulnerabilities":
                m = re.match(r"(\d+)", check.get("reason", ""))
                vuln_detected = int(m.group(1)) if m else 0
        for check in sc["checks"]:
            rows.append(
                {
                    "scorecard_date": scorecard_date,
                    "repo_name": repo_name,
                    "aggregate_score": aggregate_score,
                    "check_name": check["name"],
                    "check_score": check["score"],
                    "vulnerabilities_detected": vuln_detected,
                }
            )

    return pl.DataFrame(
        rows,
        schema={
            "scorecard_date": pl.Date,
            "repo_name": pl.String,
            "aggregate_score": pl.Float64,
            "check_name": pl.String,
            "check_score": pl.Int64,
            "vulnerabilities_detected": pl.Int64,
        },
    )


def publish_cli_scorecard(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize repo_name and pivot checks into feature columns.

    Ensures all EXPECTED_CHECKS are present as columns (null-filled if absent
    from the source data), and returns columns in a fixed canonical order.
    """
    normalized = df.with_columns(
        pl.col("repo_name").str.to_lowercase().str.replace_all("github.com/", "").alias("repo_name")
    )
    pivoted = normalized.pivot(
        index=["scorecard_date", "repo_name", "aggregate_score", "vulnerabilities_detected"],
        on="check_name",
        values="check_score",
    )
    for check in EXPECTED_CHECKS:
        if check not in pivoted.columns:
            logger.warning(f"Check '{check}' not found in CLI data — adding null column")
            pivoted = pivoted.with_columns(pl.lit(None).cast(pl.Int64).alias(check))
    return pivoted.select(["scorecard_date", "repo_name", "aggregate_score", "vulnerabilities_detected"] + list(EXPECTED_CHECKS))


def main() -> None:
    logger.info("Starting OSSF Scorecard CLI feature publishing...")

    df_long = load_cli_scorecards()
    cli_frame = publish_cli_scorecard(df_long)

    output_path = Path(SETTINGS.feature_ossf_scorecard_path)

    if output_path.exists():
        logger.info(f"Appending to existing feature file at {output_path}")
        existing = pl.read_parquet(str(output_path))

        # Backfill vulnerabilities_detected from CLI data onto existing rows
        vuln_lookup = cli_frame.select("repo_name", "vulnerabilities_detected").unique(subset=["repo_name"])
        existing = existing.drop("vulnerabilities_detected", strict=False).join(
            vuln_lookup, on="repo_name", how="left"
        )

        combined = pl.concat([existing, cli_frame], how="diagonal")
    else:
        combined = cli_frame

    # Keep most recent scorecard_date per repo
    result = combined.sort("scorecard_date", descending=True).unique(subset=["repo_name"], keep="first")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Writing {len(result)} rows to {output_path}")
    result.write_parquet(str(output_path))
    logger.info(f"Saved OSSF Scorecard CLI features to {output_path}")


if __name__ == "__main__":
    main()
