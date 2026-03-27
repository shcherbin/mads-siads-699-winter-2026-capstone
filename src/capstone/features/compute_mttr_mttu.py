import glob
import logging
import json
import pandas as pd
import polars as pl
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from dependency_metrics.analyzer import DependencyAnalyzer

# --- Configuration ---
DATA_PATH = "/workspaces/mads-siads-699-winter-2026-capstone/notebooks/data/source_data/initial_dataset/*.parquet"
OUTPUT_DIR = Path("./output_results")
CHECKPOINT_FILE = OUTPUT_DIR / "processed_files.json"
FINAL_EXPORT = OUTPUT_DIR / "final_metrics.parquet"

OUTPUT_DIR.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, 'r') as f:
            return set(json.load(f))
    return set()

def save_checkpoint(processed_list):
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(list(processed_list), f)

def process_row(row_dict):
    """Internal logic from Zahan et al. method"""
    pkg = row_dict['package_name']
    start = row_dict['package_published_at']
    end = row_dict['next_snapshot']
    
    try:
        analyzer = DependencyAnalyzer(
            ecosystem="pypi",
            package=pkg,
            start_date=start,
            end_date=end,
            weighting_type="exponential",
            half_life=180,
            output_dir=Path("./temp_analyzer")
        )
        results = analyzer.analyze()
        return {
            'package_name': pkg,
            'snapshot_start': start,
            'snapshot_end': end,
            'avg_ttu': results.get('ttu'),
            'avg_ttr': results.get('ttr')
        }
    except Exception as e:
        logger.error(f"Error processing {pkg}: {e}")
        return None

def main():
    all_files = sorted(glob.glob(DATA_PATH))
    processed_files = load_checkpoint()
    
    files_to_process = [f for f in all_files if f not in processed_files]
    
    if not files_to_process:
        logger.info("No new files to process.")
        return

    for file_path in files_to_process:
        logger.info(f"Processing file: {file_path}")
        
        try:
            # 1. Load and prepare snapshots
            df_deps = (
                pl.scan_parquet(file_path)
                .select(["package_name", "package_version", "package_published_at"])
                .filter(pl.col("package_published_at").is_not_null())
                .collect()
            )

            snapshots_with_bounds = (
                df_deps.select(["package_name", "package_published_at"])
                .unique()
                .sort(["package_name", "package_published_at"])
                .with_columns([
                    pl.col("package_published_at").shift(-1).over("package_name").alias("next_snapshot")
                ])
                .filter(pl.col("next_snapshot").is_not_null())
            )

            rows = snapshots_with_bounds.to_dicts()
            with ProcessPoolExecutor() as executor:
                results_list = list(executor.map(process_row, rows))

            valid_results = [r for r in results_list if r is not None]
            if valid_results:
                batch_df = pd.DataFrame(valid_results)
                # Save each file's output separately to prevent loss
                batch_out = OUTPUT_DIR / f"res_{Path(file_path).stem}.parquet"
                batch_df.to_parquet(batch_out)

            processed_files.add(file_path)
            save_checkpoint(processed_files)
            
        except Exception as e:
            logger.critical(f"Critical failure on file {file_path}: {e}")
            continue

    logger.info("Processing complete. You can now merge the output directory Parquets.")

if __name__ == "__main__":
    main()