/*
This query constructs a dataset of total downloads for each package version in the PyPI repository over the last 12 months.
The results are exported to a Parquet file in a Google Cloud Storage bucket for further analysis. The dataset includes the package name, version, and total download count.
*/
-- EXPORT DATA OPTIONS (
--   uri = 'gs://pypi_deps/pypi_file_downloads/*.parquet',
--   format = 'PARQUET',
--   overwrite = true
-- ) AS
SELECT
  file.project AS package_name,
  file.version AS package_version,
  COUNT(*) AS total_downloads
FROM `bigquery-public-data.pypi.file_downloads`
WHERE timestamp >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)) -- Filter for the last 12 months
  AND file.version IS NOT NULL
GROUP BY package_name, package_version
ORDER BY total_downloads DESC
