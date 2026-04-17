/*
This query retrieves the timestamp of the latest and earliest snapshots of PyPI package versions available in the
Deps.dev dataset. This information can be useful for understanding the time range of the data being analyzed.
*/

SELECT
  MAX(SnapshotAt) AS latest_pypi_snapshot_timestamp,
  MIN(SnapshotAt) AS earliest_pypi_snapshot_timestamp,
FROM
  `bigquery-public-data.deps_dev_v1.PackageVersions`
WHERE
  System = 'PYPI';