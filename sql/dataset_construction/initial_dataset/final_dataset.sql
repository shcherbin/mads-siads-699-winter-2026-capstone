/*
This query constructs an initial dataset of Python packages and their dependencies, along with metadata about the packages and their associated GitHub repositories (if available).
*/
-- PyPI package version -> resolved dependency (with timestamps)
-- + GitHub repository mapping if deps.dev has one
-- EXPORT DATA OPTIONS (
--   uri = 'gs://pypi_deps/initial_dataset/*.parquet',
--   format = 'PARQUET',
--   overwrite = true
-- ) AS
WITH single_package_repos AS (
  SELECT
    ProjectName
  FROM `bigquery-public-data.deps_dev_v1.PackageVersionToProject`
  WHERE
    System = 'PYPI'
    AND ProjectType = 'GITHUB'
  GROUP BY ProjectName
  HAVING COUNT(DISTINCT Name) = 1
),

github_package_versions AS (
  SELECT DISTINCT
    pvtp.Name,
    pvtp.Version,
    pvtp.ProjectName
  FROM `bigquery-public-data.deps_dev_v1.PackageVersionToProject` pvtp
  JOIN single_package_repos spr
    ON spr.ProjectName = pvtp.ProjectName
  WHERE
    pvtp.System = 'PYPI'
    AND pvtp.ProjectType = 'GITHUB'
),

packages_with_dependents AS (
  SELECT DISTINCT
    SnapshotAt,
    System,
    Name,
    Version
  FROM `bigquery-public-data.deps_dev_v1.Dependents`
  WHERE
    System = 'PYPI'
    -- AND MinimumDepth = 1
)

SELECT
  d.SnapshotAt,
  d.Name AS package_name,
  d.Version AS package_version,
  pv.UpstreamPublishedAt AS package_published_at,
  d.Dependency.Name AS dep_name,
  d.Dependency.Version AS dep_version,
  d.MinimumDepth,
  gpv.ProjectName AS github_repo
FROM `bigquery-public-data.deps_dev_v1.Dependencies` AS d

JOIN github_package_versions AS gpv
  ON gpv.Name = d.Name
  AND gpv.Version = d.Version

JOIN packages_with_dependents AS dep
  ON dep.SnapshotAt = d.SnapshotAt
  AND dep.System = d.System
  AND dep.Name = d.Name
  AND dep.Version = d.Version

JOIN `bigquery-public-data.deps_dev_v1.PackageVersions` AS pv
  ON pv.SnapshotAt = d.SnapshotAt
  AND pv.System = d.System
  AND pv.Name = d.Name
  AND pv.Version = d.Version
WHERE
  d.System = 'PYPI'
  -- AND d.MinimumDepth = 1
ORDER BY
  d.SnapshotAt,
  package_name,
  package_version,
  dep_name
-- LIMIT 1000
;
