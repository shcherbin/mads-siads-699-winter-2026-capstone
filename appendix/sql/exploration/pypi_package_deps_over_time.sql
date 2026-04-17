-- One row per resolved dependency of a PyPI package version
SELECT
  d.SnapshotAt,                                -- when deps.dev exported this row
  d.Name            AS package_name,
  d.Version         AS package_version,
  pv.UpstreamPublishedAt AS package_published_at, -- when the version hit PyPI
  d.Dependency.Name    AS dep_name,
  d.Dependency.Version AS dep_version,
  d.MinimumDepth
FROM `bigquery-public-data.deps_dev_v1.Dependencies` d
JOIN `bigquery-public-data.deps_dev_v1.PackageVersions` pv
  USING (System, Name, Version)
WHERE d.System = 'PYPI'
  AND d.Dependency.System = 'PYPI'
  LIMIT 1000;