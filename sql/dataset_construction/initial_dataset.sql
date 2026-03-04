/*
This query constructs an initial dataset of Python packages and their dependencies, along with metadata about the packages and their associated GitHub repositories (if available).
*/
-- PyPI package version -> resolved dependency (with timestamps)
-- + GitHub repository mapping if deps.dev has one

SELECT
  d.SnapshotAt,
  d.Name                 AS package_name,
  d.Version              AS package_version,
  pv.UpstreamPublishedAt AS package_published_at,

  d.Dependency.Name      AS dep_name,
  d.Dependency.Version   AS dep_version,
  d.MinimumDepth,

  -- GitHub mapping (may be NULL)
  pvtp.ProjectName       AS github_repo,     -- "owner/repo" when present
  --pr.StarsCount          AS github_stars,
  --pr.ForksCount          AS github_forks,
  --pr.OpenIssuesCount     AS github_open_issues

FROM `bigquery-public-data.deps_dev_v1.DependenciesLatest` d
JOIN `bigquery-public-data.deps_dev_v1.PackageVersionsLatest` pv
  USING (System, Name, Version)

LEFT JOIN `bigquery-public-data.deps_dev_v1.PackageVersionToProjectLatest` pvtp
    ON pvtp.System = d.System
    AND pvtp.Name = d.Name
    AND pvtp.Version = d.Version
    AND pvtp.SnapshotAt = d.SnapshotAt     -- keep snapshots aligned
    AND pvtp.ProjectType = 'GITHUB'

LEFT JOIN `bigquery-public-data.deps_dev_v1.ProjectsLatest` pr
    ON pr.Type = pvtp.ProjectType
    AND pr.Name = pvtp.ProjectName
    AND pr.SnapshotAt = d.SnapshotAt       -- keep snapshots aligned

WHERE d.System = 'PYPI'
  AND d.Dependency.System = 'PYPI'
  -- AND d.MinimumDepth = 1 -- Uncomment to focus on direct dependencies only
LIMIT 1000
;
