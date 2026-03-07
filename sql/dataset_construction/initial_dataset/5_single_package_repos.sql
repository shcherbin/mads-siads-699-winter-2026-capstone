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

packages_with_dependencies AS (
  SELECT DISTINCT
    System,
    Name,
    Version
  FROM `bigquery-public-data.deps_dev_v1.Dependencies`
  WHERE
    System = 'PYPI'
    -- AND MinimumDepth = 1
),

packages_with_dependents AS (
  SELECT DISTINCT
    System,
    Name,
    Version
  FROM `bigquery-public-data.deps_dev_v1.Dependents`
  WHERE
    System = 'PYPI'
    -- AND MinimumDepth = 1
)

SELECT
  -- 43269 unique PyPI packages with GitHub repo mappings to a single repository and dependencies and dependents (all historical versions)
  COUNT(DISTINCT gpv.Name) AS pypi_packages_final_dataset
FROM github_package_versions gpv
JOIN packages_with_dependencies d
  ON d.System = 'PYPI'
  AND d.Name = gpv.Name
  AND d.Version = gpv.Version
JOIN packages_with_dependents dep
  ON dep.System = 'PYPI'
  AND dep.Name = gpv.Name
  AND dep.Version = gpv.Version
;