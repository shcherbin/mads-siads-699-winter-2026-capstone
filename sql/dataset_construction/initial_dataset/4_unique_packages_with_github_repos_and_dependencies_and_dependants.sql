/*
This query computes the total number of unique Python packages available in the PyPI system via Deps.dev dataset
with GitHub repository mappings and dependencies.

Query output is used in dataset construction section of the article.
*/
WITH github_package_versions AS (
  SELECT DISTINCT
    Name,
    Version
  FROM `bigquery-public-data.deps_dev_v1.PackageVersionToProject`
  WHERE
    System = 'PYPI'
    AND ProjectType = 'GITHUB'
),

packages_with_dependencies AS (
  SELECT DISTINCT
    Name,
    Version
  FROM `bigquery-public-data.deps_dev_v1.Dependencies`
  WHERE
    System = 'PYPI'
    -- AND MinimumDepth = 1
),

packages_with_dependents AS (
  SELECT DISTINCT
    Name,
    Version
  FROM `bigquery-public-data.deps_dev_v1.Dependents`
  WHERE
    System = 'PYPI'
    --AND MinimumDepth = 1
)

SELECT
  -- 54,140 unique PyPI packages with GitHub repo mappings and dependencies (all historical versions)
  COUNT(DISTINCT gpv.Name) AS pypi_packages_with_github_dependencies_and_dependents
FROM github_package_versions gpv
JOIN packages_with_dependencies d
  ON d.Name = gpv.Name
  AND d.Version = gpv.Version
 JOIN packages_with_dependents dep
  ON dep.Name = gpv.Name
  AND dep.Version = gpv.Version
;
