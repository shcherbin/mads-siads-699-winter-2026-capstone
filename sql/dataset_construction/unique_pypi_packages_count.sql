SELECT
   -- 771,272 for PackageVersions (all historical versions)
   -- 715,907 for PackageVersionsLatest (only latest versions)
  COUNT(DISTINCT Name) AS unique_pypi_packages
FROM
  -- `bigquery-public-data.deps_dev_v1.PackageVersionsLatest`
  `bigquery-public-data.deps_dev_v1.PackageVersions`
WHERE
  System = 'PYPI';