/*
This query retrieves package names, their latest versions, and their source URLs for packages in the PYPI system that have GitHub as their source repository.
*/
SELECT
  p.Name AS package_name,
  p.Version AS latest_version,
  proj.URL AS source_url
FROM
  `bigquery-public-data.deps_dev_v1.PackageVersionsLatest` AS p
  CROSS JOIN UNNEST(p.Links) AS proj
WHERE
  p.System = 'PYPI'
  AND LOWER(proj.Label) = 'source_repo'
  AND LOWER(proj.URL) LIKE '%github.com%'
GROUP BY
  package_name, latest_version, source_url
ORDER BY
  package_name
LIMIT 100
;