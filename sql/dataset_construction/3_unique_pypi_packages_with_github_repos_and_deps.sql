/*
This query computes the total number of unique Python packages available in the PyPI system via Deps.dev dataset 
with GitHub repository mappings and dependencies. 

Query output is used in dataset construction section of the article. 
*/
SELECT
   -- 348309 unique PyPI packages with GitHub repo mappings in PackageVersions and dependencies (all historical versions)
  COUNT(DISTINCT pv.Name) AS pypi_packages_with_github_and_dependencies
FROM `bigquery-public-data.deps_dev_v1.PackageVersions` pv

JOIN `bigquery-public-data.deps_dev_v1.PackageVersionToProject` pvtp
  ON pvtp.System = pv.System
  AND pvtp.Name = pv.Name
  AND pvtp.Version = pv.Version
  AND pvtp.ProjectType = 'GITHUB'

JOIN `bigquery-public-data.deps_dev_v1.Dependencies` d
  ON d.System = pv.System
  AND d.Name = pv.Name
  AND d.Version = pv.Version

WHERE   
  pv.System = 'PYPI'
  AND pvtp.ProjectName IS NOT NULL
;