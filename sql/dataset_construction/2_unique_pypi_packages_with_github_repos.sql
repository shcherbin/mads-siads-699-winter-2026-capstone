/*
This query computes the total number of unique Python packages available in the PyPI system via Deps.dev dataset with GitHub repository mappings. 
Query output is used in dataset construction section of the article. 
*/
SELECT
  -- 507029 unique PyPI packages with GitHub repo mappings in PackageVersions (all historical versions)
  COUNT(DISTINCT pv.Name)
FROM `bigquery-public-data.deps_dev_v1.PackageVersions` pv
LEFT JOIN `bigquery-public-data.deps_dev_v1.PackageVersionToProject` pvtp
  ON pvtp.System = pv.System
  AND pvtp.Name = pv.Name
  AND pvtp.Version = pv.Version
  AND pvtp.ProjectType = 'GITHUB'

LEFT JOIN `bigquery-public-data.deps_dev_v1.Projects` pr
  ON pr.Type = pvtp.ProjectType
  AND pr.Name = pvtp.ProjectName

WHERE   
  pv.System = 'PYPI'
  AND pvtp.ProjectName IS NOT NULL
;