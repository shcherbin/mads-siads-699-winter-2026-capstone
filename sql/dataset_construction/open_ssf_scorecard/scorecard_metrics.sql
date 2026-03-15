SELECT  
  card.date scorecard_date, lower(card.repo.name) as repo_name, card.repo.commit as repo_commit, 
  card.scorecard.version as scorecard_version, card.score as aggregate_score, 
  checks.name as check_name, checks.score as check_score
FROM `openssf.scorecardcron.scorecard-v2` as card
JOIN `harjason-siads-699.siads_699.pypi_package_repo_list` lst  --Uploaded table from notebooks/data/augmented_data/unique_packages.parquet
  ON lower(card.repo.name) = lower(lst.github_repo)
CROSS JOIN 
  UNNEST(card.checks) as checks
WHERE card.date >= "2023-04-09" --minimum date in deps.dev export is 2023-04-10 
--LIMIT 100 --est 66GB query