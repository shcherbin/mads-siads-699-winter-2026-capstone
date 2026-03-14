/*
This query retrieves the names of GitHub repositories that have Python as one of their programming languages.
The query checks for the presence of 'Python' in the list of languages for each repository and returns the corresponding repository names.
*/

SELECT
  repo_name
FROM
  `bigquery-public-data.github_repos.languages` AS l
WHERE EXISTS (
    SELECT 1 FROM UNNEST(l.language) AS lang WHERE lang.name = 'Python'
)
LIMIT 100
;
