WITH optional_o1 AS (
SELECT 
      v1.user_id AS "v0__user_id", 
      v2.post_id AS "v2__post_id"
FROM db_standard.posts AS v1
JOIN db_standard.posts AS v2 ON v1.post_id = v2.post_id
)
SELECT 
      v0.full_name AS "u.name", 
      count(o1.v2__post_id) AS "cnt"
FROM db_standard.users AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
GROUP BY v0.full_name

-- column u.name: Value [("u.name", "u.name")]
-- column cnt: Value [("cnt", "cnt")]