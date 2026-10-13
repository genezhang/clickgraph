WITH optional_o1 AS (
SELECT 
      v1.user_id AS "v0__user_id", 
      v2.post_id AS "v2__post_id"
FROM db_standard.posts AS v1
JOIN db_standard.posts AS v2 ON v1.post_id = v2.post_id
), 
optional_o2 AS (
SELECT 
      v3.post_id AS "v2__post_id", 
      v4.user_id AS "v4__user_id"
FROM db_standard.post_likes AS v3
JOIN db_standard.users AS v4 ON v3.user_id = v4.user_id
WHERE v3.post_id IS NOT NULL
)
SELECT 
      v0.full_name AS "u.name", 
      count(DISTINCT o1.v2__post_id) AS "posts", 
      count(DISTINCT o2.v4__user_id) AS "likers"
FROM db_standard.users AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
LEFT JOIN optional_o2 AS o2 ON o1.v2__post_id = o2.v2__post_id
GROUP BY v0.full_name
