WITH optional_o1 AS (
SELECT 
      v1.from_id AS "v0__user_id", 
      v2.post_id AS "v2__post_id"
FROM db_polymorphic.interactions AS v1
JOIN db_polymorphic.posts AS v2 ON v1.to_id = v2.post_id
WHERE ((((v1."interaction_type" = 'LIKES') AND (v1."from_type" = 'User')) AND (v1."to_type" = 'Post')))
)
SELECT 
      v0.full_name AS "u.name", 
      count(o1.v2__post_id) AS "liked"
FROM db_polymorphic.users AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
GROUP BY v0.full_name

-- column u.name: Value [("u.name", "u.name")]
-- column liked: Value [("liked", "liked")]