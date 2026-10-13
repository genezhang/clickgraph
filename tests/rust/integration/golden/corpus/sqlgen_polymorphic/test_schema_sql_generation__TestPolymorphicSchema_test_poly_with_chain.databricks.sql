WITH with_w1 AS (
SELECT 
      v0.user_id AS `v3__user_id`, 
      v0.full_name AS `p2_v3_name`, 
      count(v2.post_id) AS `v4`
FROM db_polymorphic.users AS v0
JOIN db_polymorphic.interactions AS v1 ON v1.from_id = v0.user_id
JOIN db_polymorphic.posts AS v2 ON v1.to_id = v2.post_id
WHERE ((((v1.`interaction_type` = 'AUTHORED') AND (v1.`from_type` = 'User')) AND (v1.`to_type` = 'Post')))
GROUP BY v0.user_id, v0.full_name
)
SELECT 
      w1.p2_v3_name AS `u.name`, 
      w1.v4 AS `posts`
FROM with_w1 AS w1
ORDER BY w1.v4 DESC NULLS FIRST
