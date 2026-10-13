SELECT 
      v0.full_name AS "a.name", 
      count(v2.user_id) AS "cnt"
FROM db_standard.users AS v0
JOIN db_standard.user_follows AS v1 ON v1.follower_id = v0.user_id
JOIN db_standard.users AS v2 ON v1.followed_id = v2.user_id
GROUP BY v0.full_name
ORDER BY count(v2.user_id) DESC NULLS FIRST
LIMIT 5
-- column a.name: Value [("a.name", "a.name")]
-- column cnt: Value [("cnt", "cnt")]