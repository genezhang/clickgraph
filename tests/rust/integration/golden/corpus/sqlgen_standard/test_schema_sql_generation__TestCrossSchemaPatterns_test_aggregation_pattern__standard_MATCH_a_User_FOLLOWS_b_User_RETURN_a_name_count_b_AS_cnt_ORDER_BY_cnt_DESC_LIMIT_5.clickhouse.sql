SELECT 
      a.full_name AS "a.name", 
      count(t0.followed_id) AS "cnt"
FROM db_standard.users AS a
INNER JOIN db_standard.user_follows AS t0 ON t0.follower_id = a.user_id
GROUP BY a.full_name
ORDER BY cnt DESC
LIMIT 5