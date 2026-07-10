SELECT 
      f.full_name AS `f.name`
FROM db_standard.users AS u
INNER JOIN db_standard.user_follows AS t0 ON t0.follower_id = u.user_id
INNER JOIN db_standard.users AS f ON f.user_id = t0.followed_id
WHERE (u.full_name = 'Alice' AND f.is_active = true)
