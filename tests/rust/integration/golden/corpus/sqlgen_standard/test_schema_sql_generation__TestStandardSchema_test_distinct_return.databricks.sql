SELECT DISTINCT 
      u.full_name AS `u.name`, 
      p.content AS `p.content`
FROM db_standard.users AS u
INNER JOIN db_standard.user_follows AS t0 ON t0.follower_id = u.user_id
INNER JOIN db_standard.posts AS p ON p.user_id = t0.followed_id
