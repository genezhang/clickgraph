SELECT 
      u.full_name AS "u.name", 
      count(p.post_id) AS "cnt"
FROM db_standard.posts AS p
LEFT JOIN db_standard.users AS u ON u.user_id = p.user_id
GROUP BY u.full_name
