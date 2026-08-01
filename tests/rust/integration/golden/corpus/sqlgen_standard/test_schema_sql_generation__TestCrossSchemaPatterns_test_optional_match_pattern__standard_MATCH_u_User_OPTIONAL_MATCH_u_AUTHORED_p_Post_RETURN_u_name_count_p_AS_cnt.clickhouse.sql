SELECT 
      u.full_name AS "u.name", 
      count(p.post_id) AS "cnt"
FROM db_standard.users AS u
LEFT JOIN db_standard.posts AS p ON p.user_id = u.user_id
GROUP BY u.full_name
