SELECT 
      a.full_name AS "a.name", 
      b.full_name AS "b.name"
FROM db_standard.users AS a
INNER JOIN db_standard.friendships AS t0 ON t0.user_id_1 = a.user_id
INNER JOIN db_standard.users AS b ON b.user_id = t0.user_id_2
UNION ALL 
SELECT 
      a.full_name AS "a.name", 
      b.full_name AS "b.name"
FROM db_standard.users AS b
INNER JOIN db_standard.friendships AS t0 ON t0.user_id_1 = b.user_id
INNER JOIN db_standard.users AS a ON a.user_id = t0.user_id_2
