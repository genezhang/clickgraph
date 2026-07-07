SELECT 
      u.user_id AS "u.user_id"
FROM brahmand.users_bench AS u
WHERE (u.user_id > 2 AND u.full_name = 'Bob Jones')
