SELECT 
      u.full_name AS `u.name`
FROM brahmand.users_bench AS u
ORDER BY u.full_name DESC
LIMIT 3 OFFSET 1