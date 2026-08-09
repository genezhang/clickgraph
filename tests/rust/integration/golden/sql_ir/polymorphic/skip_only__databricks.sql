SELECT 
      u.full_name AS `u.name`
FROM brahmand.users_bench AS u
ORDER BY u.full_name ASC NULLS LAST
OFFSET 2