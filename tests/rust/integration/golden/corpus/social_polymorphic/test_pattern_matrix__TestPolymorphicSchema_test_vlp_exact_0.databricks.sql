SELECT 
      a.full_name AS `a.name`, 
      b.full_name AS `b.name`
FROM brahmand.users_bench AS a
INNER JOIN brahmand.interactions AS r1 ON a.user_id = r1.from_id
INNER JOIN brahmand.interactions AS r2 ON r1.to_id = r2.from_id
INNER JOIN brahmand.users_bench AS b ON r2.to_id = b.user_id
WHERE NOT (r1.from_id = r2.from_id AND r1.to_id = r2.to_id)
LIMIT 10