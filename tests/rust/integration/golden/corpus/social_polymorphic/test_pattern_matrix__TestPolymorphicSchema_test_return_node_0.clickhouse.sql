SELECT 
      n.email_address AS "n.email", 
      n.full_name AS "n.name", 
      n.user_id AS "n.user_id"
FROM brahmand.users_bench AS n
LIMIT 5