SELECT 
      v0.email_address AS "n.email", 
      v0.full_name AS "n.name", 
      v0.user_id AS "n.user_id"
FROM brahmand.users_bench AS v0
LIMIT 5
-- column n: Node { label: "User" } [("email", "n.email"), ("name", "n.name"), ("user_id", "n.user_id")]