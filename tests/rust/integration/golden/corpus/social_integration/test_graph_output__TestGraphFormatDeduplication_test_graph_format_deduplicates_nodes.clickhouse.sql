SELECT 
      v0.age AS "u.age", 
      v0.city AS "u.city", 
      v0.country AS "u.country", 
      v0.email_address AS "u.email", 
      v0.is_active AS "u.is_active", 
      v0.full_name AS "u.name", 
      v0.registration_date AS "u.registration_date", 
      v0.user_id AS "u.user_id", 
      v1.follower_id AS "r.from_id", 
      v1.followed_id AS "r.to_id", 
      v1.follow_date AS "r.follow_date", 
      v2.age AS "f.age", 
      v2.city AS "f.city", 
      v2.country AS "f.country", 
      v2.email_address AS "f.email", 
      v2.is_active AS "f.is_active", 
      v2.full_name AS "f.name", 
      v2.registration_date AS "f.registration_date", 
      v2.user_id AS "f.user_id"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
LIMIT 10
-- column u: Node { label: "User" } [("age", "u.age"), ("city", "u.city"), ("country", "u.country"), ("email", "u.email"), ("is_active", "u.is_active"), ("name", "u.name"), ("registration_date", "u.registration_date"), ("user_id", "u.user_id")]
-- column r: Rel { rel_type: "FOLLOWS", from_label: "User", to_label: "User" } [("from_id", "r.from_id"), ("to_id", "r.to_id"), ("follow_date", "r.follow_date")]
-- column f: Node { label: "User" } [("age", "f.age"), ("city", "f.city"), ("country", "f.country"), ("email", "f.email"), ("is_active", "f.is_active"), ("name", "f.name"), ("registration_date", "f.registration_date"), ("user_id", "f.user_id")]