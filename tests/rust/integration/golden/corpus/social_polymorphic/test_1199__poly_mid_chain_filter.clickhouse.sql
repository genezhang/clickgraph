SELECT 
      count(*) AS "count(*)"
FROM brahmand.users_bench AS v0
JOIN brahmand.interactions AS v1 ON v1.from_id = v0.user_id
JOIN brahmand.users_bench AS v2 ON v1.to_id = v2.user_id
JOIN brahmand.interactions AS v3 ON v3.from_id = v2.user_id
JOIN brahmand.users_bench AS v4 ON v3.to_id = v4.user_id
WHERE (((((v1."interaction_type" = 'FOLLOWS') AND (v1."from_type" = 'User')) AND (v1."to_type" = 'User'))) AND ((((v3."interaction_type" = 'FOLLOWS') AND (v3."from_type" = 'User')) AND (v3."to_type" = 'User'))) AND (v1.from_id <> v3.from_id OR v1.to_id <> v3.to_id OR v1.interaction_type <> v3.interaction_type OR v1.timestamp <> v3.timestamp) AND v2.user_id = 3)

-- column count(*): Value [("count(*)", "count(*)")]