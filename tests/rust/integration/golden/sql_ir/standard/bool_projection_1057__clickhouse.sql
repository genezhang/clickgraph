SELECT 
      u.user_id AS "u.user_id", 
      CAST(u.user_id > 2 AS Nullable(Bool)) AS "gt", 
      CAST(startsWith(u.full_name, 'A') AS Nullable(Bool)) AS "sw", 
      CAST(u.email_address IS NULL AS Nullable(Bool)) AS "isn"
FROM social.users_bench AS u
WHERE u.user_id > 0
