SELECT 
      CASE WHEN u.is_active = true THEN 'active' ELSE 'inactive' END AS "status"
FROM social.users_bench AS u
