SELECT 
      u.email_address AS "u.email"
FROM brahmand.users_bench AS u
WHERE u.full_name IN ['Alice Smith', 'Bob Jones']
