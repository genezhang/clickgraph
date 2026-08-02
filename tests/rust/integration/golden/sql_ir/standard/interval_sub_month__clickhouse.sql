SELECT 
      u.registration_date - toIntervalMonth(1) AS "d"
FROM social.users_bench AS u
