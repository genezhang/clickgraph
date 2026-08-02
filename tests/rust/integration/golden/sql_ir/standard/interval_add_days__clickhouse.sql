SELECT 
      u.registration_date + toIntervalDay(7) AS "d"
FROM social.users_bench AS u
