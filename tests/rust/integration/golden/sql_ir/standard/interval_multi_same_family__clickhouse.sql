SELECT 
      toUnixTimestamp64Milli(fromUnixTimestamp64Milli(u.registration_date) + (toIntervalDay(5) + toIntervalHour(2))) AS "d"
FROM social.users_bench AS u
