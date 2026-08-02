SELECT 
      toUnixTimestamp64Milli(fromUnixTimestamp64Milli(r.follow_date) + toIntervalDay(7)) AS "d"
FROM social.user_follows_bench AS r
