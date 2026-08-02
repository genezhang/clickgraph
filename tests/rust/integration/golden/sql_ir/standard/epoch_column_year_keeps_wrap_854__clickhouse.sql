SELECT 
      toYear(fromUnixTimestamp64Milli(r.follow_date)) AS "y"
FROM social.user_follows_bench AS r
