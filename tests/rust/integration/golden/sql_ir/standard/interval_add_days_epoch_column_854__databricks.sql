SELECT 
      unix_millis(timestamp_millis(r.follow_date) + make_dt_interval(7, 0, 0, 0)) AS `d`
FROM social.user_follows_bench AS r
