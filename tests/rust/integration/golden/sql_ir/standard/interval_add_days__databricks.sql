SELECT 
      unix_millis(timestamp_millis(u.registration_date) + make_dt_interval(7, 0, 0, 0)) AS `d`
FROM social.users_bench AS u
