SELECT 
      unix_millis(timestamp_millis(u.registration_date) - make_ym_interval(0, 1)) AS `d`
FROM social.users_bench AS u
