SELECT 
      if(match(toString(3), '^-?[0-9]+$'), concat(toString(3), '.0'), toString(3)) AS "s"
