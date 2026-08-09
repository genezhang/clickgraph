SELECT 
      if(match(toString(0 - 2), '^-?[0-9]+$'), concat(toString(0 - 2), '.0'), toString(0 - 2)) AS "s"
