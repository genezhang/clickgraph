SELECT 
      if(match(toString(3.5), '^-?[0-9]+$'), concat(toString(3.5), '.0'), toString(3.5)) AS "s"
