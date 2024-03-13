install.packages('RSQLite')
library(DBI)
library(dplyr)
library(tidyverse)
library(magrittr)
if (file.exists("Airline2.db")) 
  file.remove("Airline2.db")
conn <- dbConnect(RSQLite::SQLite(), "Airline2.db")
dbListTables(conn)
setwd("/Users/wongrachel/Desktop/programming/03 sql/")
conn <- dbConnect(RSQLite::SQLite(), "Airline2.db")
dbListTables(conn)


ontime <- read.csv("combined.csv", header = TRUE)
airports <- read.csv("airports.csv", header = TRUE)
planes <- read.csv("plane-data.csv", header = TRUE)
carriers <- read.csv("carriers.csv", header = TRUE)

q1 <- dbGetQuery(conn, 
           "SELECT model AS model, AVG(ontime.DepDelay) AS avg_delay
           
           FROM planes JOIN ontime USING(tailnum)
           
           WHERE ontime.Cancelled = 0 AND ontime.Diverted = 0 AND ontime.DepDelay > 0
           
           GROUP BY model
           
           ORDER BY avg_delay")


q1

q2 <- dbGetQuery(conn,
                 "SELECT airports.city AS city, COUNT(*) AS total

FROM airports JOIN ontime ON ontime.dest = airports.iata

WHERE ontime.Cancelled = 0

GROUP BY airports.city

ORDER BY total DESC")

q2

q3 <- dbGetQuery(conn,
                 "SELECT carriers.Description AS carrier, COUNT(*) AS total

FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code

WHERE ontime.Cancelled = 1

    AND carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')

GROUP BY carriers.Description

ORDER BY total DESC")
q3

q4 <- dbGetQuery(conn, 
                  "SELECT
                q1.carrier AS carrier, (CAST(q1.numerator AS FLOAT)/ CAST(q2.denominator AS FLOAT)) AS ratio
                  
                  FROM
                  
                  (
                    
                    SELECT carriers.Description AS carrier, COUNT(*) AS numerator
                    
                    FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
                    
                    WHERE ontime.Cancelled = 1 AND carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
                    
                    GROUP BY carriers.Description
                    
                  ) AS q1 JOIN
                  
                  (
                    
                    SELECT carriers.Description AS carrier, COUNT(*) AS denominator
                    
                    FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
                    
                    WHERE carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
                    
                    GROUP BY carriers.Description
                    
                  ) AS q2 USING(carrier)
                  
                  ORDER BY ratio DESC ")
q4


dbDisconnect()