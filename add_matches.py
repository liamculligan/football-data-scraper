#Football Analytics

#Add matches to the database that will be scraped

#Author: Liam Culligan
#Date: January 2017

#Import required packages and functions
import pymysql
pymysql.install_as_MySQLdb() ##Install MySQL driver
import MySQLdb as my

#Connect to the MySQL database
db = my.connect(host = 'localhost', user = 'root', passwd = '', db = 'football_analytics')

cursor = db.cursor()

#Finding the matches to add is a manual process
#Need to locate the relevant range of match ids
 
#Initialise an empty list
matches = []

#For example...

#English Premier League - competition_id 1
#2012/2013 - season_id 4

#Loop through all match ids for the selected league-season
#e.g. from 32 to 411
for match_id in range(32, 412):
    row = [match_id, 1, 4]
    matches.append(row)

#La Liga - competition_id 2
#2015/2016 - season_id 7

#Loop through all match ids for the selected league-season
for match_id in range(15402, 15782):
    row = [match_id, 2, 7]
    matches.append(row)
    
#Insert the matches that will be scraped into the add_matches table 
sql = ("INSERT IGNORE INTO add_matches "
      "(match_id, competition_id, season_id) "
      "VALUES (%s, %s, %s)")
                
sql_execute = cursor.executemany(sql, matches)

#Commit the query
db.commit()

#Close the connection
db.close()