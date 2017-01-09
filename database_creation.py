#Football Analytics

#Database and tables creation

#Author: Liam Culligan
#Date: January 2017

#Import required packages and functions
import pymysql
pymysql.install_as_MySQLdb() #Install MySQL driver
import MySQLdb as my

#Connect to the localhost
db = my.connect(host = 'localhost', user = 'root', passwd = '')

cursor = db.cursor()

#Create the database
sql = ("CREATE DATABASE football_analytics COLLATE 'utf8_general_ci'")

sql_execute = cursor.execute(sql)

#Commit the query
db.commit()
#Close the connection
db.close()
#Connect to the newly-created database
db = my.connect(host = 'localhost', user = 'root', passwd = '', db = 'football_analytics')

cursor = db.cursor()

#Create players table
sql = ("CREATE TABLE players "
       "(player_id MEDIUMINT(8) UNSIGNED PRIMARY KEY, "
        "first_name VARCHAR(255), "
         "last_name VARCHAR(255), "
         "name VARCHAR(255), "
         "surname VARCHAR(255), "
         "photo VARCHAR(255), "
         "position VARCHAR(20), "
         "dob DATE, "
         "weight TINYINT(3) UNSIGNED, "
         "height TINYINT(3) UNSIGNED, "
         "country VARCHAR(255), "
         "KEY country (country))")

sql_execute = cursor.execute(sql)

#For some events, player_id is 0 -- i.e. no player
#But player_id is a foreign key, therefore must add player_id 0
sql = ("INSERT INTO players "
    "(player_id) "
    "VALUES (%s)")

sql_execute = cursor.execute(sql, 0)

#Create teams table
sql = ("CREATE TABLE teams "
       "(team_id MEDIUMINT(8) UNSIGNED PRIMARY KEY, "
        "long_name VARCHAR(255), "
         "short_name VARCHAR(255), "
         "logo VARCHAR(255), "
         "colour VARCHAR(255))")

sql_execute = cursor.execute(sql)

#Create seasons table
sql = ("CREATE TABLE seasons "
       "(season_id TINYINT(3) UNSIGNED PRIMARY KEY AUTO_INCREMENT, "
        "season_name VARCHAR(7))")

sql_execute = cursor.execute(sql)

#Insert season rows
sql = ("INSERT INTO seasons "
    "(season_name) "
    "VALUES (%s)")

sql_execute = cursor.executemany(sql, ["2009/10", "2010/11", "2011/12", "2012/13", "2013/14", \
                                       "2014/15", "2015/16", "2016/17"])

#Create competitions table
sql = ("CREATE TABLE competitions "
       "(competition_id TINYINT(3) UNSIGNED PRIMARY KEY AUTO_INCREMENT, "
        "competition_name VARCHAR(100), "
        "competition_code VARCHAR(50))")

sql_execute = cursor.execute(sql)

#Insert competition rows
sql = ("INSERT INTO competitions "
    "(competition_name, competition_code) "
    "VALUES (%s, %s)")

competitions = [["English Premier League", "epl"], ["Spanish La Liga", "laliga"], \
                ["German Bundesliga", "bliga"], ["Italian Serie A", "seriea"], \
                ["French Ligue 1", "ligue1"], ["Dutch Eredivisie", "eredivisie"], \
                ["English Championship", "championship"], ["US Major League Soccer", "mls"], \
                ["Russian Premier League", "rpl"], ["Brazilian Serie A", "brasil-serie-a"], \
                ["Turkish Super Lig", "turkish-super-lig"], \
                ["UEFA Champions League", "champions-league"], \
                ["UEFA Europa League", "europa-league"], ["Africa Cup of Nations", None], \
                ["World Cup", None], ["European Championships", None], \
                ["Confederations Cup", None], ["English FA Cup", None], \
                ["Australian A-League", "a-league"], ["Mexican Primera", "liga-mx"]]

sql_execute = cursor.executemany(sql, competitions)

#Create matches table
sql = ("CREATE TABLE matches "
       "(match_id INT(10) UNSIGNED PRIMARY KEY, "
        "season_id TINYINT(3) UNSIGNED, "
        "competition_id TINYINT(3) UNSIGNED, "
        "date_time DATETIME, "
        "venue VARCHAR(255), "
        "KEY date_time (date_time), "
         "FOREIGN KEY (season_id) REFERENCES seasons(season_id), "
         "FOREIGN KEY (competition_id) REFERENCES competitions(competition_id))")

sql_execute = cursor.execute(sql)

#Create match_teams table
sql = ("CREATE TABLE match_teams "
       "(match_id INT(10) UNSIGNED, "
        "team_id MEDIUMINT(8) UNSIGNED, "
        "team_goals_for TINYINT(3) UNSIGNED, "
        "team_goals_against TINYINT(3) UNSIGNED, "
        "team_venue VARCHAR(1), "
        "team_result VARCHAR(1), "
        "PRIMARY KEY (match_id, team_id), "
         "FOREIGN KEY (match_id) REFERENCES matches(match_id), "
         "FOREIGN KEY (team_id) REFERENCES teams(team_id))")

sql_execute = cursor.execute(sql)

#Create match_players table
sql = ("CREATE TABLE match_players "
       "(match_id INT(10) UNSIGNED, "
        "player_id MEDIUMINT(8) UNSIGNED, "
        "team_id MEDIUMINT(8) UNSIGNED, "
        "shirt_num TINYINT(3) UNSIGNED, "
        "x_loc TINYINT(3) UNSIGNED, "
        "y_loc TINYINT(3) UNSIGNED, "
        "start TINYINT(1) UNSIGNED, "
        "starting_minute DECIMAL(4, 1) UNSIGNED, "
        "finishing_minute DECIMAL(4, 1) UNSIGNED, "
        "total_minutes DECIMAL(4, 1) UNSIGNED, "
        "PRIMARY KEY (match_id, player_id), "
         "FOREIGN KEY (match_id) REFERENCES matches(match_id), "
         "FOREIGN KEY (player_id) REFERENCES players(player_id), "
         "FOREIGN KEY (team_id) REFERENCES teams(team_id))")

sql_execute = cursor.execute(sql)

#Create match_team_possession table
sql = ("CREATE TABLE match_team_possession "
       "(match_id INT(10) UNSIGNED, "
        "period TINYINT(1) UNSIGNED, "
        "slice_max_minute TINYINT(3) UNSIGNED, "
        "team_id MEDIUMINT(8) UNSIGNED, "
        "slice_name VARCHAR(15), "
        "team_possession TINYINT(3) UNSIGNED, "
        "slice_minutes TINYINT(2) UNSIGNED, "
        "PRIMARY KEY (match_id, period, slice_max_minute, team_id), "
        "FOREIGN KEY (match_id) REFERENCES matches(match_id), "
        "FOREIGN KEY (team_id) REFERENCES teams(team_id))")

sql_execute = cursor.execute(sql)

#Create match_event_types table
sql = ("CREATE TABLE match_event_types "
       "(event_type_id SMALLINT(4) UNSIGNED PRIMARY KEY AUTO_INCREMENT, "
       "event_type_name VARCHAR(30), "
       "UNIQUE (event_type_name))")

sql_execute = cursor.execute(sql)

#Insert certain known match_event_types - these types are referenced
#specifically in scraping.py
sql = ("INSERT IGNORE INTO match_event_types "
    "(event_type_name) "
    "VALUES (%s)")

sql_execute = cursor.executemany(sql, ["pass", "cross", "corner", "goal", "save", \
                                       "off_target", "blocked", "wood_work", "foul", \
                                       "offside", "headed_duel", "tackle", "tackled", \
                                       "take_on", "taken_on", "yellow_card", \
                                       "second_yellow_card", "red_card", "clearance", \
                                       "fouled", "blocked_shot", "goalkeeper_save", \
                                       "goalkeeper_punch", "goalkeeper_clearance", \
                                       "goalkeeper_failedcatch"])

#Create match_events table
sql = ("CREATE TABLE match_events "
       "(match_id INT(10) UNSIGNED, "
       "match_event_id SMALLINT(5) UNSIGNED, "
        "player_id MEDIUMINT(8) UNSIGNED, "
        "type_id SMALLINT(4) UNSIGNED, "
        "period TINYINT(1) UNSIGNED, "
        "minsec SMALLINT(4) UNSIGNED, "
        "x DECIMAL(4,1) UNSIGNED, "
        "y DECIMAL(4,1) UNSIGNED, "
        "end_x DECIMAL(4,1) UNSIGNED, "
        "end_y DECIMAL(4,1) UNSIGNED, "
        "gmouth_y DECIMAL(4,1) UNSIGNED, "
        "gmouth_z DECIMAL(4,1) UNSIGNED, "
        "headed TINYINT(1) UNSIGNED, "
        "longball TINYINT(1) UNSIGNED, "
        "throughball TINYINT(1) UNSIGNED, "
        "throw_in TINYINT(1) UNSIGNED, "
        "foul TINYINT(1) UNSIGNED, "
        "swerve VARCHAR(20), "
        "penalty TINYINT(1) UNSIGNED, "
        "freekick TINYINT(1) UNSIGNED, "
        "own_goal TINYINT(1) UNSIGNED, "
        "shot_assist TINYINT(1) UNSIGNED, "
        "goal_assist TINYINT(1) UNSIGNED, "
        "shot_match_event_id SMALLINT(5) UNSIGNED, "
        "attacking_box TINYINT(1) UNSIGNED, "
        "defensive_box TINYINT(1) UNSIGNED, "
        "end_attacking_box TINYINT(1) UNSIGNED, "
        "end_defensive_box TINYINT(1) UNSIGNED, "
        "outcome TINYINT(1) UNSIGNED, "
        "PRIMARY KEY (match_id, match_event_id), "
        "FOREIGN KEY (match_id) REFERENCES matches(match_id), "
        "FOREIGN KEY (player_id) REFERENCES players(player_id), "
        "FOREIGN KEY (type_id) REFERENCES match_event_types(event_type_id), "
        "KEY minsec (minsec), "
        "KEY x_y (x, y), "
        "KEY end_x_end_y (end_x, end_y))")

sql_execute = cursor.execute(sql)

#Create table of possible user_agents
sql = ("CREATE TABLE user_agents "
       "(user_agent VARCHAR(255) PRIMARY KEY)")

sql_execute = cursor.execute(sql)

#Create table of matches to add to the database
sql = ("CREATE TABLE add_matches "
       "(match_id INT(10) UNSIGNED PRIMARY KEY, "
       "competition_id TINYINT(3) UNSIGNED , "
         "season_id TINYINT(3) UNSIGNED, "
         "added TINYINT(1) UNSIGNED DEFAULT '0', "
         "failed TINYINT(1) UNSIGNED DEFAULT '0', "
          "KEY added (added))")

sql_execute = cursor.execute(sql)

#Commit the query
db.commit()
#Close the connection
db.close()
