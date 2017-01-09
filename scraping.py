#Football Analytics

#Scraping Squawka XML pages
#Matches to be scraped must first be manually added using add_matches.py

#Author: Liam Culligan
#Date: January 2017

#Import required packages and functions
import pymysql
pymysql.install_as_MySQLdb() #Install MySQL driver
import MySQLdb as my
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re
import pandas as pd
import numpy as np
import time
import random

#Define functions
def get_goals_for(score, index):
   return(score[index].strip())
   
def get_goals_against(score, index):
    if index == 0:
        goals_against = score[1].strip()
    else:
        goals_against = score[0].strip()
    return(goals_against)
    
def get_match_result(goals_for, goals_against):
    if goals_for > goals_against:
        result = "W"
    elif goals_for == goals_against:
        result = "D"
    else:
        result = "L"
    return(result)
 
def starting(state):
    if state == "playing":
        x = 1
    else:
        x = 0
    return(x)
    
def total_minutes(period_1, period_2, period_3 = 0, period_4 = 0, extra_time = 0):
    return(90 + (extra_time * 30) + (period_1)+ (period_2) + (period_3) + (period_4))

def replace_unknown(x):
    if x == "Unknown":
        x = None
    return(x)

def get_time_slice_name(name, scored_min, injurytime_play):
    if injurytime_play == 1:
        name = scored_min + "+"
    else:
        name = name
    return(name)

def get_period(name):
    max_time = int(re.split(" - ", re.search("[0-9]+ - [0-9]+", name).group(0))[1])
    if max_time <= 45:
        period = 1
    elif max_time <= 90:
        period = 2
    elif max_time <= 105:
        period = 3
    else:
        period = 4
    return(period)
    
def get_true_false(x):
    if x == "true":
        dummy = 1
    else:
        dummy = 0
    return(dummy)

#Connect to the MySQL database
db_add_matches = my.connect(host = 'localhost', user = 'root', passwd = '', db = 'football_analytics')
db = my.connect(host = 'localhost', user = 'root', passwd = '', db = 'football_analytics')

#Create a cursor for each connection
cursor_add_matches = db_add_matches.cursor()
cursor = db.cursor()

#SQL query to select user_agents
sql = ("SELECT user_agent FROM user_agents")
sql_execute = cursor.execute(sql)
headers = cursor.fetchall()

#SQL query to select all matches to be scraped, in a random order
sql = ("SELECT m.match_id, m.competition_id,  m.season_id, c.competition_code " 
       "FROM add_matches m "
       "JOIN competitions c "
       "ON m.competition_id = c.competition_id "
       "WHERE m.added = 0 "
       "ORDER BY RAND()")

sql_execute = cursor_add_matches.execute(sql)

#Loop through matches
for (current_match_id, competition_id, season_id, competition_code) in cursor_add_matches:
    
    match_url_id = str(current_match_id)
    #Get the url of the current match to be scraped
    url = "http://s3-irl-" + competition_code + ".squawka.com/dp/ingame/" + match_url_id
    
    #Set user-agent
    header = {'User-Agent':  headers[random.randint(0, len(headers) - 1)][0]}
              
    #Execute request
    try:
        r = requests.get(url, headers = header, timeout = 30)
        
        #Only status code 200 means a successful call
        if r.status_code != 200:
            #Update current_match_id in database - failed = 4 means a status code error
            sql = ("UPDATE add_matches "
                   "SET failed = 4 "
                   "WHERE match_id = %s")
            
            sql_execute = cursor.execute(sql, (current_match_id))
            
            #Commit the query
            db.commit()
            
            #Don't scrape again for between 5 and 10 seconds         
            time.sleep(random.uniform(5, 10))
        
            #If the connection has failed, no need to proceed with this match
            continue
        
    except:
        #Update current_match_id in database - failed = 3 means connection cannot be made to
        #this url
        sql = ("UPDATE add_matches "
               "SET failed = 3 "
               "WHERE match_id = %s")
        
        sql_execute = cursor.execute(sql, (current_match_id))
        
        #Commit the query
        db.commit()
        
        #If the connection has failed, no need to proceed with this match
        continue
    
    #Convert xml to BeautifulSoup object
    soup = BeautifulSoup(r.content, "xml")
    
    #Look for error code - most likely if this match is missing on Squawka
    try:
        match_error = soup.Error.Code.text
        
        #Update current_match_id in database - failed = 1 means match was not found (wrong url)
        sql = ("UPDATE add_matches "
               "SET failed = 1 "
               "WHERE match_id = %s")
        
        sql_execute = cursor.execute(sql, (current_match_id))
        
        #Commit the query
        db.commit()
        
        #Don't scrape again for between 5 and 10 seconds         
        time.sleep(random.uniform(5, 10))
    
        #If the url cannot be found, no need to proceed with this match
        continue
    
    except:
        #If there is no error code
        pass
        
    #If anything fails within a particular match, simply proceed to the next match
    try:
        #Get the match score
        score = re.split(" - ", re.search(" [0-9]+ - [0-9]+ ", soup.system.headline.text).group(0))
        
        #Initialise empty lists
        teams = []
        match_teams = []
        
        #In rare cases, the tag "game" is missing
        #Try find this tag, if it is missing, find the team names in the database
        try:
            
            #### Matches
            #Create teams data frame
            match_data = soup.game
            
            #Get the match data
            match = [match_url_id, season_id, competition_id, \
                     datetime.strptime(match_data.kickoff.text,'%a, %d %b %Y %H:%M:%S %z').strftime('%Y-%m-%d %H:%M:%S'), \
                     match_data.venue.text]
            ###
        
            ### Teams
            #Get team data
            team_data = soup.game.find_all("team")
            
            for team in team_data:
                row = [team["id"], team.long_name.text, team.short_name.text
                       , \
                       team.logo.text, team.team_color.text]
                teams.append(row)
            ###
            
            ### Match Teams
            
            index = 0
            for team in soup.game.find_all("team"):
                
                goals_for =  get_goals_for(score, index)
                goals_against =  get_goals_against(score, index)
                
                row = [match_url_id, team["id"], goals_for, goals_against, \
                       team.state.text[0].upper(), get_match_result(goals_for, goals_against)]
                match_teams.append(row)
                index += 1
        except:
            #If the game tag is missing
            
            #Add the match data without using the game tag
            match = [match_url_id, season_id, competition_id, None, None]
            
            #Use regular expressions to split home and away teams
            team_names = re.split(" [0-9]+ - [0-9]+ ", re.search("[aA-zZ]+ [0-9]+ - [0-9]+ [aA-zZ]+", soup.system.headline.text).group(0))
            
            index = 0
            for team_name in team_names:
                sql = ("SELECT team_id FROM teams WHERE short_name = %s")
                sql_execute = cursor.execute(sql, team_name)
        
                for (team_id) in cursor:
                    goals_for =  get_goals_for(score, index)
                    goals_against =  get_goals_against(score, index)
                    
                    if index == 0:
                        state = "H"
                    else:
                        state = "A"
                    
                    row = [match_url_id, team_id, goals_for, goals_against, \
                           state, get_match_result(goals_for, goals_against)]
                    match_teams.append(row)
                    index += 1
        ###
        
        ### Players
        #Extract the required data
        player_data = soup.players.find_all("player")
        
        #Initialise an empty list
        players = []
        
        #Initialise an empty dictionary
        player_teams = {}
        
        for player in player_data:
            
            if  player.dob.text == "00/00/0000":
                dob = None
            else:
                dob = datetime.strptime(player.dob.text,'%d/%m/%Y').strftime('%Y-%m-%d')
                
            row = [player["id"], player.first_name.text, player.last_name.text, \
                           player.find("name").text, player.surname.text, player.photo.text, \
                            player.position.text, dob, \
            replace_unknown(player.weight.text), \
            replace_unknown(player.height.text), \
             player.country.text]
            players.append(row)
        
            player_teams[int(player["id"])] = int(player["team_id"])
        ###
                
        ### Match Players
        #Find all players and their starting status for the match
        player_data = soup.players.find_all("player")
        
        match_id = []
        player_id = []
        team_id = []
        shirt_num = []
        x_loc = []
        y_loc = []
        start = []
        
        for player in player_data:
            match_id.append(match_url_id)
            player_id.append(player["id"])
            team_id.append(player["team_id"])
            shirt_num.append(player.shirt_num.text)
            x_loc.append(player.x_loc.text)
            y_loc.append(player.y_loc.text)
            start.append(starting(player.state.text))
        
        match_players = pd.DataFrame({'match_id' : match_id, 'player_id' : player_id, 'team_id' : team_id, \
                                      'shirt_num' : shirt_num, 'x_loc' : x_loc, 'y_loc' : y_loc, \
                                      'start' : start})
        
        try:
            #Find the total minutes played in the match
            period_1 = int(soup.possession.find_all("period")[0].find_all("time_slice")[8]["itp_mins"])
            period_2 = int(soup.possession.find_all("period")[1].find_all("time_slice")[8]["itp_mins"])
        except:
            #Earlier seasons didn't track injury time minutes
            period_1 = 0
            period_2 = 0
            
        try:
            #Try find a third period time, for extra time
            period_3_exists = soup.possession.find_all("period")[2].find_all("time_slice")
            #If one is found, extra time must have been played
            extra_time = 1
            try:
                #If there was extra time, try find the minutes played
                period_3 = int(soup.possession.find_all("period")[2].find_all("time_slice")[3]["itp_mins"])
            except:
                #If injury are not recorded, set to 0
                period_3 = 0
        except:
            period_3 = 0
            extra_time = 0
        
        try:
            period_4 = int(soup.possession.find_all("period")[3].find_all("time_slice")[3]["itp_mins"])
        except:
            period_4 = 0
            
        match_minutes = total_minutes(period_1, period_2, period_3, period_4, extra_time)
                        
        #Add a columns indicating player starting and finishing minute
        match_players = match_players.assign(starting_minute = 0, finishing_minute = match_minutes)
        
        match_players.loc[match_players['start'] == 0,'starting_minute'] = np.nan
        match_players.loc[match_players['start'] == 0,'finishing_minute'] = np.nan
        
        #Find all substitutions for the match
        periods = soup.possession.find_all("period")
        
        for period in periods:
            
            #To account for injury time
            if period == 2:
                injury_mins = period_1
            elif period == 3:
                injury_mins = period_1 + period_2
            elif period == 4:
                injury_mins = period_1 + period_2 + period_3
            else:
                injury_mins = 0
                
            time_slices = period.find_all("time_slice")
            for time_slice in time_slices:
                swap_players = time_slice.find_all("swap_players")
                for swap_player in swap_players:
                    
                    #Minsec not always available
                    try: 
                        minsec = swap_player['minsec']
                    except:
                        minsec = swap_player['min'] * 60
                        
                    match_players.loc[match_players['player_id'] == swap_player.sub_to_player['player_id'],'starting_minute'] = \
                    int(minsec)/60 + injury_mins
                    match_players.loc[match_players['player_id'] == swap_player.sub_to_player['player_id'],'finishing_minute'] = \
                    match_minutes
                    match_players.loc[match_players['player_id'] == swap_player.player_to_sub['player_id'],'finishing_minute'] = \
                    int(minsec)/60 + injury_mins
        
        #Calculate player minutes for the match
        match_players = match_players.assign(total_minutes = match_players.finishing_minute - \
                                             match_players.starting_minute)
        
        #Round to one decimal place
        match_players = match_players.round(decimals = 1)
        
        #MySQL does not recognise nan
        match_players = match_players.where((pd.notnull(match_players)), None)
        
        #Convert the data frame to a list of lists
        match_players = match_players.values.tolist()
        ###
        
        ### Match Team Possessions
        #Get the match_team_possession data
        match_team_possession = []
        
        periods = soup.possession.find_all("period")
        
        for period in periods:
            time_slices = period.find_all("time_slice")
            for time_slice in time_slices:
                team_possessions = time_slice.find_all("team_possession")
                for team_possession in team_possessions:
                    
                    try:
                        if team_possession["injurytime_play"] == "1":
                            time_slice_minutes = int(time_slice["itp_mins"])
                        else:
                            time_slice_minutes = 5
                    except:
                        time_slice_minutes = 5
                        
                    try:
                        injurytime_play = int(team_possession["injurytime_play"])
                    except:
                        injurytime_play = 0
                        
                    row = [match_url_id, period["id"], \
                           int(time_slice["scored_min"]) + (injurytime_play * time_slice_minutes), \
                               team_possession["team_id"], \
                            get_time_slice_name(time_slice["name"], time_slice["scored_min"], injurytime_play), \
                        team_possession.text, time_slice_minutes]
                    match_team_possession.append(row)
        ###
        
        ### Match Events
        #Extract the required goal_keeping events
        goal_keeping = soup.filters.goal_keeping
        
        #Extract the required goals_attempts events
        goals_attempts = soup.filters.goals_attempts
        
        #Extract the required goals_attempts events
        headed_duels = soup.filters.headed_duals
        
        #Extract the required goals_attempts events
        interceptions = soup.filters.interceptions
        
        #Extract the required clearances events
        clearances = soup.filters.clearances
        
        #Extract the required all_passes events
        all_passes = soup.filters.all_passes
        
        #Extract the required tackles events
        tackles = soup.filters.tackles
        
        #Extract the required crosses events
        crosses = soup.filters.crosses
        
        #Extract the required crosses events
        corners = soup.filters.corners
        
        #Extract the required offside events
        offside = soup.filters.offside
        
        #Extract the required takeons events
        takeons = soup.filters.takeons
            
        #Extract the required cards events
        cards = soup.filters.cards
        
        #Extract the required cards blocked_events
        blocked_events = soup.filters.blocked_events
        
        #Extract the required cards balls_out
        balls_out = soup.filters.balls_out
        
        #Extract the required heat_map events
        heat_maps = soup.filters.extra_heat_maps
        
        #Initialise an empty list
        events = []
        
        for time_slice in goal_keeping.find_all("time_slice"):
            period = get_period(time_slice["name"])
            for event in time_slice.find_all("event"):
                
                #Some goalkeeping event types have the same names as other events
                #Prepend 'goalkeeper' to goalkeeper events
                event_type = "goalkeeper_" + event["type"]
        
                coordinates = re.split(",", event.text)
                
                try:
                    headed = get_true_false(event["headed"])
                except:
                    headed = 0
                    
                #Sometimes minsec is available, otherwise mins and secs may be avaialble
                #Very rarely, no time info is provided for some matches
                try:
                    minsec = event["minsec"]
                except:
                    try:
                        minsec = int(event["mins"]) * 60 + int(event["secs"])
                    except:
                        try:
                            minsec = int(event["mins"]) * 60
                        except:
                            minsec = None
                
                row = [match_url_id, event["player_id"], event_type, period, minsec, \
                       coordinates[0], coordinates[1], None, None, None, None, headed, None, None, None, \
                        None, None, None, None]
                events.append(row)
           
        for time_slice in goals_attempts.find_all("time_slice"):
            period = get_period(time_slice["name"])
            for event in time_slice.find_all("event"):
                
                #Sometimes minsec is available, otherwise mins and secs may be avaialble
                #Very rarely, no time info is provided for some matches
                try:
                    minsec = event["minsec"]
                except:
                    try:
                        minsec = int(event["mins"]) * 60 + int(event["secs"])
                    except:
                        try:
                            minsec = int(event["mins"]) * 60
                        except:
                            minsec = None
                
                try:
                    headed = get_true_false(event.headed.text)
                except:
                    headed = 0
                
                try:
                    swerve = event.swere.text
                except:
                    swerve = "none"
                     
                try:
                    x = event.coordinates["x"]
                    y = event.coordinates["y"]
                except:
                    coordinates = re.split(",", event.end.text) #Really is starting coordinates - error in XML
                    x = coordinates[0]
                    y = coordinates[1]
                    
                try:
                    end_x = event.coordinates["end_x"]
                    end_y = event.coordinates["end_y"]
                except:
                    end_x = None
                    end_y = None
                    
                try:
                    gmouth_y = event.coordinates["gmouth_y"]
                    gmouth_z = event.coordinates["gmouth_z"]
                except:
                    coordinates = re.split(",", event.start.text) #Really is gmouth coordinates - error in XML
                    gmouth_y = coordinates[0]
                    gmouth_z = coordinates[1]
                
                try:
                    own_goal = event["is_own"]
                    if own_goal == "yes":
                        own_goal = 1
                    else:
                        own_goal = 0
                except:
                    own_goal = 0
                
                row = [match_url_id, event["player_id"], event["type"], period, str(minsec), \
                       x, y, end_x, end_y, gmouth_y, gmouth_z, \
                        headed, None, None, None, None, swerve, own_goal, None]
                events.append(row)
        
        for time_slice in headed_duels.find_all("time_slice"):
            period = get_period(time_slice["name"])
            for event in time_slice.find_all("event"):
                
                #Sometimes minsec is available, otherwise mins and secs may be avaialble
                #Very rarely, no time info is provided for some matches
                try:
                    minsec = event["minsec"]
                except:
                    try:
                        minsec = int(event["mins"]) * 60 + int(event["secs"])
                    except:
                        try:
                            minsec = int(event["mins"]) * 60
                        except:
                            minsec = None
                
                if event["action_type"] == "Possession": #Only want headed duels; fouls accounted for elsewhere
                
                    coordinates = re.split(",", event.loc.text)
                    
                    row = [match_url_id, event["player_id"], "headed_duel", period, str(minsec), \
                           coordinates[0], coordinates[1], None, None, None, None, None, None, None, \
                            None, None, None, None, 1]
                    events.append(row)
                    
                    row = [match_url_id, event.otherplayer.text, "headed_duel", period, str(minsec), \
                           100 - float(coordinates[0]), 100 - float(coordinates[1]), None, None, None, None, None, None, None, \
                            None, None, None, None, 0]
                    events.append(row)
        
        for time_slice in interceptions.find_all("time_slice"):
            period = get_period(time_slice["name"])
            for event in time_slice.find_all("event"):
                
                #Sometimes minsec is available, otherwise mins and secs may be avaialble
                #Very rarely, no time info is provided for some matches
                try:
                    minsec = event["minsec"]
                except:
                    try:
                        minsec = int(event["mins"]) * 60 + int(event["secs"])
                    except:
                        try:
                            minsec = int(event["mins"]) * 60
                        except:
                            minsec = None
                
                coordinates = re.split(",", event.loc.text)
                
                try:
                    headed = get_true_false(event.headed.text)
                except:
                    headed = 0
                
                row = [match_url_id, event["player_id"], "interception", period, str(minsec), \
                       coordinates[0], coordinates[1], None, None, None, None, headed, None, None, \
                        None, None, None, None, None]
                events.append(row)
        
        for time_slice in clearances.find_all("time_slice"):
            period = get_period(time_slice["name"])
            for event in time_slice.find_all("event"):
                
                #Sometimes minsec is available, otherwise mins and secs may be avaialble
                #Very rarely, no time info is provided for some matches
                try:
                    minsec = event["minsec"]
                except:
                    try:
                        minsec = int(event["mins"]) * 60 + int(event["secs"])
                    except:
                        try:
                            minsec = int(event["mins"]) * 60
                        except:
                            minsec = None
                
                coordinates = re.split(",", event.loc.text)
                
                try:
                    headed = get_true_false(event.headed.text)
                except:
                    headed = 0
                    
                if (event["type"] == "success") | (event["type"] == "headed"): #success refers to successful kick clearance; headed to succesful headed clearance.
                    outcome = 1
                else:
                    outcome = 0
        
                row = [match_url_id, event["player_id"], "clearance", period, str(minsec), \
                      coordinates[0], coordinates[1], None, None, None, None, headed, None, None, \
                        None, None, None, None, outcome]
                events.append(row) 
                
        for time_slice in all_passes.find_all("time_slice"):
            period = get_period(time_slice["name"])
            for event in time_slice.find_all("event"):
                
                #Sometimes minsec is available, otherwise mins and secs may be avaialble
                #Very rarely, no time info is provided for some matches
                try:
                    minsec = event["minsec"]
                except:
                    try:
                        minsec = int(event["mins"]) * 60 + int(event["secs"])
                    except:
                        try:
                            minsec = int(event["mins"]) * 60
                        except:
                            minsec = None
                
                coordinates = re.split(",", event.start.text)
                coordinates_end = re.split(",", event.end.text)
                
                try:
                    headed = get_true_false(event.headed.text)
                except:
                    headed = 0
                
                try:
                    longball = get_true_false(event.long_ball.text)
                except:
                    longball = 0
                
                try:
                    throughball = get_true_false(event.through_ball.text)
                except:
                    throughball = 0
                    
                #Sometimes the throw_ins tag does not exist
                try:
                    throw_ins = event["throw_ins"]
                except:
                    throw_ins = None
                    
                if event["type"] == "completed":
                    outcome = 1
                else:
                    outcome = 0
                
                row = [match_url_id, event["player_id"], "pass", period, str(minsec), \
                      coordinates[0], coordinates[1], coordinates_end[0], coordinates_end[1], None, None, \
                 headed, longball, throughball, throw_ins, None, None, None, outcome]
                events.append(row) 
        
        for time_slice in tackles.find_all("time_slice"):
            period = get_period(time_slice["name"])
            for event in time_slice.find_all("event"):
                
                #Sometimes minsec is available, otherwise mins and secs may be avaialble
                #Very rarely, no time info is provided for some matches
                try:
                    minsec = event["minsec"]
                except:
                    try:
                        minsec = int(event["mins"]) * 60 + int(event["secs"])
                    except:
                        try:
                            minsec = int(event["mins"]) * 60
                        except:
                            minsec = None
                
                coordinates = re.split(",", event.loc.text)
                
                if event["type"] == "Success":
                    outcome = 1
                else:
                    outcome = 0
                    
                if event["type"] == "Fouled":
                    foul = 1
                else:
                    foul = 0
                
                #Get the event relating to the tackler
                row = [match_url_id, event.tackler.text, "tackle", period, str(minsec), \
                      coordinates[0], coordinates[1], None, None, None, None, \
                 None, None, None, None, foul, None, None, outcome]
                events.append(row) 
                
                #Get the event relating to the player tackled
                #In some cases the player_id tag does exist
                try:
                    if event["player_id"] != "":
                        
                        row = [match_url_id, event["player_id"], "tackled", period, str(minsec), \
                               100 - float(coordinates[0]), 100 - float(coordinates[1]), None, None, \
                                           None, None, None, None, None, None, foul, None, None, \
                                           outcome]
                        events.append(row)
                except:
                    #If player_id doesn't exist - don't add this row
                    pass
        
        for time_slice in crosses.find_all("time_slice"):
            period = get_period(time_slice["name"])
            for event in time_slice.find_all("event"):
                
                #Sometimes minsec is available, otherwise mins and secs may be avaialble
                #Very rarely, no time info is provided for some matches
                try:
                    minsec = event["minsec"]
                except:
                    try:
                        minsec = int(event["mins"]) * 60 + int(event["secs"])
                    except:
                        try:
                            minsec = int(event["mins"]) * 60
                        except:
                            minsec = None
                
                coordinates = re.split(",", event.start.text)
                coordinates_end = re.split(",", event.end.text)
        
                if (event["type"] == "Completed") | (event["type"] == "Assist"):
                    outcome = 1
                else:
                    outcome = 0
                
                row = [match_url_id, event["player_id"], "cross", period, str(minsec), \
                      coordinates[0], coordinates[1], coordinates_end[0], coordinates_end[1], None, None, \
                 None, None, None, None, None, None, None, outcome]
                events.append(row) 
                
        for time_slice in corners.find_all("time_slice"):
            period = get_period(time_slice["name"])
            for event in time_slice.find_all("event"):
                
                #Sometimes minsec is available, otherwise mins and secs may be avaialble
                #Very rarely, no time info is provided for some matches
                try:
                    minsec = event["minsec"]
                except:
                    try:
                        minsec = int(event["mins"]) * 60 + int(event["secs"])
                    except:
                        try:
                            minsec = int(event["mins"]) * 60
                        except:
                            minsec = None
                
                coordinates = re.split(",", event.start.text)
                coordinates_end = re.split(",", event.end.text)
        
                if (event["type"] == "Completed") | (event["type"] == "Assist"):
                    outcome = 1
                else:
                    outcome = 0
                
                try:
                    swerve = event.swere.text
                except:
                    swerve = "none"
                
                row = [match_url_id, event["player_id"], "corner", period, str(minsec), \
                      coordinates[0], coordinates[1], coordinates_end[0], coordinates_end[1], None, None, \
                 None, None, None, None, None, swerve, None, outcome]
                events.append(row)
        
        #In rare cases, offside is not provided or did not occur in a match    
        try:       
            for time_slice in offside.find_all("time_slice"):
                period = get_period(time_slice["name"])
                for event in time_slice.find_all("event"):
                    
                    #Sometimes minsec is available, otherwise mins and secs may be avaialble
                    #Very rarely, no time info is provided for some matches
                    try:
                        minsec = event["minsec"]
                    except:
                        try:
                            minsec = int(event["mins"]) * 60 + int(event["secs"])
                        except:
                            try:
                                minsec = int(event["mins"]) * 60
                            except:
                                minsec = None
                    
                    row = [match_url_id, event["player_id"], "offside", period, str(minsec), \
                           None, None, None, None, None, None, None, None, None, None, \
                            None, None, None, None]
                    events.append(row)
        except:
            pass
                
        for time_slice in takeons.find_all("time_slice"):
            period = get_period(time_slice["name"])
            for event in time_slice.find_all("event"):
                
                #Sometimes minsec is available, otherwise mins and secs may be avaialble
                #Very rarely, no time info is provided for some matches
                try:
                    minsec = event["minsec"]
                except:
                    try:
                        minsec = int(event["mins"]) * 60 + int(event["secs"])
                    except:
                        try:
                            minsec = int(event["mins"]) * 60
                        except:
                            minsec = None
                
                coordinates = re.split(",", event.loc.text)
        
                if event["type"] == "Success":
                    outcome = 1
                else:
                    outcome = 0
                
                row = [match_url_id, event["player_id"], "take_on", period, str(minsec), \
                      coordinates[0], coordinates[1], None, None, None, None, \
                 None, None, None, None, None, None, None, outcome]
                events.append(row)
                
                #In some cases the other_player tag does exist
                try:
                    if event["other_player"] != "":
                        row = [match_url_id, event["other_player"], "taken_on", period, str(minsec), \
                              100 - float(coordinates[0]), 100 - float(coordinates[1]), None, None, \
                                          None, None, None, None, None, None, None, None, None, \
                                          outcome]
                        events.append(row)
                except:
                    #If other_player doesn't exist - don't add this row
                    pass
               
        for time_slice in cards.find_all("time_slice"):
            period = get_period(time_slice["name"])
            for event in time_slice.find_all("event"):
                
                #Sometimes minsec is available, otherwise mins and secs may be avaialble
                #Very rarely, no time info is provided for some matches
                try:
                    minsec = event["minsec"]
                except:
                    try:
                        minsec = int(event["mins"]) * 60 + int(event["secs"])
                    except:
                        try:
                            minsec = int(event["mins"]) * 60
                        except:
                            minsec = None
                
                coordinates = re.split(",", event.loc.text)
                
                if event.card.text == "yellow":
                    type = "yellow_card"
                elif event.card.text == "2nd yellow":
                    type = "second_yellow_card"
                else:
                    type = "red_card"
             
                row = [match_url_id, event["player_id"], type, period, str(minsec), \
                      coordinates[0], coordinates[1], None, None, None, None, \
                 None, None, None, None, None, None, None, None]
                events.append(row)
        
        #In rare cases, blocked_events is not provided    
        try:   
            for time_slice in blocked_events.find_all("time_slice"):
                period = get_period(time_slice["name"])
                for event in time_slice.find_all("event"):       
                    
                    #"blocked_shot" is covered by other event_types - can ignore
                    if event["type"] != "blocked_shot":
                    
                        #Sometimes minsec is available, otherwise mins and secs may be avaialble
                        #Very rarely, no time info is provided for some matches
                        try:
                            minsec = event["minsec"]
                        except:
                            try:
                                minsec = int(event["mins"]) * 60 + int(event["secs"])
                            except:
                                try:
                                    minsec = int(event["mins"]) * 60
                                except:
                                    minsec = None
                        
                        #Only considering where a block event occurs
                        #For block shots, using x & y as the block location
                        
                        try:
                            coordinates = re.split(",", event.loc.text)
                        except:
                            coordinates = re.split(",", event.start.text)
                            
                        #Minsec only exists for blocked shots
                        #If not, can't calculate minsecs in injury time
                        
                        row = [match_url_id, event["player_id"], event["type"], period, str(minsec), \
                              coordinates[0], coordinates[1], None, None, None, None, \
                         None, None, None, None, None, None, None, None]
                        events.append(row)
        except:
            pass
         
        #In rare cases, ball_sout is not provided    
        try:        
            for time_slice in balls_out.find_all("time_slice"):
                period = get_period(time_slice["name"])
                for event in time_slice.find_all("event"):
                    
                    #Sometimes minsec is available, otherwise mins and secs may be avaialble
                    #Very rarely, no time info is provided for some matches
                    try:
                        minsec = event["minsec"]
                    except:
                        try:
                            minsec = int(event["mins"]) * 60 + int(event["secs"])
                        except:
                            try:
                                minsec = int(event["mins"]) * 60
                            except:
                                minsec = None
                    
                    coordinates = re.split(",", event.start.text)
                    coordinates_end = re.split(",", event.end.text)
            
                    row = [match_url_id, event["player_id"], "ball_out", period, str(minsec), \
                          coordinates[0], coordinates[1], coordinates_end[0], coordinates_end[1], None, None, \
                     None, None, None, None, None, None, None, None]
                    events.append(row) 
        except:
            pass
        
        #In rare cases, ot_id is not provided
        #Without this, it is preferable not to add this match to the database
        #Cannot identify many events. Only know that some event occured at a time
        try:
            for time_slice in heat_maps.find_all("time_slice"):
                period = get_period(time_slice["name"])
                for event in time_slice.find_all("event"):
                    
                    #Sometimes minsec is available, otherwise mins and secs may be avaialble
                    #Very rarely, no time info is provided for some matches
                    try:
                        minsec = event["minsec"]
                    except:
                        try:
                            minsec = int(event["mins"]) * 60 + int(event["secs"])
                        except:
                            try:
                                minsec = int(event["mins"]) * 60
                            except:
                                minsec = None
                    
                    ot_id = event["ot_id"]
                    ot_outcome = event["ot_outcome"]
                    if (ot_id == "1") | (ot_id == "2") | (ot_id == "4") | (ot_id == "10") | \
                        (ot_id == "11") | (ot_id == "61"):
                        if ot_id == "1":
                            type = "pass"
                        elif ot_id == "2":
                            type = "offside_pass"
                        elif ot_id == "4":
                            if ot_outcome == "0":
                                type = "foul"
                            else:
                                type = "fouled"
                            ot_outcome = None
                        elif ot_id == "10":
                            type = "blocked_shot"
                        elif ot_id == "11":
                            type = "goalkeeper_cross_claim"
                        elif ot_id == "61":
                            type = "bad_touch"
                            
                        coordinates = re.split(",", event.loc.text)
                            
                        row = [match_url_id, event["player_id"], type, period, str(minsec), \
                              coordinates[0], coordinates[1], None, None, None, None, \
                         None, None, None, None, None, None, None, ot_outcome]
                        events.append(row)
        except:
            #If ot_id does not exist, do not add this match to the database
            #Update current_match_id in database
            sql = ("UPDATE add_matches "
                   "SET failed = 5 "
                   "WHERE match_id = %s")
            
            sql_execute = cursor.execute(sql, (current_match_id))
            
            #Commit the query
            db.commit()
            
            #Don't scrape again for between 5 to 10 seconds         
            time.sleep(random.uniform(5, 10))
        
            #If the connection has failed, no need to proceed with this match
            continue
        
        #List of column names
        col_names = ["match_id", "player_id", "type", "period", "minsec", "x", "y", "end_x", "end_y", \
                     "gmouth_y", "gmouth_z", "headed", "longball", "throughball", "throw_in", \
                     "foul", "swerve", "own_goal", "outcome"]
                     
        #Create dataframe in order to create primary key
        events_df = pd.DataFrame(events, columns = col_names)
        
        #First replace empty strings with nan
        events_df = events_df.replace("", np.nan)
        
        #Change required columns for ordering
        events_df["minsec"] = events_df["minsec"].astype(int)
        events_df["player_id"] = events_df["player_id"].astype(int)
        events_df["x"] = events_df["x"].astype(float)
        events_df["y"] = events_df["y"].astype(float)
        events_df["end_x"] = events_df["end_x"].astype(float)
        events_df["end_y"] = events_df["end_y"].astype(float)
        
        #Order the dataframe
        events_df = events_df.sort_values(by = ["period", "minsec", "type", "player_id"])
        
        #Get unique event names
        event_names = events_df["type"].unique().tolist()
        
        #Insert any new event types that have not yet been added to the match_event_type table
        sql = ("INSERT IGNORE INTO match_event_types "
            "(event_type_name) "
            "VALUES (%s)")
        
        sql_execute = cursor.executemany(sql, event_names)
        
        #Commit the query
        db.commit()
        
        #Workaround to avoid having gaps in the autoincrementing primary key
        sql = ("SELECT MAX(event_type_id) FROM match_event_types")
        sql_execute = cursor.execute(sql)
        
        for (max_event_type_id) in cursor:
            #Set the autoincrementing primary key value to the current maximum value
            sql = ("ALTER TABLE match_event_types AUTO_INCREMENT = %s")
        
            sql_execute = cursor.execute(sql, max_event_type_id)
        
            #Commit the query
            db.commit()
        
        #Initialise an empty dictionary
        event_types = {}
        
        #SQL query to insert match_event_types
        sql = ("SELECT event_type_name, event_type_id FROM match_event_types")
        sql_execute = cursor.execute(sql)
        
        for (event_type_name, event_type_id) in cursor:
            event_types[event_type_name] = event_type_id
        
        #Remap type
        events_df = events_df.replace({"type": event_types})
        
        #Find all other events that are also recorded as corners (duplicate information)
        #Can be either a pass or a cross - need to remove both of these
        for shift_value in [-1, -1, 1, 1]:
          
            events_df = events_df[(events_df["type"].shift(shift_value) == event_types["corner"]) & \
             (events_df["x"].shift(shift_value) == events_df["x"]) & \
             (events_df["y"].shift(shift_value) == events_df["y"]) & \
             (events_df["player_id"].shift(shift_value) == events_df["player_id"]) & \
             (events_df["minsec"].shift(shift_value) == events_df["minsec"]) == False]
            
        #Remove passes that are also recorded as crosses
        for shift_value in [-1, 1]:
            
            events_df = events_df[(events_df["type"] == event_types["pass"]) & \
                                  (events_df["type"].shift(shift_value) == event_types["cross"]) & \
             (events_df["x"].shift(shift_value) == events_df["x"]) & \
             (events_df["y"].shift(shift_value) == events_df["y"]) & \
             (events_df["player_id"].shift(shift_value) == events_df["player_id"]) & \
             (events_df["minsec"].shift(shift_value) == events_df["minsec"]) == False]
        
        #Now add the row number as an id
        events_df["match_event_id"] = range(1, len(events_df) + 1)
        
        #Determine whether an event started and/or ended in the attacking or defensive box
        events_df["attacking_box"] = np.where((events_df["x"] >= 82) & \
            ((events_df["y"] >= 28) & (events_df["y"] <= 72)), 1, 0)
        
        events_df["attacking_box"] = np.where((pd.isnull(events_df["x"])) | \
            (pd.isnull(events_df["y"])), None, events_df["attacking_box"])
        
        events_df["defensive_box"] = np.where((events_df["x"] <= 18) & \
            ((events_df["y"] >= 28) & (events_df["y"] <= 72)), 1, 0)
        
        events_df["defensive_box"] = np.where((pd.isnull(events_df["x"])) | \
            (pd.isnull(events_df["y"])), None, events_df["defensive_box"])
        
        events_df["end_attacking_box"] = np.where((events_df["end_x"] >= 82) & \
            ((events_df["end_y"] >= 28) & (events_df["end_y"] <= 72)), 1, 0)
        
        events_df["end_attacking_box"] = np.where((pd.isnull(events_df["end_x"])) | \
            (pd.isnull(events_df["end_y"])), None, events_df["end_attacking_box"])
        
        events_df["end_defensive_box"] = np.where((events_df["end_x"] <= 18) & \
            ((events_df["end_y"] >= 28) & (events_df["end_y"] <= 72)), 1, 0)
        
        events_df["end_defensive_box"] = np.where((pd.isnull(events_df["end_x"])) | \
            (pd.isnull(events_df["end_y"])), None, events_df["end_defensive_box"])
        
        #Subset of possible setpiece events
        #Filter where event type is: pass, off_target, cross, save, foul, goal, blocked, wood_work
        events_setpiece = events_df.copy()
        events_setpiece = events_setpiece[(events_setpiece.type == event_types["pass"]) | \
                                     (events_setpiece.type == event_types["off_target"]) | \
                                      (events_setpiece.type == event_types["cross"]) | \
                                       (events_setpiece.type == event_types["save"]) | \
                                    (events_setpiece.type == event_types["foul"]) | \
                                     (events_setpiece.type == event_types["offside"]) | \
                                     (events_setpiece.type == event_types["goal"]) | \
                                     (events_setpiece.type == event_types["blocked"]) | \
                                      (events_setpiece.type == event_types["wood_work"])]                            
        
        #Find penalties
        events_setpiece["penalty"] = np.where((events_setpiece["type"].shift(1) == event_types["foul"]) & \
         (events_setpiece["defensive_box"].shift(1) == 1) & \
         ((events_setpiece["type"] == event_types["off_target"]) | \
           (events_setpiece["type"] == event_types["save"]) | \
          (events_setpiece["type"] == event_types["goal"]) | \
          (events_setpiece["type"] == event_types["blocked"]) | \
           (events_setpiece["type"] == event_types["wood_work"])) & \
         (events_setpiece["x"] >= 88) & (events_setpiece["x"] <=  89) & \
          (events_setpiece["y"] >= 49.5) & (events_setpiece["y"] <= 50.5), 1, 0)
         
        #Find freekicks
        events_setpiece["freekick"] = np.where(((events_setpiece["type"].shift(1) == event_types["foul"]) | \
        (events_setpiece["type"].shift(1) == event_types["offside"])) & \
         (events_setpiece["penalty"] == 0), 1, 0)
        
        #Left outer join the original events with events_setpieces
        events_df = pd.merge(events_df, events_setpiece[["match_event_id", "penalty", "freekick"]], \
                      on = "match_event_id", how = "left")
        
        #Find shot and goal assists
        assists_df = events_df.copy()
        assists_df = assists_df[(assists_df.type != event_types["headed_duel"]) & \
                                     (assists_df.type != event_types["tackle"]) & \
                                      (assists_df.type != event_types["tackled"]) & \
                                       (assists_df.type != event_types["take_on"]) & \
                                    (assists_df.type != event_types["taken_on"]) & \
                                     (assists_df.type != event_types["foul"]) & \
                                     (assists_df.type != event_types["goalkeeper_save"]) & \
                                     (assists_df.type != event_types["goalkeeper_punch"]) & \
                                     (assists_df.type != event_types["goalkeeper_failedcatch"]) & \
                                      (assists_df.type != event_types["yellow_card"]) & \
                                       (assists_df.type != event_types["second_yellow_card"]) & \
                                        (assists_df.type != event_types["red_card"]) & \
                                      (assists_df.type != event_types["blocked_shot"])]   
        
        #Adding teams to the dataframe using player_teams dictionary
        assists_df["team_id"] = assists_df["player_id"].map(player_teams)
        
        #Find shot assists (these include goal assists)
        assists_df["shot_assist"] = np.where(((assists_df["type"] == event_types["pass"]) | \
            (assists_df["type"] == event_types["clearance"]) | \
            (assists_df["type"] == event_types["goalkeeper_clearance"]) | \
            (assists_df["type"] == event_types["cross"]) | \
            (assists_df["type"] == event_types["corner"]) | \
            (assists_df["type"] == event_types["fouled"]) | \
            (assists_df["type"] == event_types["blocked"]) | \
            (assists_df["type"] == event_types["wood_work"])) & \
            ((assists_df["type"].shift(-1) == event_types["goal"]) | \
            (assists_df["type"].shift(-1) == event_types["save"]) | \
            (assists_df["type"].shift(-1) == event_types["off_target"]) | \
            (assists_df["type"].shift(-1) == event_types["blocked"]) | \
            (assists_df["type"].shift(-1) == event_types["wood_work"])) & \
            (assists_df["team_id"] == assists_df["team_id"].shift(-1)), 1, 0) 
        
        #Find goal assists
        assists_df["goal_assist"] = np.where((assists_df["shot_assist"] == 1) & \
            (assists_df["type"].shift(-1) == event_types["goal"]), 1, 0)
        
        #Find the event_ids for the shots that were assisted
        assists_df["shot_match_event_id"] = np.where((assists_df["shot_assist"] == 1), \
            assists_df["match_event_id"].shift(-1), None)
        
        #Left outer join the original events with assists_df to add assists columns
        events_df = pd.merge(events_df, assists_df[["match_event_id", "shot_assist", "goal_assist", \
                                                    "shot_match_event_id"]], \
                      on = "match_event_id", how = "left")
        
        #MySQL does not recognise nan - replace with None
        events_df = events_df.where((pd.notnull(events_df)), None)
        
        #Convert the data frame back to a list of lists
        events = events_df.values.tolist()
        ###
        
        #Add new players to the players table
        sql = ("INSERT IGNORE INTO players "
            "(player_id, first_name, last_name, name, surname, photo, position, dob, weight, "
             "height, country) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        
        sql_execute = cursor.executemany(sql, players)
        
        #Add new teams to the teams table
        sql = ("INSERT IGNORE INTO teams "
            "(team_id, long_name, short_name, logo, colour) "
            "VALUES (%s, %s, %s, %s, %s)")
        
        sql_execute = cursor.executemany(sql, teams)
        
        #Add new matches to the matches table
        sql = ("INSERT IGNORE INTO matches "
            "(match_id, season_id, competition_id, date_time, venue) "
            "VALUES (%s, %s, %s, %s, %s)")
        
        sql_execute = cursor.execute(sql, match)
        
        #Add new match_teams rows to the match_teams table
        sql = ("INSERT IGNORE INTO match_teams "
            "(match_id, team_id, team_goals_for, team_goals_against, team_venue, team_result) "
            "VALUES (%s, %s, %s, %s, %s, %s)")
        
        sql_execute = cursor.executemany(sql, match_teams)
        
        #Add new match_players rows to the match_players table
        sql = ("INSERT IGNORE INTO match_players "
            "(match_id, player_id, shirt_num, start, team_id, x_loc, y_loc, finishing_minute, \
            starting_minute, total_minutes) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        
        sql_execute = cursor.executemany(sql, match_players)
        
        #Add new match_team_possession rows to the match_team_possession table
        sql = ("INSERT IGNORE INTO match_team_possession "
            "(match_id, period, slice_max_minute, team_id, slice_name, team_possession, slice_minutes) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)")
        
        sql_execute = cursor.executemany(sql, match_team_possession)
        
        #Add new match events to the match_events table
        sql = ("INSERT IGNORE INTO match_events "
            "(match_id, player_id, type_id, period, minsec, x, y, end_x, end_y, "
            "gmouth_y, gmouth_z, headed, longball, throughball, throw_in, foul, swerve, "
            "own_goal, outcome, match_event_id, attacking_box, defensive_box, end_attacking_box , "
            "end_defensive_box, penalty, freekick, shot_assist, goal_assist, shot_match_event_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s, %s, %s, %s, %s, %s, %s, %s, %s)")
        
        sql_execute = cursor.executemany(sql, events)
        
        #Update current_match_id in database
        sql = ("UPDATE add_matches "
               "SET added = 1 "
               "WHERE match_id = %s")
        
        sql_execute = cursor.execute(sql, (current_match_id))
        
        #Commit the query
        db.commit()
        
    except:
        #Update match in database - failed = 2 means something has gone wrong with the
        #scraping itself
        sql = ("UPDATE add_matches "
               "SET failed = 2 "
               "WHERE match_id = %s")
    
        sql_execute = cursor.execute(sql, (current_match_id))
    
        #Commit the query
        db.commit()
      
    #Don't scrape again for between 5 to 10 seconds         
    time.sleep(random.uniform(5, 10))
                    
#Close the connection
db_add_matches.close()
db.close()