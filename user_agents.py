#Football Analytics

#Scraper to obtain multiple user-agents and insert them into the user_agents table

#Author: Liam Culligan
#Date: January 2017

#Import required packages and functions
import pymysql
pymysql.install_as_MySQLdb() #Install MySQL driver
import MySQLdb as my
import requests
from bs4 import BeautifulSoup

#Connect to the MySQL database
db = my.connect(host = 'localhost', user = 'root', passwd = '', db = 'football_analytics')

cursor = db.cursor()

#Get user-agents
#Get url
url = "http://techpatterns.com/downloads/firefox/useragentswitcher.xml"
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
r = requests.get(url, headers = headers)

#Convert xml to BeautifulSoup object
soup = BeautifulSoup(r.content, "xml")

#Initialise empty list
user_agents = []

#Find all folder tags
folders = soup.find_all("folder")

#Loop through these tags
for folder in folders:
    if (folder["description"] == "Browsers - Windows") | \
        (folder["description"] == "Browsers - Mac") | \
         (folder["description"] == "Browsers - Linux") | \
         (folder["description"] == "Browsers - Unix") | \
         (folder["description"] == "Mobile Devices"):
        for user in folder.find_all("useragent"):
            try:
                row = [user["useragent"]]
                user_agents.append(row)
            except:
                pass

#Insert unique user_agents into the table
sql = ("INSERT IGNORE INTO user_agents "
    "(user_agent) "
    "VALUES (%s)")

sql_execute = cursor.executemany(sql, user_agents)

#Commit the query
db.commit()
#Close the connection
db.close()