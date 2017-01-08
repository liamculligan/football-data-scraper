# Football Data Scraper

## Introduction
The goal of this project is to automate the process of collecting and warehousing publicly available football data. Python's Beautiful Soup
is used for web scraping and the resulting data is stored in a MySQL database. The original data is provided by Opta, which tracks 
apporximately 1500 on-the-ball events in every football match that they cover. At the time of publishing this repository, Squawka's 
robots.txt file placed no limitations on scraping. The scraping performed by the script is very conservative, but I would nevertheless 
implore any potential user of the script to check the latest version of the robots.txt file before using the script.

## Execution
1. Create the MySQL database by executing the script `database_creation.py` <br>
2. Obtain a list of possible user-agents and add these to the database by executing the script `user_agents.py` <br>
3. Manually add the desired matches to be scraped to the script `add_matches.py` and then execute this script <br>
4. Obtain the desired data by executing the script `scraping.py`
5. Repeat steps 3-4 for any additional matches

## Requirements
* Python 3+
* MySQL 5.6+
