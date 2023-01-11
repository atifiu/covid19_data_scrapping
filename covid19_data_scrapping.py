import os
import requests
from datetime import datetime
import pandas as pd
import csv
from bs4 import BeautifulSoup as soup
import psycopg2
from dotenv import load_dotenv
import pytz

print("Successfully imported necessary modules")

url = "https://www.worldometers.info/coronavirus/"
response = requests.get(url, allow_redirects = True)
response.status_code

soup_response = soup(response.text, "html.parser")
#soup_response
sections = soup_response.find_all("div", id= "maincounter-wrap")
for section in sections:
    #print(section)
    print("----------------")
    
cases = int(sections[0].find("span").text.strip().replace(",", ""))
deaths = int(sections[1].find("span").text.strip().replace(",", ""))
recoveries = int(sections[2].find("span").text.strip().replace(",", ""))

print(f"Total {(cases)= } Total {(deaths)= } and Total {(recoveries)= }")

matched_tags = soup_response.find_all(lambda tag: len(tag.find_all()) == 0 and "Last updated" in tag.text)
LastUpdated = matched_tags[0].text.strip("Last updated: ")

date = datetime.utcnow().strftime("%Y-%m-%d")
print(date)
write_columns = False
columns = ["Total_Cases", "Total_Deaths", "Total_Recoveries", "Last_Updated"]
values_obs = cases, deaths, recoveries, LastUpdated
outfile = r"./data/covid19_worldmeters_summary_stats-"+date+".csv"
if not os.path.exists(outfile):
    write_columns = True
with open(outfile, "a",newline = "") as f:
    writer = csv.writer(f)
    if write_columns:
        writer.writerow(columns)
    writer.writerow(values_obs)



unware_time = datetime.strptime(LastUpdated, "%B %d, %Y, %H:%M %Z")#"%Y-%m-%d %H:%M:%S")
print(unware_time)
max_timestamp= int(unware_time.strftime("%Y%m%d%H%M%S"))
aware_time = unware_time.replace(tzinfo=pytz.UTC)
type(aware_time)

if (os.environ.get("DB_TYPE") == None):
 
 from config.definitions import ROOT_DIR
 load_dotenv(os.path.join(ROOT_DIR, 'config', '.env'))

conn = psycopg2.connect(
    dbname=os.environ.get("POSTGRES_DB"),
    user=os.environ.get("POSTGRES_USER"),
    password=os.environ.get("POSTGRES_PASS"),
    host=os.environ.get("POSTGRES_HOST"),
    port=os.environ.get("POSTGRES_PORT")
)

sql_insert = """INSERT INTO covid_worldmeter_summary_stats(total_cases, total_deaths, total_recoveries, lastupdated, int_timestamp)
             VALUES(%s, %s, %s, %s, %s) ;"""
sql_select = """select count(1) from covid_worldmeter_summary_stats where int_timestamp >= %s"""

with conn.cursor() as cursor:
    cursor.execute(sql_select, (max_timestamp,))
    v_count = cursor.fetchone()
    if v_count[0] == 0:
        cursor.execute(sql_insert,(cases, deaths, recoveries, aware_time,max_timestamp,))
    conn.commit()