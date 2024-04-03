import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
from datetime import datetime


url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
db_name = 'World_Economies.db'
table_name = 'countries_by_gdp'
target_file = 'Countries_by_GDP.json'
log_file = 'etl_project_log.txt'


html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')





def extract():
    tables = data.find('table', {'class': 'wikitable'})
    rows = tables.find_all('tr')[3:]
    df = pd.DataFrame(columns=["Country", "GDP_USD_billion"])

    for row in rows:
        col = row.find_all('td')
        if col:
            country_name = col[0].find('a').text.strip()
            GDP_str = col[2].text.strip().replace(',', ''
                                                  )
            try:
                GDP_USD_billion = float(GDP_str)/1000 if GDP_str else np.nan
            except ValueError:
                GDP_USD_billion = np.nan

            formatted_result = "{:.2f}".format(GDP_USD_billion) if not pd.isna(GDP_USD_billion) else GDP_str
            data_dict = {
            "Country": country_name,
            "GDP_USD_billion" : formatted_result
        }
        df1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df,df1], ignore_index=True)

    return df

def load_data(target_file, extracted_data):
    conn = sqlite3.connect(db_name)
    extracted_data.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    extracted_data.to_json(target_file)


def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 


log_progress("ETL Job Started")

log_progress("Extract phase Started") 
extracted_data = extract() 

log_progress("Extract phase Ended") 

log_progress("Load phase Started") 
load_data(target_file,extracted_data) 