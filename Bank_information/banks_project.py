import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
from datetime import datetime

log_file = 'code_log.txt'
csv_file_path = 'exchange_rate.csv'
target_file = 'Largest_banks_data.csv'
db_name = 'banks.db'
table_name = 'Largest_banks'
url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'

html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')


def extract():
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    df = pd.DataFrame(columns=["Name", "MC_USD_Billion"])

    for row in rows:
       col = row.find_all('td')
       if col:
           name = col[1].find_all('a')[1]
           market_cap = col[2].text.strip()
           
           data_dict = {
            "Name": name,
            "MC_USD_Billion" : market_cap
        }
           df1 = pd.DataFrame(data_dict, index=[0])
           df = pd.concat([df,df1], ignore_index=True)
    return df

extracted_data = extract()
print(extracted_data)

def transform_data(extracted_data, csv_file_path):
     exchange_rates = pd.read_csv(csv_file_path)
     eur_rate = exchange_rates.iat[0,1]
     gbp_rate = exchange_rates.iat[1,1]
     inr_rate = exchange_rates.iat[2,1]

     extracted_data['MC_USD_Billion'] = pd.to_numeric(extracted_data['MC_USD_Billion'], errors='coerce')
     extracted_data['MC_EUR_Billion'] = (extracted_data['MC_USD_Billion'] * eur_rate).round(2)
     extracted_data['MC_GBP_Billion'] = (extracted_data['MC_USD_Billion'] * gbp_rate).round(2)
     extracted_data['MC_INR_Billion'] = (extracted_data['MC_USD_Billion'] * inr_rate).round(2)
     transformed_data = extracted_data
     return transformed_data



def load_to_csv(target_file, transformed_data):
    transformed_data.to_csv(target_file)

def load_to_db(transformed_data):
    conn = sqlite3.connect(db_name)
    transformed_data.to_sql(table_name, conn, if_exists='replace', index=False)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Largest_banks")
    rows = cursor.fetchall()
    for row in rows:
        print(rows)
    cursor.close()
    conn.close()




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

log_progress("Transfrom phase Started") 
transformed_data = transform_data(extracted_data,csv_file_path)
print(transformed_data)
log_progress("Transform phase Ended") 


log_progress("Load phase Started") 
load_to_csv(target_file, transformed_data)
load_to_db(transformed_data)
log_progress("Load phase Ended") 