from datetime import datetime 
import pandas as pd
import bs4 
import requests
import numpy as np
import sqlite3


# Task 1 log_progress
def log_progress(message) 
    timestamp_format = '%Y-%h-%d-%H%M%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,a) as f 
        f.write(timestamp + ',' + message + 'n') 

#Task 2 Extraction of data
def extract(url, table_attribs)
    page = requests.get(url).text
    soup = bs4.BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = soup.find_all(tbody)
    # Find the 1st table in the webpage
    rows = tables[0].find_all(tr)
    for row in rows
        col = row.find_all(td)
        if len(col) != 0
            data_dict = {Name col[1].find_all(a)[1][title],
                         MC_USD_Billion float(col[2].contents[0][-1])}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    log_progress('Extract completed')
    return df

 # Task 3 Transformation of data
def transform(df, csv_path)
    exchange_rate = pd.read_csv(csv_path)
    exchange_rate = exchange_rate.set_index(Currency).to_dict()[Rate]
    df[MC_GBP_Billion] = [np.round(x  exchange_rate[GBP], 2) for x in df[MC_USD_Billion]]
    df[MC_EUR_Billion] = [np.round(x  exchange_rate[EUR], 2) for x in df[MC_USD_Billion]]
    df[MC_INR_Billion] = [np.round(x  exchange_rate[INR], 2) for x in df[MC_USD_Billion]]
    log_progress('Transform completed')
    return df
   
# Task 4 Loading to CSV
def load_to_csv(df, output_path)
    df.to_csv(output_path, index = False)
    log_progress('Load to csv completed')

# Task 5 Loading to Database
def load_to_db(df, db_name, table_name)
    sql_connection = sqlite3.connect(db_name)
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress('Load to SQL DB completed')

# Task 6 Function to Run queries on Database
def run_queries(query_statement, db_name)
    sql_connection = sqlite3.connect(db_name)
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    log_progress('query run '+ query_statement)

#Input variables
log_file = log_file.txt 
table_attribs =  [Name, MC_USD_Billion]
url = 'httpsweb.archive.orgweb20230908091635%20httpsen.wikipedia.orgwikiList_of_largest_banks'
csv_path = .exchange_rate.csv
output_path = .Largest_banks_data.csv
table_name = Largest_banks
db_name = Banks.db

# Execution
log_progress('Starting ETL process')
df = extract(url, table_attribs)
df = transform(df, csv_path)
load_to_csv(df, output_path)
load_to_db(df, db_name, table_name)
# Print the contents of the entire table
query_statement = fSELECT  from {table_name}
run_queries(query_statement, db_name)

# Print the average market capitalization of all the banks in Billion GBP
query_statement = fSELECT AVG(MC_GBP_Billion) FROM {table_name}
run_queries(query_statement, db_name)

# Print only the names of the top 5 banks
query_statement = fSELECT Name from {table_name} LIMIT 5
run_queries(query_statement, db_name)

