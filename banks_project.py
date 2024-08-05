
# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests 
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime
from io import StringIO
import sqlite3 

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    
    with open('code_log.txt', 'a') as f:
      
        f.write(f'{datetime.now()} : {message}\n')

log_progress("Preliminaries complete. Initiating ETL process")

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    
    url="https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
    response= requests.get(url)
    soup=BeautifulSoup(response.text,"html.parser")
    
    table = soup.find_all('table')[0] 
    df_banks = pd.read_html(StringIO(str(table)))[0]

    df_banks = df_banks[['Bank name', 'Market cap (US$ billion)']].head(10)
    df_banks.columns = ['Name', 'MC_USD_Billion']
    
    log_progress("Data extraction complete. Initiating Transformation process")
    
    return df_banks

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ['Bank name', 'Market cap (US$ billion)']

df_banks1= extract(url, table_attribs)


def transform(df,csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    exchange_rate_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
    df_exchange = pd.read_csv(exchange_rate_url)
    
    # Convert to dictionary for easier access
    exchange_rates = dict(zip(df_exchange['Currency'], df_exchange['Rate']))

    df_banks1['MC_GBP_Billion'] = [np.round(x * exchange_rates['GBP'], 2) for x in df_banks1['MC_USD_Billion']]
    
    df_banks1['MC_EUR_Billion'] = [np.round(x * exchange_rates['EUR'], 2) for x in df_banks1['MC_USD_Billion']]
    
    df_banks1['MC_INR_Billion'] = [np.round(x * exchange_rates['INR'], 2) for x in df_banks1['MC_USD_Billion']]
    
    # Log the progress
    log_progress("Data transformation complete. Initiating Loading process")
    
    return df_banks1
    
exchange_rate_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
 
df_banks=transform(df_banks1,exchange_rate_url)

def load_to_csv(df_banks, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df_banks.to_csv(output_path,index=False)
    log_progress("Data saved to CSV file")
output_path="./Largest_banks_data.csv"
load_to_csv(df_banks,output_path)

def load_to_db(df_banks, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    # Connecting to SQLite database
    conn = sqlite3.connect('Banks.db')
    log_progress("SQL Connection initiated")
    # Storing data into the database
    df_banks.to_sql('Largest_banks', conn, if_exists='replace', index=False)
    log_progress("Data loaded to Database as a table, Executing queries")
    # Closing the connection
    conn.close()
    log_progress("Server Connection closed")
conn = sqlite3.connect('Banks.db')    
load_to_db(df_banks,conn,"Largest_banks")  
  
def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)
    
    # Print the query statement
    print("Executing query:")
    print(query_statement)
    
    # Fetch and print all results
    results = cursor.fetchall()
    for row in results:
        print(row)
    
    # Commit changes if necessary
    sql_connection.commit()
    
    # Log the progress
    log_progress("Executed query: " + query_statement)
    log_progress("Process Complete")

# Establishing connection to the SQLite database
conn = sqlite3.connect('Banks.db')

# Query 1: Print the contents of the entire table
query1 = 'SELECT * FROM Largest_banks'
run_query(query1, conn)

# Query 2: Print the average market capitalization of all the banks in Billion USD
query2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
run_query(query2, conn)

# Query 3: Print only the names of the top 5 banks
query3 = 'SELECT Name FROM Largest_banks LIMIT 5'
run_query(query3, conn)

# Close the connection after running all queries
conn.close()
