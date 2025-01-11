import pandas as pd
import requests
import sqlite3
from bs4 import BeautifulSoup
from io import StringIO
from datetime import datetime
from icecream import ic

#### Step 0: Maintaining a Log File

def Logs(Message):
    """This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing"""

    try:
        with open('./Log/code_log.txt' , 'a') as file:
            file.write(f'{datetime.now()} : {Message}\n')
    except Exception as e:
        print(e)

#### Step 1: Extract

def Extract(URL , Table_Attribute):
    """ This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. """
    try:
        Soup = BeautifulSoup(requests.get(URL).text , 'html.parser')
        Table = Soup.find('span' , string=Table_Attribute).find_next('table')
        Data = pd.read_html(StringIO(str(Table)))[0]

        Logs('Data extraction complete. Initiating Transformation process')
        return Data
    
    except Exception as e:
        print(e)
        Logs('Data extraction Incomplete.')

#### Step 2: Transform

def Transformation(Data , CSV_Path):
    """ This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies"""

    try:
        Exachange_rate = pd.read_csv(CSV_Path , index_col=0).to_dict()['Rate']

        Data['Market cap EUR billion'] = round(Data['Market cap (US$ billion)'] * Exachange_rate['EUR'],2)
        Data['Market cap GBP billion'] = round(Data['Market cap (US$ billion)'] * Exachange_rate['GBP'],2)
        Data['Market cap INR billion'] = round(Data['Market cap (US$ billion)'] * Exachange_rate['INR'],2)
        Data['Market cap PKR billion'] = round(Data['Market cap (US$ billion)'] * Exachange_rate['PKR'],2)

        Logs('Data transformation complete. Initiating Loading process')

        return Data

    except Exception as e:
        print(e)
        Logs('Data transformation Incomplete')

#### Step 3: Load (CSV)

def Load(Data , Output_Path):
    """ This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing."""
    try:
        Data.to_csv(Output_Path)

        Logs('Data saved to CSV file')

    except Exception as e:
        print(e)
        Logs('Data Not saved to CSV file')

###Loading data to SQL

def Load_TO_DB(Data , sql_connection , table_Name):
    """ This function saves the final data frame to a database
    table with the provided name. Function returns nothing."""

    try:
        Data.to_sql(table_Name , sql_connection  , index = False)

        Logs('Data loaded to Database as a table, Executing queries')

    except Exception as e:
        print(e)
        Logs('Data Not Loaded to Database')
    
#Run Query

def Run_Query(Query , sql_connection):
    try:
        cursor = sql_connection.cursor()
        cursor.execute(Query)
        Result = cursor.fetchall()       
        Logs('Process Complete')       
        return Result
    
    except Exception as e:
        print(e)
        Logs('Process Incomplete')

### Executing Pipeline

if __name__ == '__main__':
    URL = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
    Output_Path = './Output/Largest_Banks_Data.csv'
    DataBase = './Output/Banks.db'
    table_Name = 'Largest_Bank'
    Table_Attribute = 'By market capitalization'
    CSV_Path = './Input/exchange_rate.csv'

    try:
        Data = Extract(URL ,  Table_Attribute)

        Transformation(Data , CSV_Path)

        #Load To CSV
        Load(Data , Output_Path)

        #Load to DB
        with sqlite3.connect(DataBase) as sql_connection:
            Load_TO_DB(Data , sql_connection , table_Name)

            print(Run_Query('select * from Largest_Bank' , sql_connection))
    except Exception as e:
        print(e)

    
