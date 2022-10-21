from sqlite3 import Time
import pandas as pd
import pyodbc 
import datetime as dt
from time import sleep
import csv
import os
from sqlalchemy.engine import URL
from sqlalchemy import Column, Integer, String, ForeignKey, create_engine, event, MetaData
# from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base

#     # ... (event handling logic) ...
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost\SQLExpress;'
                      'Database=StockData;'
                      'Trusted_Connection=yes;')

symbol='VN30F1M'
digits = 1
todayOnly = False
open_market_time = dt.datetime.strptime("09:00:00 AM", '%I:%M:%S %p')
close_market_time = dt.datetime.strptime("02:30:59 PM", '%I:%M:%S %p')
timenow = dt.datetime.now()
timenows = timenow.strftime('%d%m%Y') 
currentDateString = dt.datetime.now().strftime('%d%m%Y')  
pathExport = f'C:/AmiExportData/SYMBOL/{currentDateString}/'
os.makedirs(pathExport, exist_ok = True)


# queryLastRecord = f'SELECT TOP 1 SYMBOL,DATE,[OPEN],VOLUME FROM VN_Intraday WHERE SYMBOL=\'{symbol}\' ORDER BY DATE DESC'
# df = pd.read_sql_query(f'SELECT * FROM VN_Intraday WHERE SYMBOL=\'{symbol}\'', conn)
# df = pd.read_sql_query(queryLastRecord, conn)
df = pd.read_sql_query(f'SELECT SYMBOL,DATE,[OPEN],VOLUME FROM VN_Intraday WHERE SYMBOL=\'{symbol}\'  ORDER BY DATE ASC', conn)
# df = df.set_index('DATE')
with open(pathExport + f'{symbol}_{timenows}_ODBC.csv'.replace('/','').replace('^',''), 'w', newline='') as mcFile:
    for index, row in df.iterrows():            
        datetime = row['DATE']  
        if (datetime.time() < open_market_time.time() or datetime.time() > close_market_time.time()): continue   
        if todayOnly and datetime.date() != timenow.date(): continue
        acsii_date = datetime.strftime('%m/%d/%Y')    
        acsii_time = datetime.strftime('%I:%M:%S %p')      
        price = round(row['OPEN'], digits)
        volume = int(row['VOLUME'])
        mcRow = [acsii_date, acsii_time, price, volume]
        mcWriter = csv.writer(mcFile, delimiter=',', quoting=csv.QUOTE_NONE, escapechar='\\', doublequote=False) 
        mcWriter.writerow(mcRow)
# lastTickTime = dt.datetime.now()
# while True:
#     cursor = conn.cursor()
#     #run query to pull newest row
#     cursor.execute(queryLastRecord)
#     results = cursor.fetchone()
#     # print(results[1])
#     if lastTickTime < results[1]:
#         lastTickTime = results[1]
#         print(results)
#     sleep(0.0001)

conn.close()
print("Export Done!")


# con = sqlite3.connect('Driver={SQL Server};'
#                       'Server=localhost\SQLExpress;'
#                       'Database=StockData;'
#                       'Trusted_Connection=yes;')
# cur = con.cursor()
# def select_authorizer(*args):
#     print("Query args: " + str(args))
#     return sqlite3.SQLITE_OK
# con.set_authorizer(select_authorizer)

# # cur.execute("SELECT * FROM samples")
# df = pd.read_sql_query(f'SELECT SYMBOL,DATE,[OPEN],VOLUME FROM VN_Intraday WHERE SYMBOL=\'{symbol}\'', conn)
# df = df.set_index('DATE')
# print(df)