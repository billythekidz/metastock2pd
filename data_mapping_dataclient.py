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

# connection_string = "DRIVER={SQL Server Native Client 11.0};SERVER=localhost\SQLExpress;DATABASE=StockData;Trusted_Connection=yes;"
# connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})

# engine = create_engine(connection_url)

# # produce our own MetaData object
# metadata = MetaData()

# # we can reflect it ourselves from a database, using options
# # such as 'only' to limit what tables we look at...
# metadata.reflect(engine, only=['SYMBOL', 'DATE', 'OPEN', 'VOLUME'])
# # we can then produce a set of mappings from this MetaData.
# Base = automap_base(metadata=metadata)

# # calling prepare() just sets up mapped classes and relationships.
# Base.prepare()

# # mapped classes are ready
# Date, Price, Volume = Base.classes.DATE, Base.classes.OPEN, Base.classes.VOLUME

# Base = automap_base()
# reflect the tables
# Base.prepare(autoload_with=engine)
# mapped classes are now created with names by default
# matching that of the table name.
# Datetime = Base.classes.DATE
# Price = Base.classes.OPEN
# Volume = Base.classes.VOLUME

# @event.listens_for(Date, 'after_insert')
# def receive_after_insert(mapper, connection, target):
#     # "listen for the 'after_insert' event"
#     print(mapper, "after insert ")
# @event.listens_for(SomeClass.some_attribute, 'append')
# def receive_append(target, value, initiator):
#     "listen for the 'append' event"

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
def export(record):
    print(record)
    with open(pathExport + f'{symbol}_{timenows}_ODBC.csv'.replace('/','').replace('^',''), 'a', newline='') as mcFile:
        # for index, row in df.iterrows():            
        datetime = record[1]  
        # if (datetime.time() < open_market_time.time() or datetime.time() > close_market_time.time()): return   
        acsii_date = datetime.strftime('%m/%d/%Y')    
        acsii_time = datetime.strftime('%I:%M:%S %p')      
        price = round(record[2], digits)
        volume = int(record[3])
        mcRow = [acsii_date, acsii_time, price, volume]
        mcWriter = csv.writer(mcFile, delimiter=',', quoting=csv.QUOTE_NONE, escapechar='\\', doublequote=False) 
        mcWriter.writerow(mcRow)

queryLastRecord = f'SELECT TOP 1 SYMBOL,DATE,[OPEN],VOLUME FROM VN_Intraday WHERE SYMBOL=\'{symbol}\' ORDER BY DATE DESC'
# df = pd.read_sql_query(f'SELECT * FROM VN_Intraday WHERE SYMBOL=\'{symbol}\'', conn)
# df = pd.read_sql_query(queryLastRecord, conn)
# df = pd.read_sql_query(f'SELECT SYMBOL,DATE,[OPEN],VOLUME FROM VN_Intraday WHERE SYMBOL=\'{symbol}\'', conn)
# df = df.set_index('DATE')
lastTickTime = dt.datetime.now()
while True:
    cursor = conn.cursor()
    #run query to pull newest row
    cursor.execute(queryLastRecord)
    lastRecord = cursor.fetchone()
    # print(results[1])
    if lastTickTime < lastRecord[1]:
        export(lastRecord)
        lastTickTime = lastRecord[1]
        # print(lastRecord)
    sleep(0.0001)

conn.close()
# print(df)


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