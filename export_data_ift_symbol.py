# from cgitb import reset
# from metastock2pd import metastock_read, metastock_read_master, metastock_master, metastock_emaster, metastock_xmaster, meta
from unicodedata import decimal
import metastock2pd as metastock
import os
# import matplotlib.pyplot as plt
from time import sleep
import csv
import datetime as dt
# from torch_sparse import dictable
#   CONFIG HEADER
symbol = 'VN30F1M'
digits = 1
todayOnly = True

open_market_time = dt.datetime.strptime("09:00:00 AM", '%I:%M:%S %p')
close_market_time = dt.datetime.strptime("02:30:59 PM", '%I:%M:%S %p')
def ExportDAT(symbol, filename, fromtime, totime):
    df = metastock.metastock_read_ift(filename)    
    timenow = dt.datetime.now()
    timenows = timenow.strftime('%d%m%Y') 
    # print(df)
    with open(pathExport + f'{symbol}_{timenows}.csv'.replace('/','').replace('^',''), 'w', newline='') as mcFile:
        for index, row in df.iterrows():            
            datetime = index  
            if (datetime.time() < open_market_time.time() or datetime.time() > close_market_time.time()): continue   
            if todayOnly and datetime.date() != timenow.date(): continue
            acsii_date = datetime.strftime('%m/%d/%Y')    
            acsii_time = datetime.strftime('%I:%M:%S %p')      
            price = round(row['close'], digits)
            volume = row['volume'] 
            mcRow = [acsii_date, acsii_time, price, volume]
            mcWriter = csv.writer(mcFile, delimiter=',', quoting=csv.QUOTE_NONE, escapechar='\\', doublequote=False) 
            mcWriter.writerow(mcRow)

            # print(index, price, volume)
        

currentDateString = dt.datetime.now().strftime('%d%m%Y')  
pathExport = f'C:/AmiExportData/SYMBOL/{currentDateString}/'
os.makedirs(pathExport, exist_ok = True)
for root, dirs, files in os.walk("C:\\ami\MetaStock\intraday"):
    for file in files:
        if file == ("MASTER"):
            fullpath = os.path.join(root, file)
            # print(root)
            res = metastock.metastock_read_master(root)
            dicts = (res.to_dict('records'))    
            for record in dicts:                                             
                if symbol == record['symbol']:
                    print (record)                       
                    fileurl = record['filename']
                    first_date = record['first_date'].strftime('%d%m%Y') 
                    last_date = record['last_date'].strftime('%d%m%Y') 
                    ExportDAT(symbol, fileurl, first_date, last_date)            
                    # df = metastock.metastock_read_ift(pathDAT)  
                    # print(root +  "_file)
                    break

