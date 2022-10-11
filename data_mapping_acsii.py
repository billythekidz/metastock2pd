# from cgitb import reset
# from metastock2pd import metastock_read, metastock_read_master, metastock_master, metastock_emaster, metastock_xmaster, meta
from unicodedata import decimal
import metastock2pd as metastock
import os
# import matplotlib.pyplot as plt
from time import sleep
import csv
import datetime as dt
from watchfiles import awatch, watch
# from torch_sparse import dictable

def GetPath(SYMBOL):
    # path = os.path.join(os.getcwd(), 'data')
    # path = 'C:/ami/MetaStock/Intraday/futures/'
    # path = 'C:/ami/MetaStock/EOD/futures'
    # path = 'C:/Users/nguye/Documents/DataTick/intraday'
    path = 'C:\DataTick\intraday'
    # res = metastock_emaster(path)
    # res = metastock_xmaster(path)
    # master = metastock_master(path)
    res = metastock.metastock_read_master(path)
    # print(res)
    # print(xmaster)
    # print(master)
    # print(rmaster)
    dicts = (res.to_dict('records'))    
    for record in dicts:
        # print (record)
        if record['symbol'] == SYMBOL:        
            return record['filename']
    return ""

def readDAT(filename):
    # filename = os.path.join(os.getcwd(), 'data/F1.dat')
    df = metastock.metastock_read_last(filename)
    # assert len(df) == 62 and list(df.columns) == ['open', 'high', 'low', 'close', 'volume', 'oi']
    print(df)
    # plt.plot(df)
    # plt.ylabel('some numbers')
    # plt.show()

#   CONFIG HEADER
symbol = 'VN30F1M'
digits = 1

pathDAT = GetPath(symbol)
lastPrice = -1
lastVolume = -1
lastTime = dt.datetime.now().time
open_market_time = dt.datetime.strptime("09:00:00 AM", '%I:%M:%S %p')
close_market_time = dt.datetime.strptime("02:30:59 PM", '%I:%M:%S %p')
for changes in watch(pathDAT, step=1):
    # print(changes)    
    dateTime = dt.datetime.now()
    if (dateTime.time() < open_market_time.time() or dateTime.time() > close_market_time.time()):        
        continue     
    df = metastock.metastock_read_last(pathDAT)  
    newTime = df['time']    
    newPrice = df['close']
    newVolume = df['volume']
    if (newPrice == lastPrice and newVolume == lastVolume and newTime == lastTime): 
        print("===")
        continue
    tickvolume = newVolume - lastVolume
    if newVolume < lastVolume: tickvolume = newVolume

    acsii_date = dateTime.strftime('%m/%d/%Y')    
    acsii_time = dateTime.strftime('%I:%M:%S %p')      
    lastPrice = round(newPrice,digits)
    lastVolume = tickvolume
    lastTime = newTime
    mcRow = [acsii_date, acsii_time, lastPrice, tickvolume]
    with open('C:/AmiExportData/' + f'{symbol}_MC.csv'.replace('/','').replace('^',''), 'a', newline='') as mcFile:
        mcWriter = csv.writer(mcFile, delimiter=',', quoting=csv.QUOTE_NONE, escapechar='\\', doublequote=False) 
        mcWriter.writerow(mcRow)
        
    print(acsii_time + "  " + str(lastPrice) + "  " + str(tickvolume))  
    # print(lastPrice)
    # print(lastVolume)
    
import pyodbc 
import datetime as dt
from time import sleep
import csv
import os
#     # ... (event handling logic) ...
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost\SQLExpress;'
                      'Database=StockData;'
                      'Trusted_Connection=yes;')

timenow = dt.datetime.now()
timenows = timenow.strftime('%d%m%Y') 
currentDateString = dt.datetime.now().strftime('%d%m%Y')  
pathExport = f'C:/AmiExportData/SYMBOL/{currentDateString}/'
os.makedirs(pathExport, exist_ok = True)
def exportDataClient(record):
    print(record)
    with open(pathExport + f'{symbol}_{timenows}_ODBC.csv'.replace('/','').replace('^',''), 'a', newline='') as mcFile:
        # for index, row in df.iterrows():            
        datetime = record[1]  
        if (datetime.time() < open_market_time.time() or datetime.time() > close_market_time.time()): return   
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
lastDataClientTime = dt.datetime.now()
cache_stamp = 0
while True:
    stamp = os.stat(pathDAT).st_mtime    
    # print(stamp)
    if stamp != cache_stamp:        
        cache_stamp = stamp
        # file changed
        df = metastock.metastock_read_last(pathDAT)

    cursor = conn.cursor()
    #run query to pull newest row
    cursor.execute(queryLastRecord)
    lastRecord = cursor.fetchone()
    # print(results[1])
    if lastDataClientTime < lastRecord[1]:        
        lastDataClientTime = lastRecord[1]
        # print(lastRecord)
    exportDataClient(lastRecord)
    sleep(0.0001)

conn.close()

# 'Request' example added jjk  11/20/98
# import win32ui
# from pywin.mfc import object
# import dde
# # import asyncio


# class MultiChartTopic(object.Object):
#     def __init__(self, topicName):
#         self.topic = dde.CreateTopic(topicName)
#         object.Object.__init__(self, self.topic)
#         self.items = {}

#     # def Exec(self, cmd):
#     #     print("Other Topic asked to exec", cmd)
#     #     return (345)

#     def setData(self, itemName, value):
#         try:
#             self.items[itemName].SetData( str(value) )
#         except KeyError:
#             if itemName not in self.items:
#                 self.items[itemName] = dde.CreateStringItem(itemName)
#                 self.topic.AddItem( self.items[itemName] )
#                 self.items[itemName].SetData( str(value) )
    
#     # def Request(self, aString):
#     #     return (self.items[aString])

# server = dde.CreateServer()
# ddeLastPrice = MultiChartTopic('LAST')
# ddeVolume    = MultiChartTopic('VOLUME')
# server.AddTopic(ddeLastPrice)
# server.AddTopic(ddeVolume)
# server.AddTopic(MySystemTopic())
# server.AddTopic(MyOtherTopic("RunAnyCommand"))
# server.AddTopic(MyRequestTopic("ComputeStringLength"))
# server.Create("VNI4")
# symbol = 'EUR/USD'
# pathDAT = GetPath(symbol)
# pathDAT = 'C:/Users/nguye/Documents/DataTick/intraday/F3.DAT'
# print(pathDAT)
# async def getDataRealTime(symbol, path):
#     async for changes in awatch(path):
#         print(changes)

#         # file changed
#         df = metastock.metastock_read_last(path)
#         ddeLastPrice.setData('E', df['close'])       
#         ddeVolume.setData('E', df['volume']) 
#         print(df['close'])
#         print(df['volume'])
#         win32ui.PumpWaitingMessages(0, -1)
#         sleep(0.1)
# # getDataRealTime(symbol, pathDAT)
# asyncio.run(getDataRealTime(symbol, pathDAT))

# cache_stamp = 0
# while 1:    
#     stamp = os.stat(pathDAT).st_mtime    
#     # print(stamp)
#     if stamp != cache_stamp:        
#         cache_stamp = stamp
#         # file changed
#         df = metastock.metastock_read_last(pathDAT)
#         ddeLastPrice.setData("EURUSD", df['close'])       
#         ddeVolume.setData("EURUSD", df['volume']) 
#         print(df['close'])
#         print(df['volume'])
#     win32ui.PumpWaitingMessages(0, -1)
#     sleep(0.01)


