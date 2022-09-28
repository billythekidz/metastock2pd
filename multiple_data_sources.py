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

    
class MetastockDataSources:
    def __init__(self, path, type):
        self.path = path
        self.save = {}
        self.sourcesType = type

    def ListingSymbols(self):    
        res = metastock.metastock_read_master(self.path)
        dicts = (res.to_dict('records'))    
        for record in dicts:
            print(record['symbol']);
        return ""

    def GetSymbolPath(self, symbol):    
        res = metastock.metastock_read_master(self.path)
        dicts = (res.to_dict('records'))    
        for record in dicts:
            if record['symbol'] == symbol:        
                return record['filename']
        return ""

    def SetSymbol(self, symbol):
        self.symbol = symbol
        self.symbolPath = self.GetSymbolPath(symbol)

    # def ReadLastDAT(self, filename):
    #     df = metastock.metastock_read_last(filename)
    #     print(df)

    def GetMyLastTick(self):
        if self.sourcesType == "DATATICK":
            return self.GetLastTick(self.symbolPath, self.symbol)
        elif self.sourcesType == "IFT":
            return self.GetLastTick(self.symbolPath, self.symbol)
        return []

    def GetLastTick(self, path, symbol):
        df = metastock.metastock_read_last(path)  
        newTime = df['time']    
        newPrice = df['close']
        newPrice = round(newPrice,digits)
        newVolume = df['volume']
        # print ("newTime ", newTime)
        dateTime = dt.datetime.now()        
        if not symbol in self.save: 
            self.save[symbol] = {}
            self.save[symbol]["lastPrice"] = -1
            self.save[symbol]["lastVolume"] = -1
            self.save[symbol]["lastTime"] = dt.datetime.now().time
        lastPrice = self.save[symbol]["lastPrice"]
        lastVolume = self.save[symbol]["lastVolume"]
        lastTime  = self.save[symbol]["lastTime"]
        tickvolume = newVolume - lastVolume
        if newTime != lastTime: tickvolume = newVolume
        if tickvolume < 0: tickvolume = 1

        acsii_date = dateTime.strftime('%m/%d/%Y')    
        acsii_time = dateTime.strftime('%I:%M:%S %p')      
        self.save[symbol]["lastPrice"] = newPrice
        self.save[symbol]["lastVolume"] = newVolume
        self.save[symbol]["lastTime"] = newTime
        return [acsii_date, acsii_time, newPrice, tickvolume]

    def GetLastTickIFT(self, path, symbol):
        df = metastock.metastock_read_last(path)    
        timenow = dt.datetime.now()
        timenows = timenow.strftime('%d%m%Y')            
        acsii_date = df['date'].strftime('%m/%d/%Y')    
        acsii_time = df['time'].strftime('%I:%M:%S %p')       
        tickPrice = df['close']
        tickVolume = df['volume']  
        return [acsii_date, acsii_time, tickPrice, tickVolume]


#   CONFIG HEADER
digits = 1

dataTick =  MetastockDataSources('C:\DataTick\intraday', "DATATICK")
dataIFT =  MetastockDataSources('C:\DataTick\intraday', "IFT")
dataTick.SetSymbol('^GOLD')# GetPath(dataTickPath, symbol)
dataIFT.SetSymbol("^DOW30 FUTURE")
# dataTick.ListingSymbols()
# iftPath = 'C:\\ami\MetaStock\EOD\other'#
# iftPath = 'C:\\ami\MetaStock\Intraday\\futures'

lastPrice = -1
lastVolume = -1
lastTime = dt.datetime.now().time
open_market_time = dt.datetime.strptime("09:00:00 AM", '%I:%M:%S %p')
close_market_time = dt.datetime.strptime("02:30:59 PM", '%I:%M:%S %p')
for changes in watch(dataTick.symbolPath, dataIFT.symbolPath, step=1):
    # print('.', sep=' ', end='', flush=True)   
    # print(changes)
    if changes.pop()[1] == dataTick.symbolPath: 
        print("dataTick ", dataTick.GetMyLastTick()) 
    # if changes[1] == dataIFT.symbolPath: dataIFT.GetMyLastTick()
    # print("dataTick ", dataTick.GetMyLastTick()) 
    # print("dataIFT ", dataIFT.GetMyLastTick())
    dateTime = dt.datetime.now()
    if (dateTime.time() < open_market_time.time() or dateTime.time() > close_market_time.time()):        
        continue     
    # df = metastock.metastock_read_last(dataTickPathDAT)  
    # newTime = df['time']    
    # newPrice = df['close']
    # newVolume = df['volume']
    # if (newPrice == lastPrice and newVolume == lastVolume and newTime == lastTime): 
    #     print("===")
    #     continue
    # tickvolume = newVolume - lastVolume
    # if newTime != lastTime: tickvolume = newVolume
    # if tickvolume < 0: tickvolume = 1

    # acsii_date = dateTime.strftime('%m/%d/%Y')    
    # acsii_time = dateTime.strftime('%I:%M:%S %p')      
    # lastPrice = round(newPrice,digits)
    # lastVolume = newVolume
    # lastTime = newTime
    # mcRow = [acsii_date, acsii_time, lastPrice, tickvolume]
    # symbol = "test"
    # with open('C:/AmiExportData/' + f'{symbol}_MC.csv'.replace('/','').replace('^',''), 'a', newline='') as mcFile:
    #     mcWriter = csv.writer(mcFile, delimiter=',', quoting=csv.QUOTE_NONE, escapechar='\\', doublequote=False) 
    #     mcWriter.writerow(mcRow)
        
    # print(acsii_time + "  " + str(lastPrice) + "  " + str(lastVolume))  
    # print(lastPrice)
    # print(lastVolume)

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


