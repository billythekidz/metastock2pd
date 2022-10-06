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
        # if not symbol in self.save: 
        self.save[symbol] = {}
        self.save[symbol]["lastPrice"] = -1
        self.save[symbol]["lastVolume"] = -1
        self.save[symbol]["lastTime"] = dt.datetime.now().time

    # def ReadLastDAT(self, filename):
    #     df = metastock.metastock_read_last(filename)
    #     print(df)

    def GetMyLastTick(self):
        if self.sourcesType == "DATATICK":
            return self.GetLastTick(self.symbolPath, self.symbol)
        elif self.sourcesType == "IFT":
            return self.GetLastTickIFT(self.symbolPath, self.symbol)
        return []

    def GetLastTick(self, path, symbol):
        df = metastock.metastock_read_last(path)  
        newTime = df['time']    
        newPrice = df['close']
        newPrice = round(newPrice,digits)
        newVolume = df['volume']
        # print ("newTime ", newTime)
        dateTime = dt.datetime.now()        
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
        self.save[symbol]["lastTime"] = dateTime
        self.save[symbol]["lastTick"] = [acsii_date, acsii_time, newPrice, tickvolume]
        self.save[symbol]["lastSaveTime"] = dateTime
        return self.save[symbol]["lastTick"]

    def GetLastTickIFT(self, path, symbol):
        df = metastock.metastock_read_last_ift(path)    
        timenow = dt.datetime.now()
        timenows = timenow.strftime('%d%m%Y')            
        acsii_date = df['date'].strftime('%m/%d/%Y')    
        acsii_time = df['time'].strftime('%I:%M:%S %p')       
        tickPrice = round(df['close'],digits)
        tickVolume = df['volume']  
        self.save[symbol]["lastSaveTime"] = df['time']
        self.save[symbol]["lastTick"] = [acsii_date, acsii_time, tickPrice, tickVolume]
        return self.save[symbol]["lastTick"]
    def GetLastSaveTime(self, symbol):
        if symbol in self.save and "lastSaveTime" in self.save[symbol]:
            return self.save[symbol]["lastSaveTime"] 
        return dt.datetime.fromordinal(1)
    def WriteFileLastTick(self, path, symbol):
        if symbol in self.save and "lastTick" in self.save[symbol]:
            with open(path.replace('/','').replace('^',''), 'a', newline='') as mcFile:
                mcWriter = csv.writer(mcFile, delimiter=',', quoting=csv.QUOTE_NONE, escapechar='\\', doublequote=False) 
                mcWriter.writerow(self.save[symbol]["lastTick"])

#   CONFIG HEADER
symbol = 'VN30F1M'
digits = 1
switch_data_sources_seconds = 10

dataTick =  MetastockDataSources('C:\DataTick\intraday', "DATATICK")
dataIFT =  MetastockDataSources('C:\\ami\MetaStock\Intraday\\futures', "IFT")
dataF = MetastockDataSources('C:\FData\MetaStock\Intraday\Phai sinh', 'FDATA')
# dataIFT = MetastockDataSources('C:\\ami\MetaStock\Intraday\\warrant', "IFT")
# dataIFT = MetastockDataSources('C:\\ami\MetaStock\EOD\other', "IFT")
dataTick.SetSymbol(symbol)# GetPath(dataTickPath, symbol)
dataIFT.SetSymbol(symbol)
dataF.SetSymbol(symbol)
# dataTick.ListingSymbols()
# dataIFT.ListingSymbols()
print(dataIFT.symbolPath)
print(dataTick.symbolPath)
print(dataF.symbolPath)
# iftPath = 'C:\\ami\MetaStock\EOD\other'#
# iftPath = 'C:\\ami\MetaStock\Intraday\\futures'

lastPrice = -1
lastVolume = -1
lastTime = dt.datetime.now().time
open_market_time = dt.datetime.strptime("09:00:00 AM", '%I:%M:%S %p')
pausing_market_time = dt.datetime.strptime("11:30:59 AM", '%I:%M:%S %p')
resume_market_time = dt.datetime.strptime("01:00:00 PM", '%I:%M:%S %p')
close_market_time = dt.datetime.strptime("02:30:59 PM", '%I:%M:%S %p')
last_tick_1 = []
last_tick_2 = []
last_tick_3 = []
for changes in watch(dataTick.symbolPath, dataIFT.symbolPath, dataF.symbolPath, step=1):
    # print('.', sep=' ', end='', flush=True)   
    print(changes)
    # continue
    for change in changes:
        # change = changes.pop()
        timenow = dt.datetime.now()
        if (timenow.time() < open_market_time.time() or timenow.time() > close_market_time.time()):        
            continue
        if change[1] == dataTick.symbolPath: 
            last_tick_1 = dataTick.GetMyLastTick()       
        if change[1] == dataIFT.symbolPath: 
            last_tick_2 = dataIFT.GetMyLastTick()
        if change[1] == dataF.symbolPath: 
            last_tick_3 = dataF.GetMyLastTick()
        deltaTime = timenow-(dataTick.GetLastSaveTime(symbol))        
        # if (deltaTime.total_seconds() >= switch_data_sources_seconds and dataTick.GetLastSaveTime(symbol) < dataIFT.GetLastSaveTime(symbol)):
        #     dataIFT.WriteFileLastTick('C:/AmiExportData/' + f'{symbol}_TEST.csv', symbol)
        # else:
        #     dataTick.WriteFileLastTick('C:/AmiExportData/' + f'{symbol}_TEST.csv', symbol)

        if change[1] == dataTick.symbolPath: 
            dataTick.WriteFileLastTick('C:/AmiExportData/SYMBOL/' + f'{symbol}_DT.csv', symbol)                   
            print("dataTick ", last_tick_1) 
        if change[1] == dataIFT.symbolPath: 
            dataIFT.WriteFileLastTick('C:/AmiExportData/SYMBOL/' + f'{symbol}_IFT.csv', symbol)
            print("dataIFT ", last_tick_2)
        if change[1] == dataF.symbolPath: 
            dataF.WriteFileLastTick('C:/AmiExportData/SYMBOL/' + f'{symbol}_F.csv', symbol)
            print("dataF ", last_tick_3)

        # with open('C:/AmiExportData/' + f'{symbol}_MC.csv'.replace('/','').replace('^',''), 'a', newline='') as mcFile:
        #     mcWriter = csv.writer(mcFile, delimiter=',', quoting=csv.QUOTE_NONE, escapechar='\\', doublequote=False) 
        #     mcWriter.writerow(mcRow)
        
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

# print("watch ok")
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


