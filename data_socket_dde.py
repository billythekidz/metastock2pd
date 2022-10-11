# from cgitb import reset
# from metastock2pd import metastock_read, metastock_read_master, metastock_master, metastock_emaster, metastock_xmaster, meta
from unicodedata import decimal
import metastock2pd as metastock
import os
from time import sleep
import csv
import datetime as dt
from watchfiles import awatch, watch
import time
import asyncio
import socketio
from threading import Thread
from flask import Flask
import pyodbc 
import datetime as dt
from time import sleep
import csv
import os

portSocket = 11003
sio = socketio.Server(async_mode='threading')
app = Flask(__name__)
app.wsgi_app = socketio.WSGIApp(sio, app.wsgi_app)
# sio = socketio.AsyncServer(async_mode='tornado')
# app = tornado.web.Application(
#     [
#         (r"/socket.io/", socketio.get_tornado_handler(sio)),
#     ],
#     # ... other application options
# )
# app.listen(port)


@sio.event
def connect(sid, environ, auth):
    print('socketio connect ', sid)

@sio.event
def disconnect(sid):
    print('socketio disconnect ', sid)
# webhook = Thread(target=lambda: pywsgi.WSGIServer(('', 11003), app, handler_class=WebSocketHandler).serve_forever())
webhook = Thread(target=lambda: app.run(port=portSocket,debug=True,use_reloader=False))
# webhook = Thread(target=lambda: tornado.ioloop.IOLoop.current().start())
webhook.daemon = True
webhook.start() 
# 
#    
symbol = 'VN30F1M'
digits = 2

def GetPath(SYMBOL):
    path = 'C:\DataTick\intraday'
    res = metastock.metastock_read_master(path)
    dicts = (res.to_dict('records'))    
    for record in dicts:
        # print (record)
        if record['symbol'] == SYMBOL:        
            return record['filename']
    return ""
currentDateString = dt.datetime.now().strftime('%d%m%Y')  
pathExport = f'C:/AmiExportData/SYMBOL/'
os.makedirs(pathExport, exist_ok = True)
def export(symbol, record):
    # print(record)
    timenow = dt.datetime.now()
    timenows = timenow.strftime('%d%m%Y') 
    with open(pathExport + f'{symbol}_{timenows}_MD.csv'.replace('/','').replace('^',''), 'a', newline='') as mcFile:
        # for index, row in df.iterrows():            
        datetime = record[0]          
        acsii_date = datetime.strftime('%m/%d/%Y')    
        acsii_time = datetime.strftime('%I:%M:%S %p')      
        price = round(record[1], digits)
        volume = int(record[2])
        mcRow = [acsii_date, acsii_time, price, volume]
        mcWriter = csv.writer(mcFile, delimiter=',', quoting=csv.QUOTE_NONE, escapechar='\\', doublequote=False) 
        mcWriter.writerow(mcRow)

#   CONFIG HEADER


#     # ... (event handling logic) ...
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost\SQLExpress;'
                      'Database=StockData;'
                      'Trusted_Connection=yes;')

queryLastRecord = f'SELECT TOP 1 SYMBOL,DATE,[OPEN],VOLUME FROM VN_Intraday WHERE SYMBOL=\'{symbol}\' ORDER BY DATE DESC'

pathDAT = GetPath(symbol)
print(pathDAT)
lastPrice = -1
lastVolume = -1
lastTime = dt.datetime.now().time
open_market_time = dt.datetime.strptime("09:00:00 AM", '%I:%M:%S %p')
close_market_time = dt.datetime.strptime("02:30:59 PM", '%I:%M:%S %p')
cache_stamp = 0
dumpPrice = 1000.0
# lastDataClientTime = dt.datetime.now()
lastTickRecord = []
try:
    while True:    
        sleep(0.0001)
        stamp = os.stat(pathDAT).st_mtime    
        dateTime = dt.datetime.now()             
        isNewTick = False   
        isInTradingTime = dateTime.time() >= open_market_time.time() and dateTime.time() <= close_market_time.time()
        # isInTradingTime = True
        # print(stamp)
        # GET DATA TICK
        if stamp != cache_stamp and isInTradingTime:      
            cache_stamp = stamp 
            df = metastock.metastock_read_last(pathDAT)  
            newTime = df['time']    
            newPrice = df['close']
            newVolume = df['volume']
            tickvolume = newVolume - lastVolume
            if newVolume < lastVolume: tickvolume = newVolume
            tickvolume = int(tickvolume)
            acsii_date = dateTime.strftime('%m/%d/%Y')    
            acsii_time = dateTime.strftime('%I:%M:%S %p')      
            lastPrice = float(round(newPrice,digits))
            lastVolume = newVolume
            lastTime = newTime
            lastTickRecord = [dateTime, lastPrice, tickvolume]     
            isNewTick = True                
            print("DT: " + str(lastTickRecord[0]) + "  " + str(lastTickRecord[1]) + "  " + str(lastTickRecord[2])) 
        # GET DATA CLIENT
        cursor = conn.cursor()
        #run query to pull newest row
        cursor.execute(queryLastRecord)
        lastRecord = cursor.fetchone()          
        if lastRecord and (len(lastTickRecord) <= 0 or lastTickRecord[0] < lastRecord[1]) and isInTradingTime:
            price = round(lastRecord[2], digits)
            volume = int(lastRecord[3])
            lastTickRecord = [lastRecord[1], price, volume]            
            isNewTick = True
            print("DC: " + str(lastRecord[1]) + " " + str(lastRecord[2]) + " " + str(lastRecord[3]))
        if not isNewTick: continue        
        # dumpPrice +=0.1
        # symbol = 'VN30XX'

        sio.emit(symbol, {'symbol': symbol, 'time': int(round(lastTickRecord[0].timestamp())), 'price':lastTickRecord[1], 'volume':lastTickRecord[2], 'digits':1})   

        print("=> " + str(lastTickRecord[0]) + "  " + str(lastTickRecord[1]) + "  " + str(lastTickRecord[2])) 
        # export(symbol, lastTickRecord)
        # print(lastTickRecord) 
  
except KeyboardInterrupt:
    pass

# conn.close()