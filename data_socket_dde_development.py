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

def GetPath(SYMBOL):
    path = 'C:\DataTick\intraday'
    res = metastock.metastock_read_master(path)
    dicts = (res.to_dict('records'))    
    for record in dicts:
        # print (record)
        if record['symbol'] == SYMBOL:        
            return record['filename']
    return ""

#   CONFIG HEADER
symbol = 'USD/JPY'
digits = 2

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
lastTickTime = dt.datetime.now()
try:
    while True:    
        sleep(0.0001)
        stamp = os.stat(pathDAT).st_mtime    
        dateTime = dt.datetime.now()
        # print(stamp)
        # GET DATA TICK
        if stamp != cache_stamp:               
            # if (dateTime.time() < open_market_time.time() or dateTime.time() > close_market_time.time()): continue     
            cache_stamp = stamp 
            df = metastock.metastock_read_last(pathDAT)  
            newTime = df['time']    
            newPrice = df['close']
            newVolume = df['volume']
            tickvolume = newVolume - lastVolume
            if newVolume < lastVolume: tickvolume = newVolume
            acsii_date = dateTime.strftime('%m/%d/%Y')    
            acsii_time = dateTime.strftime('%I:%M:%S %p')      
            lastPrice = float(round(newPrice,digits))
            lastVolume = int(tickvolume)
            lastTime = newTime
        # GET DATA CLIENT
        cursor = conn.cursor()
        #run query to pull newest row
        cursor.execute(queryLastRecord)
        lastRecord = cursor.fetchone()
        # print(results[1])
        if lastTickTime < lastRecord[1]:
            lastTickTime = lastRecord[1]
            datetime = lastRecord[1]  
            # if (datetime.time() < open_market_time.time() or datetime.time() > close_market_time.time()): return   
            acsii_date = datetime.strftime('%m/%d/%Y')    
            acsii_time = datetime.strftime('%I:%M:%S %p')      
            price = round(lastRecord[2], digits)
            volume = int(lastRecord[3])
            # print(lastRecord)
    
        # dumpPrice +=0.1
        # symbol = 'VN30XX'

        sio.emit(symbol, {'symbol': symbol, 'time': int(round(dateTime.timestamp())), 'price':lastPrice, 'volume':lastVolume, 'digits':1})   

        print(acsii_time + "  " + str(lastPrice) + "  " + str(tickvolume))  
  
except KeyboardInterrupt:
    pass


while True:


conn.close()