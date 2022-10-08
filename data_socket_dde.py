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
import tornado
# from gevent import pywsgi
# from geventwebsocket.handler import WebSocketHandler

# socket = socketio.Server(async_mode='gevent')
# app = socketio.WSGIApp(socket)
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
digits = 2

pathDAT = GetPath(symbol)
print(pathDAT)
lastPrice = -1
lastVolume = -1
lastTime = dt.datetime.now().time
open_market_time = dt.datetime.strptime("09:00:00 AM", '%I:%M:%S %p')
close_market_time = dt.datetime.strptime("02:30:59 PM", '%I:%M:%S %p')
cache_stamp = 0
dumpPrice = 1000.0

try:
    while True:    
        stamp = os.stat(pathDAT).st_mtime    
        # print(stamp)
        # if stamp != cache_stamp:        
        cache_stamp = stamp
        dateTime = dt.datetime.now()
        # if (dateTime.time() < open_market_time.time() or dateTime.time() > close_market_time.time()):        
        #     continue     
        df = metastock.metastock_read_last(pathDAT)  
        newTime = df['time']    
        newPrice = df['close']
        newVolume = df['volume']
        # if (newPrice == lastPrice and newVolume == lastVolume and newTime == lastTime): 
        #     print("===")
        #     continue
        tickvolume = newVolume - lastVolume
        if newVolume < lastVolume: tickvolume = newVolume

        acsii_date = dateTime.strftime('%m/%d/%Y')    
        acsii_time = dateTime.strftime('%I:%M:%S %p')      
        lastPrice = round(newPrice,digits)
        lastVolume = int(newVolume)
        lastTime = newTime

        dumpPrice +=0.1
        
        sio.emit(symbol, {'symbol': symbol, 'time': int(round(dateTime.timestamp())), 'price':dumpPrice, 'volume':lastVolume, 'digits':1})

        sleep(1)
except KeyboardInterrupt:
    pass
