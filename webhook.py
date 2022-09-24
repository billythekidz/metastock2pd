from threading import Thread
import requests
import asyncio
from flask import Flask, request, abort
from time import sleep
import datetime as dt

# Create Flask object called app.
app = Flask(__name__)
signalQueue = []

@app.route('/vn30/entrade', methods=['GET'])
def vn30():
    if request.method == 'GET':
        # Parse the string data from tradingview into a python dict
        args = request.args
        signalQueue.append(args)           
        return 'OK', 200
    else:
        abort(400)

if __name__ == '__main__':    
    Thread(target=lambda: app.run(port=1159,debug=True,use_reloader=False)).start()    

    WAIT_FOR = 5
    count_down = WAIT_FOR
    sum_trade = {}
    while 1:
        if count_down <= 0:            
            while len(signalQueue) > 0:
                args = signalQueue.pop()
                symbol = args.get('symbol', 'VN30F1M', str)
                side = args.get('side', 'NB')
                clientId = args.get('clientId', 2, int)
                quantity = args.get('quantity', 1, int)        
                sum_trade.setdefault(clientId, 0)    
                if side == 'NS': sum_trade[clientId] -= quantity
                else: sum_trade[clientId] += quantity
            # print(sum_trade)
            for client in sum_trade:                
                try:
                    symbol = 'VN30F1M'
                    side = 'NB'
                    quantity = sum_trade[clientId]
                    if quantity < 0: side = 'NS'                  
                    signalUrl = f"http://localhost:6868/api/trade?symbol={symbol}&side={side}&clientId={client}&quantity={abs(quantity)}" 
                    print(signalUrl)
                    response = requests.get(signalUrl)
                    print(response.text)            
                except:
                    print("FAIL", 200)        
                sum_trade.pop(client, None)
                count_down = WAIT_FOR
                break    
        sleep(0.01)
        count_down -= 0.01
