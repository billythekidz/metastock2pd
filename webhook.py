from threading import Thread
import requests
import asyncio
from flask import Flask, request, abort
from time import sleep
import datetime as dt
import keyboard
import sys
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
    webhook = Thread(target=lambda: app.run(port=1159,debug=True,use_reloader=False))
    webhook.daemon = True
    webhook.start()    

try:

    # print("Press Shift + Q to exit")
    WAIT_FOR = 5
    count_down = WAIT_FOR
    sum_trade = {}
    total_position = 0
    accept_slippage = 0.5
    price = 0.0
    while 1:
        if count_down <= 0:            
            while len(signalQueue) > 0:
                args = signalQueue.pop()
                symbol = args.get('symbol', 'VN30F1M', str)
                side = args.get('side', 'NB')
                clientId = args.get('clientId', 2, int)
                quantity = args.get('quantity', 1, int)        
                price = round(args.get('price', 0, float),1)
                sum_trade.setdefault(clientId, 0)    
                if side == 'NS': sum_trade[clientId] -= quantity
                else: sum_trade[clientId] += quantity
                print(args)
            for client in sum_trade:                
                try:
                    symbol = 'VN30F1M'
                    side = 'NB'
                    orderPrice = price + accept_slippage                    
                    quantity = sum_trade[clientId]
                    if quantity < 0: 
                        side = 'NS'      
                        orderPrice = price - accept_slippage      
                    # signalUrl = f"http://localhost:6868/api/trade?symbol={symbol}&side={side}&clientId={client}&quantity={abs(quantity)}"           
                    # if (total_position >= 0 and quantity > 0) or (total_position <= 0 and quantity < 0):
                    signalUrl = f"http://localhost:6868/api/trade?symbol={symbol}&side={side}&clientId={client}&quantity={abs(quantity)}&price={abs(orderPrice)}"
                    # else:                                                                           
                    # total_position += quantity
                    print(signalUrl)
                    # print("total_position " + total_position)         
                    response = requests.get(signalUrl)                       
                except:
                    print("FAIL", 200)        
                sum_trade.pop(client, None)
                count_down = WAIT_FOR
                break    
        sleep(0.001)
        count_down -= 0.001
        # if keyboard.is_pressed("shift+q"):
        #     print("q pressed, ending loop")
        #     sys.exit()
except KeyboardInterrupt:
    pass
