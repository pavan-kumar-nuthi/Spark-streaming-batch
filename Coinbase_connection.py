import websocket
import json

def on_open(ws):
    print('the socket is open')
    subscribe_message={
        'type':'subscribe',
        'channels':[
            {
            'name':'ticker',
            'product_ids':['BTC-USD','ETH-USD']
            }
        ]
    }
    ws.send(json.dumps(subscribe_message))


def on_message(ws,message):
    cur_data=json.loads(message)
    # print(message)
    print(cur_data)

# url="wss://ws-feed-public.sandbox.exchange.coinbase.com"
url="wss://ws-feed.pro.coinbase.com"
ws=websocket.WebSocketApp(url,on_open=on_open,on_message=on_message)
ws.run_forever()