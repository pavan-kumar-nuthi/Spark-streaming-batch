import websocket
import json
from kafka import KafkaProducer 

producer = KafkaProducer(bootstrap_servers="localhost:9092")
""" spark = SparkSession.builder.appName("SimpleApp").getOrCreate() """


def on_open(ws):
    print("the socket is open")
    subscribe_message = {
        "type": "subscribe",
        "channels": [{"name": "ticker", "product_ids": ["BTC-USD", "ETH-USD"]}],
    }
    ws.send(json.dumps(subscribe_message))


def on_message(ws, message):
    cur_data = json.loads(message)
    if cur_data["type"] == "ticker":
        producer.send(cur_data["product_id"], value=json.dumps(message).encode("utf-8"))


# url="wss://ws-feed-public.sandbox.exchange.coinbase.com"
url = "wss://ws-feed.pro.coinbase.com"
ws = websocket.WebSocketApp(url, on_open=on_open, on_message=on_message)


ws.run_forever()
