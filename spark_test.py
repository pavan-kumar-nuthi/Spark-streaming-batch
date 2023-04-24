import websocket
import json
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")


def on_open(ws):
    print("the socket is open")
    subscribe_message = {
        "type": "subscribe",
        "channels": [
            {"name": "ticker", "product_ids": ["BTC-USD", "ETH-USD", "LTC-USD"]}
        ],
    }
    ws.send(json.dumps(subscribe_message))


def on_message(ws, message):
    cur_data = json.loads(message)
    print(cur_data)
    if cur_data["type"] == "ticker":
        producer.send(
            cur_data["product_id"],
            value=json.dumps(
                {
                    "price": cur_data["price"],
                    "product_id": cur_data["product_id"],
                    "time": cur_data["time"],
                    "volume_24h": cur_data["volume_24h"],
                    "best_bid": cur_data["best_bid"],
                    "best_ask": cur_data["best_ask"],
                }
            ).encode("utf-8"),
        )


# url="wss://ws-feed-public.sandbox.exchange.coinbase.com"
url = "wss://ws-feed.pro.coinbase.com"
ws = websocket.WebSocketApp(url, on_open=on_open, on_message=on_message)


ws.run_forever()
