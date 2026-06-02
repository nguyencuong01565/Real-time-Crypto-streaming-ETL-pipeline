import json
import os
import time

import websocket
from dotenv import load_dotenv
from kafka import KafkaProducer
from loguru import logger


# 1. Load configuration: Khai báo config
load_dotenv()
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC = os.getenv("CRYPTO_TOPIC")
SYMBOLS = os.getenv("FINNHUB_SYMBOLS").split(",")


# 2. Initialize Kafka Producer: Khai báo producer (port + data)
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER], #Khai báo port
    value_serializer=lambda x: json.dumps(x).encode("utf-8"), #Khai báo data dạng bytes để truyền vào Kafka
)

#3 Websocket
def open_websocket(ws): #Kết nối websocket (sub 5 cặp crypto)
    logger.info("Connected to Finnhub WebSocket")

    for a in SYMBOLS:
        a = a.strip()
        ws.send(json.dumps({"type": "subscribe", "symbol": a})) 
        logger.info(f"Subscribed: {a}")
#Gửi thông tin cho Finnhub theo đúng cú pháp {"type": "subscribe", "symbol": symbol}
# Cần chuyển dict -> dạng string để phù hợp ws.send()

def message_received(ws, message):
    data = json.loads(message) #Chuyển string -> object(list/dict)

    if data.get("type") == "trade":
        for info in data.get("data", []): #an toàn hơn khi viết: ... in data.get("data")
            event ={
            "symbol" : info.get("s"),
            "price" : info.get("p"),
            "volume" : info.get("v"),
            "trade_time" : info.get("t"),
            "received at" : time.time()
            }

        producer.send(TOPIC, event)
        logger.info(f"Sent to Kafka: {event}")


def error(ws, error):
    logger.error(f"WebSocket error: {error}")


def close(ws, close_status_code, close_msg):
    logger.warning(f"WebSocket closed: {close_status_code} - {close_msg}")

#4 Run producer
def main():
    logger.info("Starting Crypto Producer...")

    ws = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}",
        on_open=open_websocket,
        on_message=message_received,
        on_error=error,
        on_close=close,
    )

    ws.run_forever()


if __name__ == "__main__":
    main()


    