import json
import os

import psycopg2
from dotenv import load_dotenv
from kafka import KafkaConsumer
from loguru import logger


# 1. Load configuration
load_dotenv()
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC = os.getenv("CRYPTO_TOPIC")
GROUP_ID_DB = os.getenv("KAFKA_GROUP_ID_DB")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")


# 2. Define Kafka Consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    group_id=GROUP_ID_DB,
    auto_offset_reset="earliest",
    enable_auto_commit=True, #Đọc tiếp bản ghi trong Kafka
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

# 3. Connect Database
def connect_postgres(): #Đưa thông số để kết nối đến Database
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

# 4. Parse json

# 5. Insert data into DB
def insert_trade(conn, event): #event = message.value ở Run ETL/ conn = connect_postgres()
    platform, currency = event.get("symbol").split(":", 1) #tách symbol = Binance + crypto

    query_raw = """
            INSERT INTO raw.crypto_streaming (raw_data)
            VALUES (%s)
            """
    value_raw = [json.dumps(event)]

    query_stagging = """
            INSERT INTO stagging.crypto_streaming
                (platform, currency, price, volume, trade_time)
            VALUES (%s, %s, %s, %s, %s)
            """
    value_stagging = [
        platform,
        currency,
        event.get("price"),
        event.get("volume"),
        event.get("trade_time")
    ]
    with conn.cursor() as cursor:
        cursor.execute(query_raw, value_raw)
        cursor.execute(query_stagging, value_stagging)

    conn.commit()

# 6. Run ETL
def main():
    logger.info("Starting Crypto DB Consumer...")
    logger.info(f"Reading Kafka topic: {TOPIC}")

    conn = connect_postgres()
    logger.info("Connected to Postgres")

    for Kafka_message in consumer:
        event = Kafka_message.value #message là thư viện websocket tự dựng = data trả về
        insert_trade(conn, event)
        logger.info(f"Inserted trade: {event}")


if __name__ == "__main__":
    main()