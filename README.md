# Real-time-Crypto-streaming-ETL-pipeline


## 1.Project Overview 
This project implements a real-time data pipeline for cryptocurrency market data. Live trade events are collected from the Finnhub WebSocket API, streamed through Kafka, and stored in PostgreSQL for further analysis.
<img width="1198" height="229" alt="Screenshot 2026-06-19 at 16 16 01" src="https://github.com/user-attachments/assets/2ef07093-25b5-487b-907a-78935f0dfb7f" />


## 2.Architecture
```md
Finnhub WebSocket/API
        |
        v
WebSocket Client / Kafka Producer
        |
        v
Kafka Topic: market-trades / prices / candles
        |
        v
Kafka Consumer / Stream Processor
        |
        v
PostgreSQL
        |
        v
DBeaver / App / Dashboard
```

## 3.Technologies
- Python
- Apache Kafka
- Docker
- Docker Compose
- PostgreSQL
- WebSocket API
- 
## 4.Features
- Real-time cryptocurrency trade ingestion
- Kafka-based message streaming
- Automated data persistence
- Containerized deployment
- Support for multiple trading pairs


  
<img width="855" height="647" alt="Screenshot 2026-06-19 at 15 14 45" src="https://github.com/user-attachments/assets/caeb904a-f1f8-45b9-b7cc-7e19abbdf547" />
successful pipeline database imported
