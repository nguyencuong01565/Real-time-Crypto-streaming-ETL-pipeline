# Real-time-Crypto-streaming-ETL-pipeline


1.Project Overview 
This project implements a real-time data pipeline for cryptocurrency market data. Live trade events are collected from the Finnhub WebSocket API, streamed through Kafka, and stored in PostgreSQL for further analysis.

2.Architecture

Finnhub WebSocket
        ↓
Kafka Producer
        ↓
Kafka Topic
        ↓
Kafka Consumer
        ↓
PostgreSQL

3.Technologies
- Python
- Apache Kafka
- Docker
- Docker Compose
- PostgreSQL
- WebSocket API
- 
4.Features
- Real-time cryptocurrency trade ingestion
- Kafka-based message streaming
- Automated data persistence
- Containerized deployment
- Support for multiple trading pairs
