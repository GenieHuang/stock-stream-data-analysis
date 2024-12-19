# stock-stream-data-analysis
A real-time stock market data pipeline that processes and analyzes streaming stock data using modern data engineering tools and cloud infrastructure.

Alpaca API → Kafka → PySpark → AWS Glue → AWS Athena

```
stock-stream-data-analysis/
├── airflow/
│   └── dags/
│       └── kafka_streaming.py
├── pyspark_data_transformation/
│   ├── jar_files/
│   ├── requirements.txt
│   ├── spark_processing.py
│   └── stand_ups/
│       ├── Week 2.md
│       ├── Week 3.md
│       └── ... (Week 4-15.md)
├── stock_data_extraction/
│   ├── get_stock_data.py
│   └── stock_data_producer.py
├── docker-compose.yml
├── README.md
├── requirements.txt
└── stock-stream-analysis-key.pem
```

Prerequisites

Docker and Docker Compose
Python 3.8+
AWS Account with appropriate permissions
Alpaca API credentials
Apache Kafka
Apache Airflow
Pyspark
