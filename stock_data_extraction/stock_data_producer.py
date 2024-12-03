'''
This script is used to configure the producer.
'''

from json import dumps
import json
from datetime import datetime
import pytz
import socket

from dotenv import load_dotenv
import os

from confluent_kafka import Producer, admin

from get_stock_data import StockRealTimeData

load_dotenv()

EAST_TIMEZONE = pytz.timezone('US/Eastern')
UTC_TIMEZONE = pytz.timezone('UTC')

api_key = os.getenv("ALPACA_API_KEY")
api_secret = os.getenv("ALPACA_API_SECRET")

topic_prefix = os.getenv("KAFKA_TOPIC")

kafka_config={
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVER"),
}

base_url = "https://data.alpaca.markets/v2/stocks/bars"

symbol_list = ["AAPL", "MSFT", "GOOGL", "AMZN"]
today = datetime.today().strftime("%Y-%m-%d")

producer = Producer(kafka_config)


# check if the kafka server is running
def is_kafka_running(server):
    try:
        host, port = server.split(":")
        socket.create_connection((host, port), timeout=5)
        print("Kafka is running")
        return True
    except socket.error as e:
        print(f"Kafka is not running: {e}")
        return False


# get the regular market session time
def regular_market_session(current_date):
        current_date = datetime.strptime(current_date, "%Y-%m-%d")

        open_time = current_date.replace(hour = 9, minute=0, second = 0, microsecond = 0, tzinfo=EAST_TIMEZONE)
        close_time = current_date.replace(hour = 16, minute=0, second = 0, microsecond = 0, tzinfo=EAST_TIMEZONE)

        # Convert the session times to UTC
        utc_open_time = open_time.astimezone(UTC_TIMEZONE).strftime("%Y-%m-%dT%H:%M:%SZ")
        utc_close_time = close_time.astimezone(UTC_TIMEZONE).strftime("%Y-%m-%dT%H:%M:%SZ")

        return utc_open_time, utc_close_time


# create new kafka topics
def create_topic(topic, stocks_count):
    admin_client = admin.AdminClient(kafka_config)
    print(admin_client.list_topics().topics)

    if topic not in admin_client.list_topics().topics:
        new_topic = [admin.NewTopic(topic, num_partitions = stocks_count, replication_factor = 1)]

        fs = admin_client.create_topics(new_topic)

        for topic, f in fs.items():
            try:
                f.result()
                print(f"Topic {topic} created")
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")
            
    else:
        print(f"Topic {topic} is already created")


def send_stock_dataset():
    start_time, end_time = regular_market_session(today)
    topic = f"""{topic_prefix}{today}"""

    stocks_count = len(symbol_list)

    create_topic(topic, stocks_count)

    idx = 0

    # regular_market_session()
    for symbol in symbol_list:
        partition_key = symbol
        partition_idx = idx

        stock_real_time_data = StockRealTimeData(base_url, api_key, api_secret, symbol, '2024-11-19', end_time, partition_key, partition_idx, topic, producer)

        stock_real_time_data.produce_stock_data()
        idx += 1


if __name__ == "__main__":
    send_stock_dataset()




