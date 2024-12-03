'''
This script is used to get the stock data from the Alpaca API.
'''

import requests

from json import dumps
from confluent_kafka import Producer

response_dict = {"t": "timestamp", "o": "opening", "h": "high", "l": "low", "c": "closing", "v": "volume", "n": "trade_count", "vw": "vw_avg_price"}
class StockRealTimeData:
    def __init__(self, base_url: str, api_key: str, api_secret:str, symbol: str, start_time: str, end_time: str, partition_key: str, partition_idx: int, topic: str, producer):
    # api_key : str, template_url : str, producer, stock : str, topic_name : str, part_idx : int):
        self.base_url = base_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbol = symbol
        self.start_time = start_time
        self.end_time = end_time
        self.partition_key = partition_key
        self.partition_idx = partition_idx
        self.topic = topic
        self.producer = producer


    # request data from the API
    def get_stock_data(self, symbol, start_time, end_time):

        params = {
                "symbols": symbol,               # input
                "timeframe": "1Hour",              # fixed
                "start": start_time,      # input
                "end": end_time,        # input
                "adjustment": "raw",  # fixed
                "limit": "10000",   # fixed
                "feed": "iex",  # fixed
                "sort": "asc"  # fixed
            }

        url = self.base_url + "?" + "&".join([f"{k}={v}" for k, v in params.items()])

        # Request Headers
        headers = {
            "accept": "application/json",
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        return response.json()

    # kafka producer callback
    def callback(self, err, msg):
        if err is not None:
            print(f"Faild to deliver msg: {err}")
        else:
            print(f"Msg produced: {msg.topic()}[{msg.partition()}] @ {msg.offset()}")
    
    # Produce the stock data
    def produce_stock_data(self):
        try:
            stock_data = self.get_stock_data(self.symbol, self.start_time, self.end_time)
            for stock, records in stock_data["bars"].items():
                for record in records:
                    data_point = {response_dict[k]: record[k] for k in response_dict}
                    self.producer.produce(self.topic, value=dumps(data_point).encode('utf-8'), key=self.partition_key.encode('utf-8'), partition = self.partition_idx, callback = self.callback)
            self.producer.flush()
        except Exception as e:
            print(f"Error producing stock data: {e}")
        
        print("Stopping the producer")
        
