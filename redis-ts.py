#With RedisTimeseriesManager
#ZeroMQ
import random
import redis
import json

import time, datetime, random
from pytz import timezone
from kafka import KafkaConsumer

from redis_timeseries_manager import RedisTimeseriesManager

consumer = KafkaConsumer('events', bootstrap_servers="localhost:9092", auto_offset_reset='earliest')
r = redis.Redis(host="localhost")
settings = { 'host': 'localhost',
    'port': 6379,
    'db': 2,
    'password': None,
}


class MarketData(RedisTimeseriesManager):
    _name = 'markets'
    _lines = ['open', 'high', 'low', 'close', 'volume']
    _timeframes = {
        'raw': {'retention_secs': 60*60*24*4}, # retention 4 days
        '1m': {'retention_secs': 60*60*24*7, 'bucket_size_secs': 60}, # retention 7 day; timeframe 60 secs
        '1h': {'retention_secs': 60*60*24*30, 'bucket_size_secs': 60*60}, # retention 1 month; timeframe 3600 secs
        '1d': {'retention_secs': 60*60*24*365, 'bucket_size_secs': 60*60*24}, # retention 1 year; timeframe 86400 secs
    }

    #compaction rules
    def _create_rule(self, c1:str, c2:str, line:str, timeframe_name:str, timeframe_specs:str, source_key:str, dest_key:str):
        if line == 'open':
            aggregation_type = 'first'
        elif line == 'close':
            aggregation_type = 'last'
        elif line == 'high':
            aggregation_type = 'max'
        elif line == 'low':
            aggregation_type = 'min'
        elif line == 'volume':
            aggregation_type = 'sum'
        else:
            return
        bucket_size_secs = timeframe_specs['bucket_size_secs']
        self._set_rule(source_key, dest_key, aggregation_type, bucket_size_secs)
    
    @staticmethod
    def print_data(data):
        for ts, open, high, low, close, volume in data:
            print(f"{datetime.datetime.fromtimestamp(ts, tz=timezone('UTC')):%Y-%m-%d %H:%M:%S}, open: {open}, high: {high}, low: {low}, close: {close}, volume: {volume}")


md = MarketData(**settings)

if __name__ == "__main__":
    products = [f"product_{i}" for i in range(3001)]

    for data in consumer:
       try:
           start_time = time.time()
           product = data.key.decode('utf-8')
           timestamp, open_price, high_price, low_price, close_price, volume = data.value.decode('utf-8').split(',')
           md.insert(data=[timestamp, open_price, high_price, low_price, close_price, volume], c1="stock", c2=product, create_inplace=True)
           print(f'updated {len(products)} products with random ohlcv data in {round(time.time()- start_time, 3)}s & published')
           # time.sleep(0.1)
       except:
           print('Skipped because of exception')
