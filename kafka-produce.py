#With RedisTimeseriesManager
#ZeroMQ
import random
import redis
import json
from kafka import KafkaProducer

import time, datetime, random
from pytz import timezone

producer = KafkaProducer(bootstrap_servers="localhost:9092")

def generate_ohlcv():
  open_price = random.uniform(10, 100)
  high_price = random.uniform(open_price, open_price * 1.1)
  low_price = random.uniform(open_price, open_price * 0.9)
  close_price = random.uniform(low_price, high_price)
  volume = random.randint(100, 10000)
  timestamp = int(time.time())
  return (timestamp, open_price, high_price, low_price, close_price, volume)


if __name__ == "__main__":
    products = [f"product_{i}" for i in range(3001)]

    while True:
      # Generate data for all products
      start_time = time.time()
      for product in products:
        timestamp, open_price, high_price, low_price, close_price, volume = generate_ohlcv()
        producer.send('live', key=product.encode('utf-8'), value=f'{timestamp},{open_price},{high_price},{low_price},{close_price},{volume}'.encode('utf-8'))
      print(f'produced {len(products)} products with random ohlcv data in {round(time.time()- start_time, 3)}s')
      time.sleep(0.1)


