import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serializer to send data as JSON
)

# Function to generate realistic stock data
def generate_stock_data(symbol):
    current_time = datetime.now()
    
    # Define realistic ranges for each stock symbol
    stock_ranges = {
        'AAPL': {'low': 120, 'high': 150},
        'GOOGL': {'low': 2000, 'high': 2500},
        'MSFT': {'low': 250, 'high': 300},
        'AMZN': {'low': 3000, 'high': 3500},
        'TSLA': {'low': 600, 'high': 800},
        'NFLX': {'low': 500, 'high': 600},
        'FB': {'low': 250, 'high': 350}
    }
    
    if symbol in stock_ranges:
        low = stock_ranges[symbol]['low']
        high = stock_ranges[symbol]['high']
    else:
        low = 100
        high = 500
    
    stock_data = {
        'symbol': symbol,
        'open': round(random.uniform(low, high), 2),
        'high': round(random.uniform(low, high), 2),
        'low': round(random.uniform(low, high), 2),
        'close': round(random.uniform(low, high), 2),
        'volume': random.randint(1000, 10000),
        'timestamp': current_time.strftime('%Y-%m-%d %H:%M:%S')
    }
    return stock_data

# Example usage
if __name__ == "__main__":
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'NFLX', 'FB']  # List of stock symbols
    while True:
        for symbol in symbols:
            stock_data = generate_stock_data(symbol)
            producer.send('stock-market-data', stock_data)
            print(f"Sent data: {stock_data}")
        time.sleep(30)  # Sleep for 30 seconds