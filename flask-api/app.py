from flask import Flask, jsonify
from cassandra.cluster import Cluster
from datetime import datetime

app = Flask(__name__)

# Connect to Cassandra
cluster = Cluster(['cassandra'])
session = cluster.connect('stock_market')

@app.route('/api/stock_prices', methods=['GET'])
def get_stock_prices():
    rows = session.execute('SELECT * FROM stock_prices')
    result = []
    for row in rows:
        result.append({
            'symbol': row.symbol,
            'open_price': row.open_price,
            'high_price': row.high_price,
            'low_price': row.low_price,
            'close_price': row.close_price,
            'volume': row.volume,
            'timestamp': row.timestamp.isoformat()  # Ensure timestamp is in ISO 8601 format
        })
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)