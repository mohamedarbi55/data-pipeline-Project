from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json, date_format

# Créer une session Spark avec les connecteurs nécessaires
spark = SparkSession.builder \
    .appName("StockMarketConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Configuration de la connexion Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock-market-data") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Définition du schéma des données boursières
schema = schema_of_json('''{
    "symbol": "",
    "open": 0.0,
    "high": 0.0,
    "low": 0.0,
    "close": 0.0,
    "volume": 0,
    "timestamp": ""
}''')

# Transformation des données Kafka
stock_data = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data"))

# Extraction et transformation des données boursières
stock_info = stock_data.select(
    col("data.symbol").alias("symbol"),
    col("data.open").alias("open_price"),
    col("data.high").alias("high_price"),
    col("data.low").alias("low_price"),
    col("data.close").alias("close_price"),
    col("data.volume").alias("volume"),
    col("data.timestamp").alias("timestamp")
)

# Ajout des transformations supplémentaires
transformed_stock_info = stock_info \
    .withColumn("formatted_time", date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# Écriture dans Cassandra
query = transformed_stock_info.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "stock_market") \
    .option("table", "stock_prices") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

# Attente de l'arrêt de la lecture
query.awaitTermination()