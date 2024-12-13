# Stock Market Data Pipeline

Ce projet met en place un pipeline de données pour le marché boursier en utilisant Kafka, Spark, Cassandra et Flask. Il génère des données boursières réalistes, les consomme via Spark, les stocke dans Cassandra et les expose via une API Flask.

## Structure du projet

## Prérequis

- Docker
- Docker Compose

## Installation

1. Clonez le dépôt :

   ```sh
   git clone https://github.com/baccouchee/dataengECE.git
   cd Pipeline
   ```

2. Lancez les services Docker :
   ```sh
   docker-compose up -d
   ```

## Services

### Kafka Producer

Le producteur Kafka génère des données boursières réalistes et les envoie à un topic Kafka.

- Fichier : [kafka_producer.py](kafka_producer.py)
- Commande pour lancer :
  ```sh
  python kafka_producer.py
  ```

### Spark Consumer

Le consommateur Spark lit les données du topic Kafka, les transforme et les stocke dans Cassandra.

- Fichier : [kafka_spark_consumer.py](kafka_spark_consumer.py)
- Commande pour lancer :
  ```sh
  python kafka_spark_consumer.py
  ```

### Flask API

L'API Flask expose les données boursières stockées dans Cassandra via une route `/api/stock_prices`.

- Fichier : [flask-api/app.py](flask-api/app.py)
- Commande pour lancer :
  ```sh
  docker-compose up -d flask-api
  ```

## Utilisation

1. Accédez à l'API Flask :

   ```sh
   http://localhost:5000/api/stock_prices
   ```

2. Visualisez les données dans Grafana :
   ```sh
   http://localhost:3000
   ```

## Configuration

### Docker Compose

Le fichier [docker-compose.yml](docker-compose.yml) configure les services suivants :

- Zookeeper
- Kafka
- Spark Master
- Spark Worker
- Cassandra
- Grafana
- Flask API

### Flask API

Le fichier [flask-api/Dockerfile](flask-api/Dockerfile) configure l'image Docker pour l'API Flask.

### Dépendances

Les dépendances Python sont listées dans [flask-api/requirements.txt](flask-api/requirements.txt).

## Contribution

Les contributions sont les bienvenues ! Veuillez soumettre une pull request ou ouvrir une issue pour discuter des changements que vous souhaitez apporter.

## Licence

Ce projet est sous licence MIT. Voir le fichier [LICENSE](LICENSE) pour plus de détails.
