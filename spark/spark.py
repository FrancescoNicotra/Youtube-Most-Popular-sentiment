from __future__ import print_function
import sys
import json
from elasticsearch import Elasticsearch
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.dataframe import DataFrame
import time


def elaborate(batch_df: DataFrame, batch_id: int):
    batch_df.show(truncate=False)


# Funzione per inviare i dati a Elasticsearch
sc = SparkContext(appName="PythonStructuredStreamsKafka")
spark = SparkSession(sc)
print(spark.version)
sc.setLogLevel("WARN")

kafkaServer = "kafkaServer:9092"
topic = "youtap"


# Funzione per inviare i dati a Elasticsearch
def send_to_elasticsearch(batch_df: DataFrame, batch_id: int):
    es = Elasticsearch({'host': 'elasticsearch', 'port': 9200, 'use_ssl': False}, timeout=30,
                       max_retries=10, retry_on_timeout=True)  # Indirizzo IP o hostname di Elasticsearch
    if not es.ping():
        raise ValueError("Impossibile connettersi a Elasticsearch")

    # Conversione del DataFrame in una lista di dizionari
    records = batch_df.toJSON().map(json.loads).collect()

    # Invio dei dati a Elasticsearch
    for record in records:
        converted_dict = json.loads(record["value"])

        # Utilizza l'id come id del documento in Elasticsearch
        doc_id = int(str(time.time()).replace(".", ""))

        update_body = {
            "doc": converted_dict,
            "doc_as_upsert": True
        }

        es.update(index="videos", id=doc_id, body=update_body)
        # es.index(index="movies", id=doc_id, body=converted_dict)

# Streaming Query


# Streaming Query
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", topic) \
    .load()

df.selectExpr("CAST(timestamp AS STRING)", "CAST(value AS STRING)") \
  .writeStream \
  .foreachBatch(send_to_elasticsearch) \
  .start() \
  .awaitTermination()

# df.writeStream \
#  .format("console") \
#  .start() \
#  .awaitTermination()
'''
df.selectExpr("CAST(timestamp AS STRING)","CAST(value AS STRING)") \
  .writeStream \
  .foreachBatch(send_to_elasticsearch) \
  .start() \
  .awaitTermination()

Mirko, [15/05/2023 22:01]
# Funzione per inviare i dati a Elasticsearch
def send_to_elasticsearch(batch_df: DataFrame, batch_id: int):
    es = Elasticsearch({'host': 'elasticsearch', 'port': 9200, 'use_ssl': False}, timeout=30, max_retries=10, retry_on_timeout=True)  # Indirizzo IP o hostname di Elasticsearch
    if not es.ping():
        raise ValueError("Impossibile connettersi a Elasticsearch")

    # Conversione del DataFrame in una lista di dizionari
    records = batch_df.toJSON().map(json.loads).collect()

    # Invio dei dati a Elasticsearch
    for record in records:
        converted_dict = json.loads(record["value"])

        if "dailyData" not in converted_dict:
            converted_dict["dailyData"] = []

        converted_dict["dailyData"].append({
          "popularity": converted_dict["popularity"],
          "vote_average": converted_dict["vote_average"],
          "vote_count": converted_dict["vote_count"],
          "date": datetime.today().strftime('%Y-%m-%d')
         })

        converted_dict["dailyData"].append({
          "popularity": converted_dict["popularity"]+50,
          "vote_average": converted_dict["vote_average"]+100,
          "vote_count": converted_dict["vote_count"]+20,
          "date": (datetime.today()+timedelta(days=1)).strftime('%Y-%m-%d')
         })

        print(converted_dict)
        # print(type(converted_dict))


        doc_id = converted_dict["id"]  # Utilizza l'id come id del documento in Elasticsearch

        update_body = {
            "doc": converted_dict,
            "doc_as_upsert": True
        }

        es.update(index="movies", id=doc_id, body=update_body)
        # es.index(index="movies", id=doc_id, body=converted_dict)
'''
