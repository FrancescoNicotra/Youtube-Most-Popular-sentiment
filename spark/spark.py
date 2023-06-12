from __future__ import print_function
from pyspark.sql.functions import count, when, col, date_format, from_json
from elasticsearch import Elasticsearch
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.conf import SparkConf
import json
from pyspark import SparkContext
import pyspark.sql.types as tp
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.ml.classification import LogisticRegression

elastic_host = "http://elasticsearch:9200"
elastic_index = "videos"
kafkaServer = "kafkaServer:9092"
topic = "youtap"

es_mapping = {
    "mappings": {
        "properties": {
            "created_at": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss"},
            "comment": {"type": "text", "fielddata": True},
            "positive_percentage": {"type": "float"},
            "negative_percentage": {"type": "float"}
        }
    }
}

es = Elasticsearch(hosts=elastic_host, verify_certs=False)
response = es.indices.create(
    index=elastic_index,
    body=es_mapping,
    ignore=400
)

if 'acknowledged' in response:
    if response['acknowledged'] == True:
        print("INDEX MAPPING SUCCESS FOR INDEX:", response['index'])

# create traingin set
youtap = tp.StructType([
    tp.StructField(name='created_at',
                   dataType=tp.TimestampType(), nullable=True),
    tp.StructField(name='videoTitle', dataType=tp.StringType(), nullable=True),
    tp.StructField(name='comment', dataType=tp.StringType(), nullable=True),
    tp.StructField(name='videoId', dataType=tp.StringType(), nullable=True),
])

# create schema for training set
schema = tp.StructType([
    tp.StructField(name='id', dataType=tp.StringType(), nullable=True),
    tp.StructField(name='subjective',
                   dataType=tp.IntegerType(), nullable=True),
    tp.StructField(name='positive', dataType=tp.IntegerType(), nullable=True),
    tp.StructField(name='negative', dataType=tp.IntegerType(), nullable=True),
    tp.StructField(name='ironic', dataType=tp.IntegerType(), nullable=True),
    tp.StructField(name='lpositive', dataType=tp.IntegerType(), nullable=True),
    tp.StructField(name='lnegative', dataType=tp.IntegerType(), nullable=True),
    tp.StructField(name='top', dataType=tp.IntegerType(), nullable=True),
    tp.StructField(name='comment', dataType=tp.StringType(), nullable=True)
])


# create spark session
sparkConf = SparkConf()
sc = SparkContext(appName="TapSentiment", conf=sparkConf)
spark = SparkSession(sc)

sc.setLogLevel("ERROR")

print("Sto leggendo il traingin set...")
training_set = spark.read.csv(
    'training_set_sentipolc16.csv', schema=schema, header=True, sep=','
)
print("Finito.")

# create pipeline
tokenizer = Tokenizer(inputCol="comment", outputCol="words")
ita = StopWordsRemover.loadDefaultStopWords("italian")
stopWords = StopWordsRemover(
    inputCol='words', outputCol='filtered_words', stopWords=ita
)
hashtf = HashingTF(numFeatures=2**16,
                   inputCol="filtered_words", outputCol='tf')
idf = IDF(inputCol='tf', outputCol="features", minDocFreq=5)
model = LogisticRegression(featuresCol='features', labelCol='positive')
pipeline = Pipeline(stages=[tokenizer, stopWords, hashtf, idf, model])

print("Sto addestrando il modello...")
pipelineFit = pipeline.fit(training_set)
print("Ho finito di addestrare il modello.")

modelSummary = pipelineFit.stages[-1].summary
print("Accuratezza del modello:")
print(modelSummary.accuracy)

print("Sono pronto a leggere streaming da kafka...")
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", topic) \
    .load()

df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", youtap).alias("data")) \
    .select("data.*")

# Modifica il formato di created_at
df = df.withColumn("created_at", date_format(
    "created_at", "yyyy-MM-dd'T'HH:mm:ss"))

df = pipelineFit.transform(df) \
    .select("created_at", "videoTitle", "videoId", "comment", "prediction")


def calculate_percentages(df):
    positive_comments = df.groupBy("videoId").agg(
        count(when(col("prediction") == 1.0, True)).alias("positive_comments")
    )

    negative_comments = df.groupBy("videoId").agg(
        count(when(col("prediction") == 0.0, True)).alias("negative_comments")
    )

    df_with_percentages = df.join(positive_comments, "videoId").join(
        negative_comments, "videoId")

    df_with_percentages = df_with_percentages.withColumn(
        "percentuale_positivi", (col(
            "positive_comments") / (col("positive_comments") + col("negative_comments"))) * 100
    ).withColumn(
        "percentuale_negativi", (col(
            "negative_comments") / (col("positive_comments") + col("negative_comments"))) * 100
    )

    return df_with_percentages

# Send to Elasticsearch


def send_to_elasticsearch(batch_df, batch_id):
    batch_df = calculate_percentages(batch_df)
    batch_df = batch_df.withColumn("percentuale_positivi", col(
        "percentuale_positivi").cast("double"))
    batch_df = batch_df.withColumn("percentuale_negativi", col(
        "percentuale_negativi").cast("double"))

    json_records = batch_df.toJSON().collect()
    records = [json.loads(record) for record in json_records]

    for record in records:
        es.index(index=elastic_index, document=record)


# Write to Elasticsearch
df.writeStream \
    .foreachBatch(send_to_elasticsearch) \
    .start() \
    .awaitTermination()
