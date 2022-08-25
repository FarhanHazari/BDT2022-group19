# To run:
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 C:\Users\farha\Downloads\Big Data\BDT_FINAL\forecast_spark_stream_handler.py

from pymongo import MongoClient as Client
import json
from confluent_kafka import Producer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType, StructField, TimestampType
from functools import reduce
import pandas as pd
import pprint
import json
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

KAFKA_TOPIC = "fcz61y67-weather_data"
FORECAST_TOPIC = "fcz61y67-weather_forecast"
KAFKA_SERVER = "moped-01.srvs.cloudkafka.com:9094"
KAFKA_USERNAME = 'fcz61y67'
KAFKA_PASSWORD = '3yYb9hzK2cifpPr5fRANXY7ICM0vFBW5'
MONGODB_USERNAME = 'BDT_Group_19'
MONGODB_PASSWORD = 'u6LTD699lbAGSdyP'


class WeatherClassifier():
    data = None
    class_attr = None
    priori = {}
    cp = {}
    hypothesis = None

    def __init__(self, filename=None, class_attr=None):
        self.data = pd.read_csv(filename, sep=',', header=(0))
        self.class_attr = class_attr

    '''
        probability(class) =    How many  times it appears in cloumn
                             __________________________________________
                                  count of all class attribute
    '''

    def calculate_priori(self):
        class_values = list(set(self.data[self.class_attr]))
        class_data = list(self.data[self.class_attr])
        for i in class_values:
            self.priori[i] = class_data.count(i)/float(len(class_data))
        print("Result: ", self.priori)
        return self.priori

    '''
        Here we calculate the individual probabilites 
        P(outcome|evidence) =   P(Likelihood of Evidence) x Prior prob of outcome
                               ___________________________________________
                                                    P(Evidence)
    '''

    def get_cp(self, attr, attr_type, class_value):
        data_attr = list(self.data[attr])
        class_data = list(self.data[self.class_attr])
        total = 1
        for i in range(0, len(data_attr)):
            if class_data[i] == class_value and data_attr[i] == attr_type:
                total += 1
        return total/float(class_data.count(class_value))

    '''
        Here we calculate Likelihood of Evidence and multiple all individual probabilities with priori
        (Outcome|Multiple Evidence) = P(Evidence1|Outcome) x P(Evidence2|outcome) x ... x P(EvidenceN|outcome) x P(Outcome)
        scaled by P(Multiple Evidence)
    '''

    def calculate_conditional_probabilities(self, hypothesis):
        for i in self.priori:
            self.cp[i] = {}
            for j in hypothesis:
                self.cp[i].update(
                    {hypothesis[j]: self.get_cp(j, hypothesis[j], i)})
        # print("\nCalculated Conditional Probabilities: \n")
        # pprint.pprint(self.cp)

    def classify(self):
        forecast = []
        # print("Result: ")
        for i in self.cp:
            # print(i, " ==> ", reduce(lambda x, y: x*y,
            #       self.cp[i].values())*self.priori[i])
            forecast.append(reduce(lambda x, y: x*y, self.cp[i].values())*self.priori[i])
        return forecast


# define the spark session and add the required packages
spark = SparkSession.\
    builder.\
    config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3").\
    config("spark.mongodb.input.uri", "mongodb://localhost:127.0.0.1/BDT_rain_forecast").\
    config("spark.mongodb.input.readPreference.name", "secondaryPreferred").\
    config("spark.mongodb.output.uri", "mongodb://127.0.0.1/BDT_rain_forecast").\
    appName("streamingExampleWrite").\
    getOrCreate()

# set the level of "alert" to warning
spark.sparkContext.setLogLevel("WARN")

# define the schema
# save as json
schema = StructType([
    StructField("weather", StringType(), True),  # outlook field as string
    StructField("main", StringType(), True),  # temp field as string
    StructField("wind", StringType(), True),  # humidity field as string
    StructField("clouds", StringType(), True),  # windy field as string
    StructField("coord", StringType(), True),  # forecast field as string
    StructField("dt", StringType(), True),  # date field as TimestampType
    StructField("name", StringType(), True)  # date field as string
])

# define the spark dataframe
# read all the kafka streams of the topic "fcz61y67-weather_data"
df = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_SERVER)
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username='{}' password='{}';".format(KAFKA_USERNAME, KAFKA_PASSWORD))
      .option("kafka.ssl.endpoint.identification.algorithm", "https")
      .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
      .option("subscribe", KAFKA_TOPIC)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load().select(from_json(col("value").cast("string"), schema).alias("value")).select("value.*"))

df.printSchema()

# define a function that write the kafka streams to mongodb
def write_to_mongo(df, b):
    df.write \
            .format("mongo") \
            .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/BDT_rain_forecast") \
            .mode("append") \
            .save()


# connect to mongo and select database
client = Client("mongodb://127.0.0.1/")
db = client.BDT_rain_forecast

# kafka Producer configuration
conf = {'bootstrap.servers': 'moped-01.srvs.cloudkafka.com:9094',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'SCRAM-SHA-256',
        'sasl.username': KAFKA_USERNAME,
        'sasl.password': KAFKA_PASSWORD}
producer = Producer(**conf)

# define function to query forecasts collection and send message on kafka topic "fcz61y67-weather_forecast"


def get_forecast(df, b):
    c = WeatherClassifier(filename="dataset.csv", class_attr="Play")

    #print(df)

    batch = df.collect()  # read the batch
    # for each message in the batch
    for i in range(len(batch)):
        # "forecasts" collection in "BDT_rain_forecast" database
        #print("Printing batch: ")
        #print(batch[i])
        
        # weather, main, wind, clouds, coord, dt, name
        outlook = batch[i]['weather']
        outlook = json.loads(outlook)[0]['main']
        
        temp = batch[i]['main']
        temp = json.loads(temp)['temp']
        
        humidity = batch[i]['main']
        humidity = json.loads(humidity)['humidity']
        
        windy = batch[i]['wind']
        windy = json.loads(windy)['speed']
        
        date = batch[i]['dt']

        c.hypothesis = {"Outlook": outlook, "Temp": temp,
                  "Humidity": humidity, "Windy": windy}
        forep = c.calculate_priori()
        c.calculate_conditional_probabilities(c.hypothesis)

        # define message
        c.classify()
        msg = {'outlook': outlook, 'temp': temp, 'humidity': humidity, 'windy': windy, 'forecast': forep, 'date': date}

        # send message to "fcz61y67-weather_forecast" topic
        producer.produce(FORECAST_TOPIC, json.dumps(msg).encode('utf-8'))
        producer.flush()
        producer.poll(0)


# write the kafka streams to mongodb and send weather forecast
query = df\
    .writeStream \
    .foreachBatch(get_forecast)\
    .start()

# we can set a timeout in order to stop the streaming after some time -- query.awaitTermination(timeout=30)
query.awaitTermination()
query.stop()
