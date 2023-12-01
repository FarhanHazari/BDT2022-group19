# BDT2022-group19
- Md Ashraful Alam Hazari


**Big Data Technologies Project 2022 - Group 19**

The project aims to implement a big data system that provides real-time rain prediction for any location in the world on an hourly basis over the following 5 days. A pipeline has been defined, starting from the data collection,  passing through real-time data ingestion and ending with a working demo that, using the map of Trento, plots real-time fake accidents taking into account their severity.


#
Technologies used: 
- Kafka
- Apache Spark
- MongoBD
- ReactJS
#
Web Hosts: 

- [Cloud Karafka](https://cloudkarafka.com)

#

**Prerequisites:**
- Python >= 3.7
- Spark >= 3.0.3
- MongoDB >= 5.0

#
**Dataset source**: 
[Open Weather](https://openweathermap.org/api)


#
**How to run the Project**
- To run the "kafka_weather_producer.py"
  - python3 kafka_weather_producer.py
- To run the "kafka_weather_consumer.py"
  - python3 kafka_weather_consumer.py
- To run the forecast_spark_stream_handler
  - spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /opt/apps/forecast_spark_stream_handler.py

