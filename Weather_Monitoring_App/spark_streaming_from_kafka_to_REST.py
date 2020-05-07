import sys

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils,KafkaDStream
from uuid import uuid1
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json,Column

kafka_bootstrap_servers='localhost:9092'
kafka_topic_name ='exampletopic1'

if __name__ == '__main__':

    # conf = SparkConf()
    # conf.set('spark.executor.memory', '1g')
    # conf.set('spark.core.max', '2')
    # conf.setAppName('WeatherAPI')

    # sc = SparkContext('local[2]', conf=conf)  # 2 threads
    # ssc = StreamingContext(sc, 2)

    # broker, topic=sys.argv[1:]
    # kvs = KafkaUtils.createStream(ssc,  broker,  “raw - event - streaming - consumer”, \{topic: 1})
    sparkSes=SparkSession.builder.config("spark.local.dir", "..\\path\\temp\\").appName('WeatherAPI').master('local').getOrCreate()
    sparkSes.sparkContext.setLogLevel("Error")

    #stream from kafka
    weather_detail_df=sparkSes.readStream.format(source="kafka").option("kafka.bootstrap.servers",kafka_bootstrap_servers).option("subscribe", kafka_topic_name).option("startingOffsets", "latest").load()
    print("Printing schema of weather_detail_df")
    weather_detail_df.printSchema()

    weather_detail_df_1=weather_detail_df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")

    #define a schema for the transaction_detail_data

    detail_schema=[StructField("CityName",StringType()),StructField("Temperature",DoubleType()),StructField("Humidity",IntegerType()),StructField("CreationTime",StringType())]
    transaction_detail_schema = StructType(fields=detail_schema)

    weather_detail_df_2=weather_detail_df_1.select(from_json("value", schema=transaction_detail_schema).alias("weatherdetails"),"timestamp")
    print(weather_detail_df_2.printSchema())

    weather_detail_df_3=weather_detail_df_2.select("weatherdetails.*","timestamp")

    from pyspark.sql.types import DateType
    weather_detail_df_4=weather_detail_df_3.withColumn("CreationDate", weather_detail_df_3["CreationTime"].cast(DateType()))

    print("printing schema of weather_detail_df_4: ")
    print(weather_detail_df_4.printSchema())

    weather_detail_df_5=weather_detail_df_4.select("CityName","Temperature","Humidity","CreationTime","CreationDate")
    print("printing schema of weather_detail_df_5: ")
    print(weather_detail_df_5.printSchema())

    # final result in console
    weather_detail_write_stream=weather_detail_df_5.writeStream.trigger(processingTime='10 seconds').\
        outputMode("append").option("truncate","false").\
        format("console").start()

    #setting checkpoint location
    sparkSes.conf.set("spark.sql.streaming.checkpointLocation","..\\path")
    weather_detail_df_5.writeStream.format("csv").option("path","..\\path\\streams").start()
    print("Weather Monitoring Streaming with KAFKA")

    print("Test")

    import time
    time.sleep(500)