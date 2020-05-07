# Weather_Monitoring_Dashboard in windows 10
This is a weather monitoring dashboard built using DASH python library. Live data is fed using KAFKA, SPARK and FLASK

This is a good demo project for understanding how KAFKA and SparkStream works together to achieve business requirement

1> Start Zooker server. 
run only shown command from command prompt----zkServer.cmd
default port---clientPort=2181  (refer zoo_sample.conf file under conf folder of zooker)
 
2>Start KAFKA server/broker
run kafka-server-start.bat C:\Kafka\config\server.properties from command prompt.
make configuration changes if required in properties file. Please note that kakfa requires zookeeper to be running.
default port of kafka---9092 (update server.properties file if required)

3>correct all the paths in the folder such as backup and stream folder. This is also referred in flask code.

4>Create API key from http://api.openweathermap.org/data/2.5/weather and configure in KAFKA_producer_main file.

5>Run spark code with below command. This is more like running a deployment package.
C:\Users\pyspark\bin\spark-submit.cmd --master local --driver-memory 1g  --num-executors 2 --executor-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0  spark_streaming_from_kafka_to_REST.py

6>Run flask code and verify by hitting http://127.0.0.1:8090/ from browser

7>Run app code at the last. This is the dashboard code. I have also shared a sample dash file which is a standalone application and can be execute independently.
