import time
import json
from kafka import KafkaProducer
import requests

kafka_bootstrap_servers='localhost:9092'
kafka_topic_name ='exampletopic1'

# producer=KafkaProducer(kafka_bootstrap_servers=kafka_bootstrap_servers, value_serializer= lambda v: json.dumps(v).encode('utf-8'))
# producer=KafkaProducer(security_protocol="SSL",bootstrap_servers=kafka_bootstrap_servers, value_serializer= lambda v: json.dumps(v).encode('utf-8'))
producer=KafkaProducer(bootstrap_servers=kafka_bootstrap_servers, value_serializer= lambda v: json.dumps(v).encode('utf-8'))

json_message=None
city_name=None
temperature=None
humidity=None
openweathermap_api_endpoint=None
appid=None

def get_weather_detail(openweathermap_api_endpoint):
    api_response=requests.get(openweathermap_api_endpoint)
    json_data=api_response.json()
    city_name=json_data["name"]
    humidity=json_data["main"]["humidity"]
    temperature=json_data["main"]["temp"]
    json_message={"CityName":city_name, "Temperature":temperature, "Humidity": humidity, "CreationTime":time.strftime("%Y-%m-%d %H:%M:%S")}
    return json_message

#below function will return out API key, this is a unique from weather API. pls set your key here
def get_appid(appid):
    return "your-API-KEY"

while True:
    city_name="Chennai"
    appid=get_appid(appid)
    openweathermap_api_endpoint= "http://api.openweathermap.org/data/2.5/weather?q="+city_name+"&appid="+appid
    print(openweathermap_api_endpoint)
    json_message=get_weather_detail(openweathermap_api_endpoint)
    producer.send(kafka_topic_name,json_message)
    print("Published message 1:"+json.dumps(json_message))
    print("Wait for 2 seconds....")
    time.sleep(2)

    city_name = "Bengaluru"
    appid = get_appid(appid)
    openweathermap_api_endpoint = "http://api.openweathermap.org/data/2.5/weather?q="+city_name+"&appid="+appid
    json_message = get_weather_detail(openweathermap_api_endpoint)
    producer.send(kafka_topic_name, json_message)
    print("Published message 2:" + json.dumps(json_message))
    print("Wait for 2 seconds....")
    time.sleep(2)

    city_name = "Mumbai"
    appid = get_appid(appid)
    openweathermap_api_endpoint = "http://api.openweathermap.org/data/2.5/weather?q="+city_name+"&appid="+appid
    json_message = get_weather_detail(openweathermap_api_endpoint)
    producer.send(kafka_topic_name, json_message)
    print("Published message 3:" + json.dumps(json_message))
    print("Wait for 2 seconds....")
    time.sleep(2)

    city_name = "New Delhi"
    appid = get_appid(appid)
    openweathermap_api_endpoint = "http://api.openweathermap.org/data/2.5/weather?q="+city_name+"&appid="+appid
    json_message = get_weather_detail(openweathermap_api_endpoint)
    producer.send(kafka_topic_name, json_message)
    print("Published message 4:" + json.dumps(json_message))
    print("Wait for 2 seconds....")
    time.sleep(2)

    city_name = "Kolkata"
    appid = get_appid(appid)
    openweathermap_api_endpoint = "http://api.openweathermap.org/data/2.5/weather?q="+city_name+"&appid="+appid
    json_message = get_weather_detail(openweathermap_api_endpoint)
    producer.send(kafka_topic_name, json_message)
    print("Published message 5:" + json.dumps(json_message))
    print("Wait for 2 seconds....")
    time.sleep(2)


