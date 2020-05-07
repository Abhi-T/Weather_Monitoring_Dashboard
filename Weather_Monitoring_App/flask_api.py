from flask import Flask, request
from flask_restplus import Api, Resource, fields

import cherrypy
from cherrypy.process.plugins import Daemonizer
from paste.translogger import TransLogger

import time
import datetime as dt
from datetime import datetime
import glob
import shutil
import os

flask_app=Flask(__name__)
app=Api(app=flask_app, Version="1.0", title="weather monitor", description="APIs for weather monitor")

name_space=app.namespace("wm", description="Weather Monitoring")
model=app.model('WeatherDetailModel',{'CityName': fields.String(required=True, description="Name of the city", help="Cityname cannot be blank"),
                                      'Temperature': fields.String(required=True, description="City's temperature", help="City's temperature"),
                                      'Humidity': fields.String(required=True, description="City's Humidity", help="City's Humidity"),
                                      'CreationTime': fields.String(required=True, description="Records creation time", help="Records creation time"),
                                      'CreationDate': fields.String(required=True, description="Event received  time", help="Event received  time")})

weather_detail_pd_df=None
def get_weather_details():
    records_list=[]
    refresh_time_1=time.strftime("%Y-%m-%d %H:%M:%S")
    refresh_date_1 = time.strftime("%Y-%m-%d")
    record_1= {"CityName": "Chennai", "Temperature" : 290, "Humidity": 56,"CreationTime":refresh_time_1, "CreationDate": refresh_date_1}
    records_list.append(record_1)
    record_2= {"CityName": "Bengaluru", "Temperature" : 312, "Humidity": 45, "CreationTime":refresh_time_1, "CreationDate": refresh_date_1}
    records_list.append(record_2)
    record_3= {"CityName": "Mumbai", "Temperature" : 322, "Humidity": 62, "CreationTime":refresh_time_1, "CreationDate": refresh_date_1}
    records_list.append(record_3)
    record_4= {"CityName": "New Delhi", "Temperature" : 285, "Humidity": 49,"CreationTime":refresh_time_1, "CreationDate": refresh_date_1}
    records_list.append(record_4)
    record_5= {"CityName": "Kolkata", "Temperature" : 313, "Humidity": 44, "CreationTime":refresh_time_1, "CreationDate": refresh_date_1}
    records_list.append(record_5)


    refresh_time_2_temp=datetime.strptime(refresh_time_1, "%Y-%m-%d %H:%M:%S")
    refresh_time_2=(refresh_time_2_temp + dt.timedelta(seconds=10)).strftime("%Y-%m-%d %H:%M:%S")
    refresh_date_2 = (refresh_time_2_temp + dt.timedelta(seconds=10)).strftime("%Y-%m-%d")
    record_6= {"CityName": "Chennai", "Temperature" : 312, "Humidity": 66,"CreationTime":refresh_time_2, "CreationDate": refresh_date_2}
    records_list.append(record_6)
    record_7= {"CityName": "Bengaluru", "Temperature" : 314, "Humidity": 55,"CreationTime":refresh_time_2, "CreationDate": refresh_date_2}
    records_list.append(record_7)
    record_8= {"CityName": "Mumbai", "Temperature" : 332, "Humidity": 62, "CreationTime":refresh_time_2, "CreationDate": refresh_date_2}
    records_list.append(record_8)
    record_9= {"CityName": "New Delhi", "Temperature" : 324, "Humidity": 49,"CreationTime":refresh_time_2, "CreationDate": refresh_date_2}
    records_list.append(record_9)
    record_10= {"CityName": "Kolkata", "Temperature" : 315, "Humidity": 54, "CreationTime":refresh_time_2, "CreationDate": refresh_date_2}
    records_list.append(record_10)

    return records_list

@name_space.route("/v1")
class WeatherMonitoring(Resource):
    @app.doc(responses={200:"OK", 400:"Invalid Argument provided", 500:"Mapping key error occured"})
    def get(self):
        try:
            weather_detail_list=[]
            # for i in os.listdir(
            #         "..\\path\\streams\\*.json"):
            #     print(i)
            # print("here")
            '''
            #weather_detail_pd_df=None
            #return weather_detail_pd_df.to_json(orient="records")
            connection_obj=get_presto_connection()
            cursor_obj=connection_obj.curson()
            #cursor_obj.execute('select * from weather_detail_tbl order by creationTime desc limit 10')
            rows=cursor_obj.fetchall()
            
            print(type(rows))
            for row in rows:
            print(row)
            weather_detail={"CityName":row[0], "Temperature":row[1], "Humidity":row[2], "CreationTime":row[3],"Creationdate":row[4]}
            weather_detail_list.append(weather_detail)
            '''

            path1 = "..\\path\\streams\\*.csv"
            path2 = "..\\path\\backup"
            for fname in glob.glob(path1):
                with open(fname) as a:
                    s1 = a.readline()
                    list1 = list(s1.split(","))
                    if len(list1) > 1:
                        weather_detail = {"CityName": list1[0], "Temperature": list1[1], "Humidity": list1[2],
                                          "CreationTime": list1[3],
                                          "Creationdate": list1[4]}
                        # print(weather_detail)
                        weather_detail_list.append(weather_detail)
                        print(weather_detail_list)
                    filename = os.path.basename(fname)
                    print(filename)
                shutil.move(fname, os.path.join(path2, os.path.basename(fname))) # moving the file to backup location
            # weather_detail_list=get_weather_details()
            return weather_detail_list
        except KeyError as e:
            name_space.abort(500, e.__doc__, status="could not retrieve weather information", statusCode="500")
        except Exception as e:
            name_space.abort(400, e.__doc__, status="could not retrieve weather information", statusCode="400")

def run_server(flask_app):
    #enable WSGI logging via paste
    api_logged=TransLogger(flask_app)

    #Mount the WSgi callable object (app) on root dir
    cherrypy.tree.graft(api_logged,'/')

    #set the configuration of the webserver
    cherrypy.config.update({'engine.autoreload.on':True,'log.screen':True, 'server.socket_port':8199, 'server.socket_host':"localhost"})

    Daemonizer(cherrypy.engine).subscribe()
    #start the cherrypy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()

if __name__ == '__main__':
    print("starting Weather Monitoring API service Application...")
    flask_app.run(port=8090, debug=True)


# test=get_weather_details()
# print(test)