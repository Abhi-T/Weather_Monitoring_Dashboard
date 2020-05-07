import dash
import dash_html_components as dhtml
import dash_core_components as dcc
from dash.dependencies import Input,Output
import dash_table
import pandas as pd
import requests
import datetime as dt
import time
from random import random

#Creating Application using DASH
import plotly

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app=dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.scripts.config.serve_locally=True
current_refresh_time_temp=None

#for Data table
df=None
'''records_list=[]
refresh_time_1=time.strftime("%Y-%m-%d %H:%M:%S")
record_1= {"CityName": "Chennai", "Temperature" : 290, "Humidity": 56, "RefreshTime": refresh_time_1}
records_list.append(record_1)
record_2= {"CityName": "Bengaluru", "Temperature" : 312, "Humidity": 45, "RefreshTime": refresh_time_1}
records_list.append(record_2)
record_3= {"CityName": "Mumbai", "Temperature" : 322, "Humidity": 62, "RefreshTime": refresh_time_1}
records_list.append(record_3)
record_4= {"CityName": "New Delhi", "Temperature" : 285, "Humidity": 49, "RefreshTime": refresh_time_1}
record_5= {"CityName": "Kolkata", "Temperature" : 313, "Humidity": 44, "RefreshTime": refresh_time_1}
records_list.append(record_4)
records_list.append(record_5)

from datatime import datetime
refresh_time_2_temp=datetime.strftime(refresh_time_1, "%Y-%m-%d %H:%M:%S")
refresh_time_2=(refresh_time_2_temp + dt.timedelta(seconds=10)).strftime("%Y-%m-%d %H:%M:%S")
record_6= {"CityName": "Chennai", "Temperature" : 312, "Humidity": 66, "RefreshTime": refresh_time_2}
records_list.append(record_6)
record_7= {"CityName": "Bengaluru", "Temperature" : 314, "Humidity": 55, "RefreshTime": refresh_time_2}
records_list.append(record_7)
record_8= {"CityName": "Mumbai", "Temperature" : 332, "Humidity": 62, "RefreshTime": refresh_time_2}
records_list.append(record_8)
record_9= {"CityName": "New Delhi", "Temperature" : 324, "Humidity": 49, "RefreshTime": refresh_time_2}
records_list.append(record_9)
record_10= {"CityName": "Kolkata", "Temperature" : 315, "Humidity": 54, "RefreshTime": refresh_time_2}
records_list.append(record_10)
'''

api_endpoint='http://127.0.0.1:8090/wm/v1'
api_response=requests.get(api_endpoint)
print(api_response)
records_list=api_response.json()
df=pd.DataFrame(records_list, columns=["CityName", "Temperature","Humidity","CreationTime","CreationDate"])
df['index']=range(1,len(df)+1)
PAGE_SIZE=5

#assign HTML content to DASH Application Layout
app.layout=dhtml.Div(
    [
        dhtml.H2(
            children="SPARK Dashboard for Weather Monitoring",
            style={
                "textAlign":"center",
                "color":"#4285F4",
                'font-weight':'bold',
                'font-family':'Verdana'
            }),
        dhtml.Div(
            children="{100% Guaranteed Correct Weather Predictions}",
            style={
                "textAlign": "center",
                "color": "#0F9D58",
                'font-weight': 'bold',
                'fontSize': 16,
                'font-family': 'Verdana'
            }
        ),
        dhtml.Br(),
        dhtml.Div(
            id="current_refresh_time",
            children="Current_Refresh_Time",
            style={
                "textAlign": "center",
                "color": "black",
                'font-weight': 'bold',
                'fontSize': 10,
                'font-family': 'Verdana'
            }
        ),
        dhtml.Div([
            dhtml.Div([
                dcc.Graph(id='live_update_graph_bar')
            ],className="six columns"),
            dhtml.Div([
                dhtml.Br(),
                dhtml.Br(),
                dhtml.Br(),
                dhtml.Br(),
                dhtml.Br(),
                dhtml.Br(),
                dash_table.DataTable(
                    id='datatable-paging',
                    columns=[
                        {"name":i,"id":i} for i in sorted(["CityName", "Temperature","Humidity","CreationTime","CreationDate"])
                    ],
                    page_current=0,
                    page_size=PAGE_SIZE,
                    page_action='custom'
                )
            ],className="six columns"
            )
        ],className="row"),

    dcc.Interval(
        id="interval_component",
        interval=20000,
        n_intervals=0
    )
]
)

@app.callback(
    Output("current_refresh_time","children"),
    [Input("interval_component", "n_intervals")]
)

def update_layout(n):
    global current_refresh_time_temp
    current_refresh_time_temp=time.strftime("%Y-%m-%d %H:%M:%S")
    return "Current Refresh Time:{}".format(current_refresh_time_temp)


@app.callback(
    Output("live_update_graph_bar","figure"),
    [Input("interval_component", "n_intervals")]
)

def update_graph_bar(n):
    traces=list()
    bar_1=plotly.graph_objs.Bar(
        x=df['CityName'].head(5),
        y=df['Temperature'].head(5),
        name='Temperature'
    )
    traces.append(bar_1)
    bar_2 = plotly.graph_objs.Bar(
        x=df['CityName'].head(5),
        y=df['Humidity'].head(5),
        name='Humidity'
    )
    traces.append(bar_2)
    layout=plotly.graph_objs.Layout(
        barmode='group',xaxis_tickangle=-45, title_text="City's Temperature and Humidity",
        title_font=dict(
            family='Verdana',
            size=12,
            color="black"
        ),
    )
    return {'data':traces, 'layout':layout}

@app.callback(
    Output("datatable-paging","data"),
    [Input("datatable-paging", "page_current"),
     Input('datatable-paging','page_size'),
     Input('interval_component','n_intervals')])
def update_table(page_current,page_size, n):
    global df
    print("Before calling API call in update_table")
    api_endpoint='http://127.0.0.1:8090/wm/v1'
    api_response=requests.get(api_endpoint)
    records_list=api_response.json()
    df=pd.DataFrame(records_list, columns=["CityName", "Temperature","Humidity","CreationTime","CreationDate"])
    df['index']=range(1, len(df)+1)
    print("after calling API call in update_table")
    print(df.head(10))

    '''
    df.loc[(df['index]==1), 'RefreshTime'] = current_refresh_time_temp
    df.loc[(df['index]==2), 'RefreshTime'] = current_refresh_time_temp
    df.loc[(df['index]==3), 'RefreshTime'] = current_refresh_time_temp
    df.loc[(df['index]==4), 'RefreshTime'] = current_refresh_time_temp
    df.loc[(df['index]==5), 'RefreshTime'] = current_refresh_time_temp
    '''

    return df.iloc[page_current*page_size:(page_current+1) * page_size].to_dict('records')

if __name__ == '__main__':
    print("Starting Dashboard for Weather Monitoring...")

    app.run_server(debug=True)


