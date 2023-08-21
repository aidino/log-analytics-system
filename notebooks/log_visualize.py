from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
from datetime import datetime,timedelta
import time
import dash
import dash_core_components as dcc
import dash_html_components as html

cassandra_host = "192.168.31.188"

spark = SparkSession.builder.appName("pyspark-visualization").\
    config("spark.jars.packages","com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,com.datastax.spark:spark-cassandra-connector-driver_2.12:3.0.0").\
    config("spark.cassandra.connection.host",cassandra_host).\
    config("spark.cassandra.auth.username","cassandra").\
    config("spark.cassandra.auth.password","cassandra").\
    getOrCreate()

logs_df = spark\
             .read\
             .format("org.apache.spark.sql.cassandra")\
             .options(table="bgllogs", keyspace="loganalysis")\
             .load()

app = dash.Dash(__name__, suppress_callback_exceptions=True)

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
], style={'textAlign': 'center'})

#Color assignment
colors = {
    'background': 'white',#'#0C0F0A',
    'text': '#FFFFFF'
}

def create_header(title):
    header_style = {
        'background-color' : '#1B95E0',
        'padding' : '1.5rem',
        'color': 'white',
        'font-family': 'Verdana, Geneva, sans-serif'
    }
    header = html.Header(html.H1(children=title, style=header_style))
    return header

def generate_table(df, max_rows=10):
    table = html.Table(className="responsive-table",
                      children=[
                          html.Thead(
                              html.Tr(
                                  children=[html.Th(col.title()) for col in df.columns.values]
                                  
                                  ),style={'border':'1px black solid'}
                              ),
                          html.Tbody(
                              [
                              html.Tr(
                                  children=[html.Td(data) for data in d]
                                  )
                               for d in df.values.tolist()],style={'border':'1px black solid'})
                          ]
                       , style={'marginLeft': 'auto', 'marginRight': 'auto'}
    )
    
    return table

realtime_dashboard = html.Div(style={'backgroundColor': colors['background']}, children=
    [   
        html.Div([create_header('Log Analysis - Realtime Dashboard')]),
        html.Div([dcc.Graph(id='live-graph', animate=False)] ,style={'width': '100%', 'display': 'inline-block'}),
        html.Div([html.H2("Top Paths"), html.Div(id="top-paths-table")],style={'width': '50%', 'display': 'inline-block', 'border':'2px black solid'}),
        ##Intervals define the frequency in which the html element should be updated
        dcc.Interval(id='graph-update',interval=60*1000, n_intervals=0),
        html.Div(id='real-time-content'),
        html.Br(),
    ]
)

def read_cassandra(filter_condition,limit=False):
    logs_df = spark\
             .read\
             .format("org.apache.spark.sql.cassandra")\
             .options(table="bgllogs", keyspace="loganalysis")\
             .load()\
             .filter(filter_condition)
    if limit:
        return logs_df.limit(5).toPandas()
    else:
        return logs_df.toPandas()
    
@app.callback(Output('top-paths-table', 'children'),
              Input('graph-update', 'n_intervals')
             )
def update_top_urls(n_intervals):
    try:
        processed_time = 0
        filter_condition = "prediction == 'Abnormal' and time_added >'"+str(processed_time)+"'"
        df = read_cassandra(filter_condition)
        processed_time = time.time()-60

        df = df[['content','label', 'prediction']]

        return generate_table(df, max_rows=5)
    except Exception as e:
        #File to capture exceptions
        with open('table_errors.txt','a') as f:
            f.write(str(e))
            f.write('\n')



if __name__ == '__main__':
    app.run_server(debug=False, use_reloader=False, port=8050,host= '0.0.0.0')