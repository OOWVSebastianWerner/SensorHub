#%%
from dash import Dash, html, dcc, callback, Output, Input, dash_table
import plotly.express as px
import pandas as pd
import geopandas as gpd
import requests
from dotenv import load_dotenv
import os
import json
#from frost import config
import dash_bootstrap_components as dbc

load_dotenv('..\\..\\.env')

#%%

all_locations = f'{os.getenv('baseURL')}Things?$expand=Locations($select=location)&$top=1000&$resultFormat=GeoJSON'

one_datastream = f'{os.getenv('baseURL')}Things(663)/Datastreams(1)/Observations'

with requests.session() as session:

    locations = session.get(all_locations).json()

    data_meas_json = session.get(one_datastream).json()

    while '@iot.nextLink' in data_meas_json.keys():
        print('present')
        data_meas_json = data_meas_json | session.get(data_meas_json['@iot.nextLink']).json()
        # data_meas_json.pop('@iot.nextLink')


with open(r'data\locations.geojson', 'w') as loc_geojson:
    loc_geojson.write(json.dumps(locations))

geo_df = gpd.read_file(r'data\locations.geojson')

#%%

fig = px.scatter_map(geo_df,
                        lat=geo_df.geometry.y,
                        lon=geo_df.geometry.x,
                        hover_name="name",
                        zoom=5,
                        height=800,
                        color=geo_df['properties/station_type'])

fig.update_traces(cluster=dict(enabled=True))

# fig2 = px.line(data_meas)

app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = [
    html.Div(children=[
    html.H1(children='SensorHub Dashboard', style={'textAlign':'center'}),
    # dcc.Dropdown(geo_df.name.unique(), 'Canada', id='dropdown-selection'),
    dcc.Graph(figure=fig),
    html.Div(children=
    dash_table.DataTable(data=geo_df[['name', 'description','properties/station_type']].to_dict('records'), page_size=20)
    )], id='main', className='container')
]

# @callback(
#     Output('graph-content', 'figure'),
#     Input('dropdown-selection', 'value')
# )
# def update_graph(value):
#     dff = df[df.country==value]
#     return px.line(dff, x='year', y='pop')

if __name__ == '__main__':
    app.run(debug=True)