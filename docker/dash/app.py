#%%
from dash import Dash, dcc, html, Input, Output, callback
from dash.dependencies import Output,Input
from dash import dash_table
import plotly.express as px
import pandas as pd
import geopandas as gpd
import requests
#from dotenv import load_dotenv
import os
import json
#from frost import config
import dash_bootstrap_components as dbc

#load_dotenv('..\\..\\.env')

#%%
frost_URL = os.getenv('URL')
frost_port = os.getenv('PORT')
frost_api = f'{frost_URL}:{frost_port}/FROST-Server/v1.1/'

all_things = f'{frost_api}Things?$select=@iot.id,name,properties'
all_locations = f'{frost_api}Things?$select=@iot.id,name,properties/station_type&$expand=Locations($select=location)&$top=10000&$resultFormat=GeoJSON'
all_datastreams = f'{frost_api}Datastreams?$select=@iot.id,name,unitOfMeasurement'

one_datastream = f'{frost_api}Things(1286)?$select=@iot.id,name&$expand=Datastreams($expand=Observations($select=phenomenonTime,result))&$select=@iot.id' 

def get_data(session, url):
    """Get data from FROST-Server."""
    
    data = session.get(url).json()
    
    while '@iot.nextLink' in data.keys():
        
        nextLink = data['@iot.nextLink']
        next_data = session.get(nextLink).json()

        data['value'] += next_data['value']

        if '@iot.nextLink' in next_data.keys():
            data['@iot.nextLink'] = next_data['@iot.nextLink']
        else:
            data.pop('@iot.nextLink')
    
    return data

# Get data
with requests.session() as session:

    locations = session.get(all_locations).json()
    
    things = get_data(session, all_things)

    data_meas_json = get_data(session, one_datastream)


with open(r'locations.geojson', 'w') as loc_geojson:
    loc_geojson.write(json.dumps(locations))

geo_df = gpd.read_file(r'locations.geojson')

df_meas = pd.DataFrame(data_meas_json['Datastreams'][0]['Observations'])
#%%

fig = px.scatter_map(geo_df,
                        lat=geo_df.geometry.y,
                        lon=geo_df.geometry.x,
                        hover_name="name",
                        zoom=5,
                        height=800,
                        color=geo_df['properties/station_type'],
                        custom_data=["@iot.id","name"]
)

for trace in fig.data:
    if trace.name == 'water_station':
        trace.name = f"Water station"
    elif trace.name == 'groundwater_station':
        trace.name = f"Groundwater station"
    elif trace.name == 'raingauge_station':
        trace.name = f"Raingauge station"

fig.update_traces(cluster=dict(enabled=True))
fig.update_layout(legend_title_text='Station type')

fig2 = px.line(df_meas, 'phenomenonTime','result')

#%%
app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = [
    dcc.Location(id='url'),
    html.Div(id='container', children=[
        dbc.NavbarSimple(
            children=[
            dbc.NavItem(dbc.NavLink("Home", href="/")),
            dbc.NavItem(dbc.NavLink("Graph", href="/graph")),
            dbc.NavItem(dbc.NavLink("Table", href="/table")),
            ],
        brand="SensorHub Dashboard",
        brand_href="#",
        color="primary",
        dark=True,
        ),
        
        html.Div(id='page-content')
        ] , className='container' 
    )
]

# index-page
index_page = html.Div(id='page-content', children=[
                        dcc.Graph(id='map-graph',figure=fig2),
                        dcc.Graph(id='map',figure=fig)
])

# table-page
table_page = html.Div(id='page-content', children=[
                dash_table.DataTable(data=geo_df[['name','properties/station_type']].to_dict('records'), page_size=20)
            ]),

# graph-page
graph_page = html.Div(id='page-content', children = [
                dcc.Graph(figure=fig2)
            ]),

@app.callback(Output('page-content', 'children'), 
              [Input('url','pathname')])
def display_page(pathname):
    if pathname == '/table':
        return table_page
    elif pathname == '/graph':
        return graph_page
    else:
        return index_page

@app.callback(
    Output('map-graph','figure'),
    Input('map','clickData'))
def update_graph(clickData):
    # print(clickData)
    thing_id = clickData['points'][0]['customdata'][0]
    url = f'{frost_api}Things({thing_id})?$select=@iot.id,name&$expand=Datastreams($expand=Observations($select=phenomenonTime,result))&$select=@iot.id'
    with requests.session() as session2:
        data = get_data(session2, url)

        df = pd.DataFrame(data['Datastreams'][0]['Observations'])
    
    fig2 = px.line(df, 'phenomenonTime','result', title=clickData['points'][0]['customdata'][1])

    return fig2

if __name__ == '__main__':
    app.run(host="0.0.0.0",port="8080",debug=True)
