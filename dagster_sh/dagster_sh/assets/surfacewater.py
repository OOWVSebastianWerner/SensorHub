import requests
from dagster import asset, AssetExecutionContext, ConfigurableResource
from dagster_duckdb import DuckDBResource
from ..ressources import WSA_API
from . import constants
import pandas as pd

@asset(
        group_name='surfacewater'
)
def surfacewater_stations_file(wsa_api: WSA_API) -> None:
    """
        Load surfacewater stations from wsa API.
    """

    # raw_sw_stations = requests.get(constants.WSA_API_STATIONS)
    raw_sw_stations = wsa_api.request(constants.WSA_API_STATIONS)#.json()

    with open(constants.raw_sw_stations_FILE_PATH, 'wb') as output_file:
        output_file.write(raw_sw_stations.content)
    

@asset(
    deps=[surfacewater_stations_file],
    group_name='surfacewater'
)
def surfacewater_stations(database: DuckDBResource) -> None:
    """
        Load surfacewater stations into DuckDB.
    """

    sql_query = f"""
    create or replace table surfacewater_stations as (
        select
            uuid as station_id,
            number,
            shortname,
            longname,
            km,
            agency,
            longitude,
            latitude,
            water
        from '{constants.raw_sw_stations_FILE_PATH}'
    );
    """

    with database.get_connection() as conn:
        conn.execute(sql_query)

@asset(
        deps=[surfacewater_stations],
        group_name='surfacewater')
def surfacewater_station_ids(database: DuckDBResource) -> pd.DataFrame:
    """
     Get List of Station Ids
    """
    sql_query = 'SELECT station_id from surfacewater_stations;'

    with database.get_connection() as conn:
       df_surfacewater_station_ids = conn.execute(sql_query).fetch_df()
    
    ls_uuids = df_surfacewater_station_ids.station_id.to_list()

    # for uuid in ls_uuids:
    #     url_measurements = f'{baseUrl}stations/{uuid}/W/measurements.json'
    #     r = s.get(url_measurements)

    #     if r.status_code == 200:
    #         data = r.json()

    #         df = pd.DataFrame(data)
    #         df['stationId'] = uuid

    #         df.to_csv(levelsPath / f'{uuid}.csv', sep=';')
                