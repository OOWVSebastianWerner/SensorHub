import requests
from dagster import asset
from .. import constants

@asset
def surfacewater_stations_file() -> None:
    """
        Load surfacewater stations from wsa API.
    """

    raw_sw_stations = requests.get(constants.WSA_API_STATIONS)

    with open(constants.raw_sw_stations_FILE_PATH, 'wb') as output_file:
        output_file.write(raw_sw_stations.content)
    

@asset(deps=[surfacewater_stations_file])
def surfacewater_levels_file() -> None:
    """
        Load surfacewater levels from wsa API.
    """