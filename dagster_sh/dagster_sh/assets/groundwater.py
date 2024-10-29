import requests
from dagster import asset
from ..ressources import NLWKN_API
from . import constants 

@asset(
        group_name="raw_file"
)
def groundwater_levels_file(nlwkn_api: NLWKN_API) -> None:
    """
        Load groundwater levels from the NLWKN-API.
    """
   
    raw_gw_levels = nlwkn_api.request(constants.NLWKN_API_GW_STATIONS)
    #requests.get(f'{constants.NLWKN_API}?key={constants.NLWKN_API_KEY}')

    with open(constants.raw_gw_levels_FILE_PATH, 'wb') as output_file:
        output_file.write(raw_gw_levels.content)


@asset(
        deps=[groundwater_levels_file],
        group_name='ingested')
def groundwater_stations() -> None:
    """
        Extract stations from groundwater-levels-file.
    """

