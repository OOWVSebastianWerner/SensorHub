import requests
from dagster import asset
from .. import constants

@asset
def groundwater_levels_file() -> None:
    """
        Load groundwater levels from the NLWKN-API.
    """
   
    raw_gw_levels = requests.get(f'{constants.NLWKN_API}?key={constants.NLWKN_API_KEY}')

    with open(constants.raw_gw_levels_FILE_PATH, 'wb') as output_file:
        output_file.write(raw_gw_levels.content)


@asset(deps=[groundwater_levels_file])
def groundwater_stations() -> None:
    """
        Extract stations from groundwater-levels-file.
    """

