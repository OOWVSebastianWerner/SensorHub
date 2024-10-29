import requests
import json
import pandas as pd
from dagster import asset
from ..ressources import FROST_API
from . import constants

@asset(
        group_name='raw_file'
)
def frost_server_things(frost_api: FROST_API) -> None:
    """
        Get a List of existing 'Things'.
    """

    things_list = frost_api.request(constants.FROST_API_THINGS)
    #requests.get(constants.FROST_API_THINGS)

    with open(constants.things_list_FILE_PATH, 'w') as output_file:
        output_file.write(json.dumps(things_list.json(), indent=4))

@asset(
    deps=[frost_server_things],
    group_name='ingested'
    )
def things_ids() -> None:
    """
    List of things and their ids.
    """
    things = pd.read_json(constants.things_list_FILE_PATH)

    
