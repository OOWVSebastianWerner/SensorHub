import requests
import json
import pandas as pd
from dagster import asset
from .. import constants

@asset
def frost_server_things() -> None:
    """
        Get a List of existing 'Things'.
    """

    things_list = requests.get(constants.FROST_API_THINGS)

    with open(constants.things_list_FILE_PATH, 'w') as output_file:
        output_file.write(json.dumps(things_list.json(), indent=4))

@asset(deps=[frost_server_things])
def things_ids() -> None:
    """
    List of things and their ids.
    """
    things = pd.read_json(constants.things_list_FILE_PATH)

    
