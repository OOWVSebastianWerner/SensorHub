from dagster import Definitions, load_assets_from_modules

from .assets import groundwater, surfacewater, things  # noqa: TID252
from .ressources import database_ressource, WSA_API, NLWKN_API, FROST_API

all_assets = load_assets_from_modules([groundwater, surfacewater, things])

defs = Definitions(
    assets=all_assets,
    resources={
        'database': database_ressource,
        'wsa_api': WSA_API(),
        'nlwkn_api': NLWKN_API(),
        'frost_api': FROST_API()
    }
)