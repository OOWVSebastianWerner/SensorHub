from dagster import Definitions, load_assets_from_modules

from .assets import groundwater, surfacewater, things  # noqa: TID252

all_assets = load_assets_from_modules([groundwater, surfacewater, things])

defs = Definitions(
    assets=all_assets,
)