raw_gw_levels_FILE_PATH="data/raw/raw_groundwater_levels.json"
raw_sw_stations_FILE_PATH="data/raw/raw_surfacewater_stations.json"
things_list_FILE_PATH="data/curr_things.json"

# NLWKN API
NLWKN_API_KEY='9dc05f4e3b4a43a9988d747825b39f43'
NLWKN_API='https://bis.azure-api.net/GrundwasserstandonlinePublic/REST/stammdaten/stationen/allegrundwasserstationen'

# WSA API
WSA_API_BASE='https://www.pegelonline.wsv.de/webservices/rest-api/v2/'
WSA_API_STATIONS=f'{WSA_API_BASE}stations.json'

# FROST SERVER
FROST_API_BASE='http://localhost:8080/FROST-Server/'
FROST_API_THINGS=f'{FROST_API_BASE}v1.1/Things'