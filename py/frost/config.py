# ------------------------------------------------------------------------------
# Global config vars
# ------------------------------------------------------------------------------
#@Todo: refactor to dotenv
baseURL = 'http://localhost:8080'

serverURL = '/FROST-Server/v1.1/'

endpoints = {
    'things': f'{baseURL}{serverURL}Things',
    'locations': f'{baseURL}{serverURL}Locations',
    'sensors': f'{baseURL}{serverURL}Sensors',
    'observedProperties': f'{baseURL}{serverURL}ObservedProperties'
}