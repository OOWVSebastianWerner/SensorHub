# ------------------------------------------------------------------------------
# Global config vars
# ------------------------------------------------------------------------------
baseURL = 'http://localhost:8080'

serverURL = '/FROST-Server/v1.1/'

endpoints = {
    'things': f'{baseURL}{serverURL}Things',
    'sensors': f'{baseURL}{serverURL}Sensors',
    'observedProperties': f'{baseURL}{serverURL}ObservedProperties'
}