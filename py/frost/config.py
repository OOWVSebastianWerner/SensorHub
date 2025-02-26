# ------------------------------------------------------------------------------
# Global config vars
# ------------------------------------------------------------------------------
#@Todo: refactor to dotenv
baseURL = 'http://localhost:8080'

serverURL = '/FROST-Server/v1.1/'

endpoints = {
<<<<<<< HEAD:py/frost/frost_config.py
    'things': f'{serverURL}Things',
    'sensors': f'{serverURL}Sensors',
    'observedProperty': f'{serverURL}ObservedProperty'
}
=======
    'things': f'{baseURL}{serverURL}Things',
    'locations': f'{baseURL}{serverURL}Locations',
    'sensors': f'{baseURL}{serverURL}Sensors',
    'observedProperties': f'{baseURL}{serverURL}ObservedProperties'
}
>>>>>>> 749e308b4294b9edbe82cff9b7957e002e1a144e:py/frost/config.py
