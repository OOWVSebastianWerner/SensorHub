#------------------------------------------------------------------------------
# Name:         data get things
#               Get all things from FROST-Server and save as json
#
# author:       Sebastian Werner
# email:        s.werner@oowv.de
# created:      07.11.2024
#------------------------------------------------------------------------------
#%%
import requests
from pathlib import Path
import json

#------------------------------------------------------------------------------
#--- global vars
#------------------------------------------------------------------------------
#%%
basePath = Path(r'..\data')

things_request = requests.get(f'http://localhost:8080/FROST-Server/v1.1/Things')

#%%
with open(basePath / 'curr_things.json', 'w') as output_file:
        output_file.write(json.dumps(things_request.json(), indent=4))
# %%
