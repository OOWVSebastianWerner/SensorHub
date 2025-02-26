#%%
import requests
import json
from pathlib import Path
from frost import config, func
#%%
outFilename = Path(r'..\data\curr_things.json')

with requests.Session() as s:
    
    func.save_response_as_file(
        s.get(f'{config.baseURL}{config.endpoints['things']}'),
        outFilename)

# %%
