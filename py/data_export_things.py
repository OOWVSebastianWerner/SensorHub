#%%
import requests
import json
from pathlib import Path
from frost import frost_config
import func
#%%
outFilename = Path(r'..\data\curr_things.json')

with requests.Session() as s:
    
    func.save_response_as_file(
        s.get(f'{frost_config.baseURL}{frost_config.endpoints['things']}'),
        outFilename)

# %%
