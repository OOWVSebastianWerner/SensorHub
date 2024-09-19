# NLWKN Groundwater API

[documentation (pdf)](https://www.grundwasserstandonline.nlwkn.niedersachsen.de/pdf/BenutzerhandbuchWebserviceGrundwasserstandonline.pdf) *german*

licence: [dl-de/by-2-0](https://www.govdata.de/dl-de/by-2-0)

## Stations

Lists all available stations:

> <https://bis.azure-api.net/GrundwasserstandonlinePublic/REST/stammdaten/stationen/allegrundwasserstationen?key=9dc05f4e3b4a43a9988d747825b39f43>

Lists a selected station (station-id):

> <https://bis.azure-api.net/GrundwasserstandonlinePublic/REST/stammdaten/stationen/%7BstationIds%7D?key=9dc05f4e3b4a4>

## Measurements

All measurements for one station:

> <https://bis.azure-api.net/GrundwasserstandonlinePublic/REST/station/{STA_ID}/datenspuren/parameter/{PAT_ID}/tage/{tage}?key=9dc05f4e3b4a43a9988d747825b39f43>

### Parameters

- STA_ID = id of the selected groundwater station
- PAT_ID = id of selected parameter
- Tage = amount of days to be deliverd (negative from today, e.g -7 for the last 7 days)
