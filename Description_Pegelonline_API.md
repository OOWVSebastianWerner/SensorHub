# Pegelonline API

(documentation)[https://www.pegelonline.wsv.de/webservice/guideRestapi] *german*

licence: (DL-DW->ZERO-2.0)[https://www.govdata.de/dl-de/zero-2-0]

Stations:

URL: https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json

JSON-File:
- uuid: id
- number: number of gauge
- shortname: name of gauge (max. 40 characters)
- longname:	name of gauge (max. 255 characters)
- km: river kilometers
- agency: name of	authority in charge
- longitude:	longitude in WGS84 in decimal notation
- latitude:	latitude in WGS84 in decimal notation
- water:
    - shortname: name of water body
    - longname: name of water body
