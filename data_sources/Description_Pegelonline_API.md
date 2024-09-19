# Pegelonline API

[documentation](https://www.pegelonline.wsv.de/webservice/guideRestapi) *german*

licence: [DL-DW->ZERO-2.0](https://www.govdata.de/dl-de/zero-2-0)

## Stations

List of available Stations.

URL: <https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json>

fields:

- uuid: id
- number: number of gauge
- shortname: name of gauge (max. 40 characters)
- longname: name of gauge (max. 255 characters)
- km: river kilometers
- agency: name of authority in charge
- longitude: longitude in WGS84 in decimal notation
- latitude: latitude in WGS84 in decimal notation
- water:
  - shortname: name of water body
  - longname: name of water body

## Measurement

measured raw data for the last 31 days.

URL: ../stations/*uuid*/W/measurements.json

fields:

- timestamp: timestamp with timezone (ISO_8601)
- value: measured value

## Waters

List of water bodies.

fields:

- longname
- shortname
