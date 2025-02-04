from dagster_duckdb import DuckDBResource
from dagster import EnvVar, asset, Definitions, ConfigurableResource
import requests
from requests import Response

database_ressource = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE")
)

class WSA_API(ConfigurableResource):

    def request(self, endpoint: str) -> Response:
        return requests.get(
            f'https://www.pegelonline.wsv.de/webservices/rest-api/v2/{endpoint}',
            headers={
                'user-agent': 'dagster',
                'content-type': 'application/json; charset=UTF-8'})

class NLWKN_API(ConfigurableResource):
    
    def request( self, endpoint: str) -> Response:
        return requests.get(
            f'https://bis.azure-api.net/GrundwasserstandonlinePublic/REST/{endpoint}',
            params={
                'key': EnvVar("NLWKN_API_KEY").get_value()
            }
        )
    
class FROST_API(ConfigurableResource):
   
    def request(self, endpoint: str) -> Response:
        return requests.get(
            f'http://localhost:8080/FROST-Server/v1.1/{endpoint}'
        )