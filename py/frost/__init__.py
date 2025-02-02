# ------------------------------------------------------------------------------
# FROST Server classes
# ------------------------------------------------------------------------------
import json
import config
import func

class Base:
    def to_json(self):
        return json.dumps(self.__dict__)

class Thing(Base):
    def __init__(self, name, station_type, foreign_id):
        self.name = name
        self.description = ''
        self.properties = {
            'station_type': station_type,
            'foreign_id': foreign_id
        }
    
    def add_property(self, property = list[str]):
        self.properties[property[0]] = property[1]

class Location(Base):
    def __init__(self, name:str, latitude:float, longitude:float): #, thing_ids:list[int]):
        self.name = name
        self.description = ''
        self.encodingType: str = "application/geo+json"
        self.properties: dict
        self.location: dict = { 
            'type': 'Point', 
            'coordinates': [longitude, latitude]
        }
        self.Things = [{'@iot.id': id} for id in thing_ids]
    
    def link_thing(self, thing_id):
        self.Things.append({'@iot.id': thing_id})

class Datastream(Base):
    def __init__(self, name:str, thing_id, sensor_id, obsProp_id):
        self.name = name
        self.description = ''
        self.observationType: str
        self.unitOfMeasurement: dict = {
            'name': str,
            'symbol': str,
            'definition': str
        }
        self.Thing: dict = {"@iot.id": thing_id}
        self.Sensor: dict = {"@iot.id": sensor_id}
        self.ObservedProperty: dict = {"@iot.id": obsProp_id}