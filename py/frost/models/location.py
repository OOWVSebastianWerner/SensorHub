from .base import Base

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
        self.Things: list[dict] = [] #= [{'@iot.id': id} for id in thing_ids]
    
    def link_thing(self, thing_id):
        self.Things.append({'@iot.id': thing_id})