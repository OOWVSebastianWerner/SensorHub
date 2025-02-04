from .base import Base

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
