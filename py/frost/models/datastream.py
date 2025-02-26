from .base import Base

class Datastream(Base):
    def __init__(self, name:str, thing_id, sensor_id, obsProp_id):
        self.name = name
        self.description = ''
        self.observationType = ''
        self.unitOfMeasurement= {
            'name': '',
            'symbol': '',
            'definition': ''
        }
        self.Thing = {"@iot.id": thing_id}
        self.Sensor = {"@iot.id": sensor_id}
        self.ObservedProperty = {"@iot.id": obsProp_id}