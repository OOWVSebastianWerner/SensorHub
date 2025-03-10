from .base import Base

class Sensor(Base):
    def __init__(self, name):
        self.name: str = name
        self.description: str = ''
        self.encodingType = 'application/text'
        self.metadata: str
