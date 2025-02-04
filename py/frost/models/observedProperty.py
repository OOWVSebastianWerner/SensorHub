from .base import Base

class ObservedProperty(Base):
    def __init__(self, name):
        self.name = name
        self.description = ''
        self.properties = {}
        self.definition = ''