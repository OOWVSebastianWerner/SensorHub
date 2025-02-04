import json

class Base:
   
    def to_json(self):
        return json.dumps(self.__dict__)