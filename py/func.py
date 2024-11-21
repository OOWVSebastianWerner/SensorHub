import json

def save_response_as_file(response, filename):
    with open(filename, 'w') as output_file:
        output_file.write(json.dumps(response.json(), indent=4))