import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

# A scipt that retrieves all places in Tübingen using the free Google Maps API.

city_coordinates = (48.5224444, 9.0573611)      # Coordinates in degrees
area_radius = 7000                              # Radius in meters. 
                                                # Visualized here: https://www.mapdevelopers.com/draw-circle-tool.php?circles=%5B%5B7000%2C48.5225496%2C9.0573426%2C%22%23AAAAAA%22%2C%22%23000000%22%2C0.4%5D%5D



# API Reference: https://developers.google.com/maps/documentation/places/web-service/nearby-search
url = 'https://places.googleapis.com/v1/places:searchNearby'

# Specify the filters for the Places API.
request_params = {
    'locationRestriction': {
        'circle': {
            'center': {
                'latitude': city_coordinates[0],
                'longitude': city_coordinates[1],
            },
            'radius': area_radius
        }
    }
}

requests_headers = {
    'Content-Type': 'application/json',
    'X-Goog-Api-Key': os.environ['GMAPS_API_KEY'],
    'X-Goog-FieldMask': '*'
}

response_json = requests.post(url, json = request_params, headers=requests_headers).json()

# Does not work unfortunately :(. Only 20 results per query allowed! Use open data instead. 
with open('../data/places.json', 'w') as fp:
    json.dump(response_json, fp, indent=4)

print("Obtained the response!")
