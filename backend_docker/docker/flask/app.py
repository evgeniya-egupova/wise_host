from flask import Flask, request, jsonify
import json


app = Flask(__name__)

@app.route('/', methods=['POST'])
def main():
    
     
    json_data = request.get_json()
    
    availability = json_data.availability
    minimum_nights = json_data.minimum_nights
    latitude = json_data.latitude
    longitude = json_data.longitude
    name = json_data.name
    neighbourhood_group = json_data.neighbourhood_group
    neighbourhood = json_data.neighbourhood
    room_type = json_data.room_type
    
    dept = [(name,neighbourhood_group,neighbourhood,room_type,latitude,longitude,0.0,minimum_nights,0.0,1.0,availability,0.0)]

        
    return jsonify(data=123)
        