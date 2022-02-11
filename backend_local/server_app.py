from flask import Flask, request, jsonify
from flask_cors import CORS
import json

app = Flask(__name__)
CORS(app)

@app.route('/', methods=['POST'])
def vzzz():
    json_data = request.get_json()
    availability = json_data["availability"]
    minimum_nights = json_data['minimum_nights']
    latitude = json_data['latitude']
    longitude = json_data['longitude']
    name = json_data['name']
    neighbourhood_group = json_data['neighbourhood_group']
    neighbourhood = json_data['neighbourhood']
    room_type = json_data['room_type']
    dept = [(name, neighbourhood_group, neighbourhood, room_type, latitude, longitude, 0.0, minimum_nights, 0.0, 1.0, availability, 0.0)]
    
    return jsonify(data=123)

app.run(host='0.0.0.0')