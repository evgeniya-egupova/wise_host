from flask import Flask, request, jsonify
import json
from pyspark.ml.regression import LinearRegression
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession
from pyspark.ml.feature import OneHotEncoderModel, VectorAssembler, StringIndexerModel, VectorIndexerModel
from pyspark.sql.functions import datediff, to_date, lit, length, when
from pyspark.ml.regression import RandomForestRegressionModel, LinearRegressionModel

model_path = '/opt/workspace/lr_model'
vectorAssembler_path = './vectorAssembler'
ohe_model_path = './ohe'
stringIndexerPath = './stringIndexer'
featureIndexer_path = './featureIndexer'

deptColumns = ['name',
               'neighbourhood_group',
               'neighbourhood',
               'room_type',
               'latitude_num',
               'longitude_num',
               'number_of_reviews_int',
               'minimum_nights_int',
               'reviews_per_month_num',
               'calculated_host_listings_count_num',
               'availability_365_int',
               'days_from_review']




app = Flask(__name__)

@app.route('/', methods=['POST'])
def main():
    
    feature_model = VectorIndexerModel.load(featureIndexer_path)
    vectorAssembler = vectorAssembler.load(vectorAssembler_path)
    ohe_model = OneHotEncoderModel.load(ohe_model_path)
    stringIndexer_model = StringIndexerModel.load(stringIndexerPath)
    lr_model = LinearRegressionModel.load(model_path)
    
    spark = SparkSession.builder.master("local").appName("Connection").getOrCreate()
    
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

    df = spark.createDataFrame(data=dept, schema = deptColumns)
    
    df = stringIndexer_model.transform(df)
    
    df = df.drop(*["neighbourhood_group", 'neighbourhood', 'room_type'])
    df = ohe_model.transform(df)
    df = df.drop(*["neighbourhood_group_int", 'neighbourhood_int', 'room_type_int'])

    df = df.withColumn("minimum_nights", when(df["minimum_nights_int"] > 30, 30).otherwise(df["minimum_nights_int"])).drop('minimum_nights_int')
    df = df.withColumn('name_length', length('name')).drop('name')

    df = vectorAssembler.transform(df)
    df = df.select(['features'])
    df = feature_model.transform(df)
    df = df.select(['features_vec'])

    lr_predictions = lr_model.transform(df)
    
    return jsonify(data=lr_predictions.collect()[-1].prediction)
        