import os
from copy import copy

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import OneHotEncoderModel
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import StringIndexerModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.feature import VectorIndexerModel
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql.functions import datediff, to_date, lit, length, when
from pyspark.sql.types import DateType


LR_MODEL_PATH = 'lr_model'
STRING_INDEXER_PATH = 'stringIndexer'
ONE_HOT_ENCODER_PATH = 'ohe'
VECTOR_ASSEMBLER_PATH = 'vectorAssembler'
FEATURE_INDEXER_PATH = 'featureIndexer'


def train_string_indexer(df, inputCols, outputCols, save_path='./', transform=True):
    string_indexer = StringIndexer(inputCols=inputCols,
                                   outputCols=outputCols,
                                   stringOrderType="frequencyDesc")
    string_indexer_model = string_indexer.fit(df)
    if transform:
        df = string_indexer_model.transform(df)
        df = df.drop(*inputCols)

    stringIndexerPath = os.path.join(save_path, STRING_INDEXER_PATH)
    string_indexer_model.save(stringIndexerPath)

    return string_indexer_model, df


def train_one_hot_encoder(df, inputCols, outputCols, save_path='./', transform=True):
    ohe = OneHotEncoder(inputCols=inputCols,
                        outputCols=outputCols)
    ohe_model = ohe.fit(df)
    if transform:
        encoded = ohe_model.transform(df)
        df = encoded.drop(*inputCols)

    ohe_model_path = os.path.join(save_path, ONE_HOT_ENCODER_PATH)
    ohe_model.save(ohe_model_path)

    return ohe_model, df


def train_vector_assembler(df, target, inputCols=None, outputCol='features', save_path='./', transform=True):
    inputCols = inputCols or copy(df.columns)
    inputCols.remove(target)
    vector_assembler = VectorAssembler(inputCols=inputCols,
                                       outputCol=outputCol)
    if transform:
        df = vector_assembler.transform(df)
        df = df.select([outputCol, target])
        df.show(5, False)
        df.printSchema()

    vector_assembler_path = os.path.join(save_path, VECTOR_ASSEMBLER_PATH)
    vector_assembler.save(vector_assembler_path)

    return vector_assembler, df


def train_feature_indexer(df, target, inputCol='features', outputCol='features_vec',
                          save_path='./', maxCategories=230, transform=True):
    feature_indexer = VectorIndexer(inputCol=inputCol,
                                    outputCol=inputCol,
                                    maxCategories=maxCategories).fit(df)
    if transform:
        df = feature_indexer.transform(df)
        df = df.select([outputCol, target])
        df.select(outputCol).show(5, False)

    feature_indexer_path = os.path.join(save_path)
    feature_indexer.save(feature_indexer_path)

    return feature_indexer, df


def preprocess_dataset(df):
    # drop redundant columns and filter outliers
    df = df.drop('id', 'host_name', 'host_id')
    df = df.filter(df.price < 800)
    # After data loading all columns have string dtype. Transform to correct types
    df = df.withColumn('latitude_num', df['latitude'].cast('float')).drop('latitude') \
        .withColumn('longitude_num', df['longitude'].cast('float')).drop('longitude') \
        .withColumn('number_of_reviews_int', df['number_of_reviews'].cast('int')).drop('number_of_reviews') \
        .withColumn('minimum_nights_int', df['minimum_nights'].cast('int')).drop('minimum_nights') \
        .withColumn('reviews_per_month_num', df['reviews_per_month'].cast('float')).drop('reviews_per_month') \
        .withColumn('calculated_host_listings_count_num', df['calculated_host_listings_count'].cast('int')).drop(
        'calculated_host_listings_count') \
        .withColumn('availability_365_int', df['availability_365'].cast('int')).drop('availability_365') \
        .withColumn('last_review_date', df['last_review'].cast(DateType())).drop('last_review') \
        .withColumn('price_num', df['price'].cast('float')).drop('price')

    return df


def create_features(df):
    # Some feature engineering
    df = df.withColumn("minimum_nights",
                       when(df["minimum_nights_int"] > 30, 30).otherwise(df["minimum_nights_int"])).drop(
        'minimum_nights_int')
    df = df.withColumn('name_length', length('name')).drop('name')
    df = df.withColumn("days_from_review",
                       datediff(to_date(lit("2020-01-01")),
                                to_date("last_review_date", "yyyy-MM-dd"))).drop('last_review_date')
    # Fill NaNs and Nulls
    df = df.fillna({'days_from_review': 0,
                    'reviews_per_month_num': 0,
                    'name_length': 0,
                    'calculated_host_listings_count_num': 1,
                    'number_of_reviews_int': 0})
    df = df.na.drop()

    return df


def train_models(df, save_path='/opt/workspace'):
    df = preprocess_dataset(df)

    string_indexer, df = train_string_indexer(
        df,
        inputCols=["neighbourhood_group", 'neighbourhood', 'room_type'],
        outputCols=["neighbourhood_group_int", 'neighbourhood_int', 'room_type_int'],
        save_path=save_path,
        transform=True)

    one_hot_encoder, df = train_one_hot_encoder(
        df,
        inputCols=["neighbourhood_group_int", 'neighbourhood_int', 'room_type_int'],
        outputCols=["neighbourhood_group_vec", 'neighbourhood_vec', 'room_type_vec'],
        save_path=save_path,
        transform=True)

    df = create_features(df)

    vector_assembler, df = train_vector_assembler(df,
                                                  target='price_num',
                                                  outputCol='features',
                                                  save_path=save_path,
                                                  transform=True)
    feature_indexer, df = train_feature_indexer(df,
                                                target='price_num',
                                                save_path=save_path,
                                                transform=True)

    # split the data into train and test
    splits = df.randomSplit([0.8, 0.2])
    train_df = splits[0]
    test_df = splits[1]

    lr = LinearRegression(featuresCol='features_vec', labelCol='price_num')
    lr_model = lr.fit(train_df)
    trainingSummary = lr_model.summary
    print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
    print("r2: %f" % trainingSummary.r2)

    lr_predictions = lr_model.transform(test_df)
    lr_predictions.select("prediction", "price_num", "features_vec").show(5)
    lr_evaluator = RegressionEvaluator(predictionCol="prediction",
                                       labelCol="price_num", metricName="r2")
    print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(lr_predictions))

    lr_model.save(os.path.join(save_path, LR_MODEL_PATH))

    return string_indexer, one_hot_encoder, vector_assembler, feature_indexer, lr_model


def load_models(load_path='/opt/workspace'):
    model_path = os.path.join(load_path, LR_MODEL_PATH)
    vector_assembler_path = os.path.join(load_path, VECTOR_ASSEMBLER_PATH)
    ohe_model_path = os.path.join(load_path, ONE_HOT_ENCODER_PATH)
    string_indexer_path = os.path.join(load_path, STRING_INDEXER_PATH)
    feature_indexer_path = os.path.join(load_path, FEATURE_INDEXER_PATH)

    feature_indexer = VectorIndexerModel.load(feature_indexer_path)
    vector_assembler = VectorAssembler.load(vector_assembler_path)
    one_hot_encoder = OneHotEncoderModel.load(ohe_model_path)
    string_indexer = StringIndexerModel.load(string_indexer_path)
    lr_model = LinearRegressionModel.load(model_path)

    return string_indexer, one_hot_encoder, vector_assembler, feature_indexer, lr_model
