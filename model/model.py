import os
from copy import copy

from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import OneHotEncoderModel
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import StringIndexerModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.feature import VectorIndexerModel
from pyspark.ml.regression import LinearRegressionModel

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


def train_models(df, save_path='/opt/workspace'):
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

    vector_assembler, df = train_vector_assembler(df,
                                                  target='price_num',
                                                  outputCol='features',
                                                  save_path=save_path,
                                                  transform=True)
    feature_indexer, df = train_feature_indexer(df,
                                                target='price_num',
                                                save_path=save_path,
                                                transform=True)
    lr_model = None

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
