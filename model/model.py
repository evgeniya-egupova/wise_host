import os
from copy import copy

from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import OneHotEncoderModel
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler

LR_MODEL_PATH = 'lr_model'
STRING_INDEXER_PATH = 'stringIndexer'
OHE_GOT_ENCODER_PATH = 'ohe'
VECTOR_ASSEMBLER_PATH = 'vectorAssembler'


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

    ohe_model_path = os.path.join(save_path, OHE_GOT_ENCODER_PATH)
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


def train_models(df):
    _, df = train_string_indexer(df,
                                 inputCols=["neighbourhood_group", 'neighbourhood', 'room_type'],
                                 outputCols=["neighbourhood_group_int", 'neighbourhood_int', 'room_type_int'],
                                 save_path='/opt/workspace',
                                 transform=True)
    _, df = train_one_hot_encoder(df,
                                  inputCols=["neighbourhood_group_int", 'neighbourhood_int', 'room_type_int'],
                                  outputCols=["neighbourhood_group_vec", 'neighbourhood_vec', 'room_type_vec'],
                                  save_path='/opt/workspace',
                                  transform=True)
    _, df = train_vector_assembler(df,
                                   target='price_num',
                                   outputCol='features',
                                   save_path='/opt/workspace',
                                   transform=True)
