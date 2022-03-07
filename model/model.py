import os

from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder, OneHotEncoderModel

LR_MODEL_PATH = 'lr_model'
STRING_INDEXER_PATH = 'stringIndexer'
OHE_GOT_ENCODER_PATH = 'ohe'


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
