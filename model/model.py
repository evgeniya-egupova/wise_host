import os

from pyspark.ml.feature import StringIndexer

LR_MODEL_PATH = 'lr_model'
STRING_INDEXER_PATH = 'stringIndexer'


def train_string_indexer(df, inputCols, outputCols, save_path='./', transform=True):
    string_indexer = StringIndexer(inputCols=inputCols,
                                   outputCols=outputCols,
                                   stringOrderType="frequencyDesc")
    string_indexer_model = string_indexer.fit(df)
    if transform:
        df = string_indexer_model.transform(df)
        df = df.drop(*["neighbourhood_group", 'neighbourhood', 'room_type'])

    stringIndexerPath = os.path.join(save_path, STRING_INDEXER_PATH)
    string_indexer_model.save(stringIndexerPath)

    return string_indexer_model, df
