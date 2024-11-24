import operator
import argparse

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql import SparkSession
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import GBTClassifier

MODEL_PATH = 'spark_ml_model'
LABEL_COL = 'is_bot'


def get_model_conf():
    """
    Создание конфигурации на обучение моделей.

    :return: dict - конфигурация экспериментов.
    """
    dt = DecisionTreeClassifier(labelCol=LABEL_COL)
    rf = RandomForestClassifier(labelCol=LABEL_COL)
    gbt = GBTClassifier(labelCol=LABEL_COL)
    model_conf = {
        'dt': {'model': dt,
               'params': ParamGridBuilder()
                   .addGrid(dt.maxDepth, [2, 5, 10])
                   .addGrid(dt.maxBins, [10, 20, 40])
                   .build(),
               'best': None,
               'score': None},
        'rf': {'model': rf,
               'params': ParamGridBuilder()
                   .addGrid(rf.maxDepth, [2, 5, 10])
                   .addGrid(rf.maxBins, [10, 20, 40])
                   .build(),
               'best': None,
               'score': None},
        'gbt': {'model': gbt,
                'params': ParamGridBuilder()
                    .addGrid(gbt.maxDepth, [2, 5, 10])
                    .addGrid(gbt.maxBins, [10, 20, 40])
                    .build(),
                'best': None,
                'score': None}
    }
    return model_conf


def build_pipeline(model_key, model_conf):
    """
    Создание пайплаина над выбранной моделью.

    :param model_key: ключ типа модели.
    :param model_conf: конфигурация моделей эксперимента.
    :return: Pipeline
    """
    user_type_indexer = StringIndexer(inputCol='user_type', outputCol="user_type_index")
    platform_indexer = StringIndexer(inputCol='platform', outputCol="platform_index")
    features = ["duration", "item_info_events", "select_item_events",
                "make_order_events", "events_per_min", "platform_index", "user_type_index"]
    features = VectorAssembler(inputCols=features, outputCol='features')
    return Pipeline(stages=[user_type_indexer, platform_indexer, features, model_conf[model_key]['model']])


def fit_cv(model_key, model_conf, evaluator, df_train, df_test):
    """
    Обучение пайплайна модели применяя CrossValidator. Все результаты сохраняются в конфигурации.

    :param model_key: ключ типа модели.
    :param model_conf: конфигурация моделей эксперимента.
    :param evaluator: оценщик качества модели.
    :param df_train: датасет для обучения модели.
    :param df_test:  датасет для оценки модели.
    """
    cv = CrossValidator(estimator=build_pipeline(model_key, model_conf),
                        estimatorParamMaps=model_conf[model_key]['params'],
                        evaluator=evaluator,
                        numFolds=2,
                        parallelism=3)
    fitted_models = cv.fit(df_train)
    best_model = fitted_models.bestModel
    score = evaluator.evaluate(best_model.transform(df_test))
    model_conf[model_key]['best'] = best_model
    model_conf[model_key]['score'] = score


def process(spark, data_path, model_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param data_path: путь до датасета
    :param model_path: путь сохранения обученной модели
    """
    model_conf = get_model_conf()
    evaluator = MulticlassClassificationEvaluator(labelCol="is_bot", predictionCol="prediction", metricName="f1")

    df = spark.read.parquet(data_path)
    df_bots_train, df_bots_test = df.filter(df.is_bot == 1).randomSplit([0.8, 0.2], 42)
    df_users_train, df_users_test = df.filter(df.is_bot == 0).randomSplit([0.8, 0.2], 42)
    df_train = df_users_train.union(df_bots_train)
    df_test = df_users_test.union(df_bots_test)

    for key in model_conf.keys():
        fit_cv(key, model_conf, evaluator, df_train, df_test)
        log_results(key, model_conf)

    key = get_best_key(model_conf)
    print(f"Best model type = {key} with score = {model_conf[key]['score']}")
    best_model = model_conf[key]['best']
    best_model.write().overwrite().save(model_path)
    print('Best model saved')


def get_best_key(model_conf):
    """
    Выбор наилучшей модели согласно оценке.

    :param model_conf: конфигурация моделей эксперимента.
    :return: ключ лучшей модели из конфигурации.
    """
    md = {k: v['score'] for k, v in model_conf.items()}
    return max(md.items(), key=operator.itemgetter(1))[0]


def log_results(model_key, model_conf):
    """
    Логирование метрик и гиперпараметров модели.

    :param model_key: ключ типа модели.
    :param model_conf: конфигурация моделей эксперимента.
    """
    j_obj = model_conf[model_key]['best'].stages[-1]._java_obj
    print(f'\nModel type = {model_key}')
    print(f"F1 = {model_conf[model_key]['score']}")
    print(f'maxDepth = {j_obj.getMaxDepth()}')
    print(f'maxBins = {j_obj.getMaxBins()}')


def main(data_path, model_path):
    spark = _spark_session()
    process(spark, data_path, model_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkMLFitJob').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_path', type=str, default='session-stat.parquet', help='Please set datasets path.')
    parser.add_argument('--model_path', type=str, default=MODEL_PATH, help='Please set model path.')
    args = parser.parse_args()
    data_path = args.data_path
    model_path = args.model_path
    main(data_path, model_path)
