import argparse
from pyspark.ml.pipeline import PipelineModel, Pipeline
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, DecisionTreeClassifier, GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit

# Путь к сохранению модели
MODEL_PATH = 'spark_ml_model'
# Признак, который мы хотим предсказать
LABEL_COL = 'is_bot'


def process(spark, data_path, model_path):
    # Загрузка данных из Parquet файла
    session_stat_df = spark.read.parquet(data_path)

    # Разделение данных на тренировочный и тестовый наборы
    train_df, eval_df = session_stat_df.randomSplit([0.8, 0.2], seed=42)

    # Преобразование категориальных признаков в числовые
    index_user_type = StringIndexer(inputCol="user_type", outputCol="index_user_type").fit(session_stat_df)
    index_platform = StringIndexer(inputCol="platform", outputCol="index_platform").fit(session_stat_df)

    # Агрегация признаков в один вектор
    features = ["index_user_type", "duration", "index_platform", "item_info_events",
                "select_item_events", "make_order_events", "events_per_min"]
    feature_assembler = VectorAssembler(inputCols=features, outputCol="features")

    # Определение моделей для классификации
    rf = RandomForestClassifier(labelCol='is_bot', featuresCol="features")
    lr = LogisticRegression(labelCol='is_bot', featuresCol="features")
    dt = DecisionTreeClassifier(labelCol='is_bot', featuresCol="features")
    gbt = GBTClassifier(labelCol='is_bot', featuresCol='features')

    # Создание пайплайнов для каждой модели
    pipeline_rf = Pipeline(stages=[index_user_type, index_platform, feature_assembler, rf])
    pipeline_lr = Pipeline(stages=[index_user_type, index_platform, feature_assembler, lr])
    pipeline_dt = Pipeline(stages=[index_user_type, index_platform, feature_assembler, dt])
    pipeline_gbt = Pipeline(stages=[index_user_type, index_platform, feature_assembler, gbt])

    # Настройка сетки гиперпараметров для каждой модели
    paramGrid_rf = ParamGridBuilder() \
        .addGrid(rf.maxDepth, [5, 10]) \
        .addGrid(rf.numTrees, [20, 50]) \
        .addGrid(rf.maxBins, [32, 64]) \
        .build()

    paramGrid_lr = ParamGridBuilder() \
        .addGrid(lr.regParam, [0.01, 0.1]) \
        .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
        .build()

    paramGrid_dt = ParamGridBuilder() \
        .addGrid(dt.maxDepth, [5, 10]) \
        .addGrid(dt.maxBins, [32, 64]) \
        .build()

    paramGrid_gbt = ParamGridBuilder() \
        .addGrid(gbt.maxDepth, [5, 10]) \
        .addGrid(gbt.maxIter, [10, 20]) \
        .build()

    # Оценщик для измерения точности модели
    evaluator = MulticlassClassificationEvaluator(labelCol='is_bot', predictionCol="prediction", metricName="accuracy")

    # Использование TrainValidationSplit для поиска наилучших гиперпараметров
    tvs_rf = TrainValidationSplit(estimator=pipeline_rf,
                                  estimatorParamMaps=paramGrid_rf,
                                  evaluator=evaluator,
                                  trainRatio=0.8)

    tvs_lr = TrainValidationSplit(estimator=pipeline_lr,
                                  estimatorParamMaps=paramGrid_lr,
                                  evaluator=evaluator,
                                  trainRatio=0.8)

    tvs_dt = TrainValidationSplit(estimator=pipeline_dt,
                                  estimatorParamMaps=paramGrid_dt,
                                  evaluator=evaluator,
                                  trainRatio=0.8)

    tvs_gbt = TrainValidationSplit(estimator=pipeline_gbt,
                                   estimatorParamMaps=paramGrid_gbt,
                                   evaluator=evaluator,
                                   trainRatio=0.8)

    # Обучение моделей
    tvs_model_rf = tvs_rf.fit(train_df)
    tvs_model_lr = tvs_lr.fit(train_df)
    tvs_model_dt = tvs_dt.fit(train_df)
    tvs_model_gbt = tvs_gbt.fit(train_df)

    # Оценка моделей
    accuracy_rf = evaluator.evaluate(tvs_model_rf.transform(eval_df))
    accuracy_lr = evaluator.evaluate(tvs_model_lr.transform(eval_df))
    accuracy_dt = evaluator.evaluate(tvs_model_dt.transform(eval_df))
    accuracy_gbt = evaluator.evaluate(tvs_model_gbt.transform(eval_df))

    # Вывод точности каждой модели
    print(f"Random Forest accuracy: {accuracy_rf}")
    print(f"Logistic Regression accuracy: {accuracy_lr}")
    print(f"Decision Tree accuracy: {accuracy_dt}")
    print(f"Gradient-Boosted Tree accuracy: {accuracy_gbt}")

    # Выбор лучшей модели
    models = [("Random Forest", tvs_model_rf, accuracy_rf),
              ("Logistic Regression", tvs_model_lr, accuracy_lr),
              ("Decision Tree", tvs_model_dt, accuracy_dt),
              ("Gradient-Boosted Tree", tvs_model_gbt, accuracy_gbt)]

    best_model_name, best_model, best_accuracy = max(models, key=lambda item: item[2])

    print(f"Best model: {best_model_name} with accuracy: {best_accuracy}")

    # Сохранение лучшей модели
    best_model.bestModel.write().overwrite().save(model_path)


def main(data_path, model_path):
    # Создание SparkSession
    spark = _spark_session()
    # Запуск процесса обучения модели
    process(spark, data_path, model_path)


def _spark_session():
    # Создание SparkSession
    return SparkSession.builder.appName('PySparkMLFitJob').getOrCreate()


if __name__ == "__main__":
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_path', type=str, default='session-stat.parquet', help='Please set datasets path.')
    parser.add_argument('--model_path', type=str, default=MODEL_PATH, help='Please set model path.')
    args = parser.parse_args()
    data_path = args.data_path
    model_path = args.model_path
    main(data_path, model_path)

# # Установка параметров для запуска в Jupyter Notebook
# data_path = 'session-stat.parquet'
# model_path = 'spark_ml_model'
#
# # Запуск основного процесса
# main(data_path, model_path)