import argparse  # Импортируем модуль для обработки аргументов командной строки
import os  # Импортируем модуль для работы с операционной системой
from pyspark.ml import Pipeline  # Импортируем класс Pipeline из библиотеки PySpark для создания пайплайна
from pyspark.ml.evaluation import MulticlassClassificationEvaluator  # Импортируем класс для оценки моделей
from pyspark.ml.feature import VectorAssembler, StringIndexer, \
    StringIndexerModel  # Импортируем классы для работы с признаками
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder  # Импортируем классы для настройки гиперпараметров
from pyspark.sql import SparkSession  # Импортируем класс для создания сессии Spark
from pyspark.ml.classification import GBTClassifier  # Импортируем классификатор градиентного бустинга
import mlflow  # Импортируем библиотеку для отслеживания экспериментов машинного обучения
from mlflow.tracking import MlflowClient  # Импортируем клиент для взаимодействия с MLflow

# Установка окружения для работы с MLflow и S3
os.environ['MLFLOW_S3_ENDPOINT_URL'] = 'https://storage.yandexcloud.net'  # Устанавливаем эндпоинт для S3
os.environ['AWS_ACCESS_KEY_ID'] = 'VsSLmhBg5or3QeP-bYwW'  # Устанавливаем ключ доступа AWS
os.environ['AWS_SECRET_ACCESS_KEY'] = 'e61seRCXf_STt5CFDQ8yoRXHWWHam_D9_pqnHGDe'  # Устанавливаем секретный ключ AWS

# Определение метки класса, которая будет использоваться в модели
LABEL_COL = 'has_car_accident'


def mlflow_start():
    """
    Функция для начала новой сессии в MLflow.
    """
    mlflow.set_tracking_uri("https://mlflow.lab.karpov.courses")  # Устанавливаем URI для подключения к MLflow
    mlflow.set_experiment(experiment_name="p-kovalchuk")  # Устанавливаем имя эксперимента в MLflow
    mlflow.start_run()  # Начинаем новый эксперимент в MLflow


def build_pipeline(train_alg):
    """
    Функция для создания пайплайна машинного обучения.

    :param train_alg: Алгоритм обучения, который будет добавлен в конец пайплайна.
    :return: Pipeline объект, содержащий все стадии преобразования и алгоритм обучения.
    """
    # Индексирование категориальных признаков (например, пол и класс автомобиля)
    sex_indexer = StringIndexer(inputCol='sex', outputCol="sex_index")
    car_class_indexer = StringIndexer(inputCol='car_class', outputCol="car_class_index")

    # Агрегация числовых признаков в вектор
    features = ["age", "sex_index", "car_class_index", "driving_experience",
                "speeding_penalties", "parking_penalties", "total_car_accident"]
    assembler = VectorAssembler(inputCols=features, outputCol='features')

    # Возвращаем пайплайн с индексаторами и агрегатором
    return Pipeline(stages=[sex_indexer, car_class_indexer, assembler, train_alg])


def evaluate_model(evaluator, predict, metric_list):
    """
    Функция для оценки модели по заданным метрикам.

    :param evaluator: Оценщик, который будет использоваться для расчета метрик.
    :param predict: Датасет с прогнозами модели.
    :param metric_list: Список метрик, по которым будет производиться оценка.
    :return: Словарь с результатами оценки по каждой метрике.
    """
    metrics = {}
    for metric in metric_list:
        evaluator.setMetricName(metric)  # Устанавливаем метрику для оценщика
        score = evaluator.evaluate(predict)  # Оцениваем модель по заданной метрике
        metrics[metric] = score  # Сохраняем результат
        print(f"{metric} score = {score}")  # Выводим результат на экран
    return metrics


def optimization(pipeline, gbt, train_df, evaluator):
    """
    Функция для оптимизации гиперпараметров модели с использованием TrainValidationSplit.

    :param pipeline: Пайплайн, содержащий модель и преобразования.
    :param gbt: Градиентный бустинг классификатор, который будет оптимизирован.
    :param train_df: Тренировочный датасет.
    :param evaluator: Оценщик для определения качества модели.
    :return: Лучшая модель после оптимизации.
    """
    # Конфигурация пространства поиска гиперпараметров
    grid = ParamGridBuilder() \
        .addGrid(gbt.maxDepth, [3, 5]) \
        .addGrid(gbt.maxIter, [20, 30]) \
        .addGrid(gbt.maxBins, [16, 32]) \
        .build()

    # Создание объекта TrainValidationSplit для разбиения данных и поиска оптимальных гиперпараметров
    tvs = TrainValidationSplit(estimator=pipeline,
                               estimatorParamMaps=grid,
                               evaluator=evaluator,
                               trainRatio=0.8)

    # Обучение модели и выбор лучшей модели
    models = tvs.fit(train_df)
    return models.bestModel


def process(spark, train_path, test_path):
    """
    Основная функция для обучения и оценки модели.

    :param spark: SparkSession для работы с данными.
    :param train_path: Путь к тренировочному датасету.
    :param test_path: Путь к тестовому датасету.
    """
    # Создание оценщика для многоклассовой классификации
    evaluator = MulticlassClassificationEvaluator(labelCol=LABEL_COL, predictionCol="prediction", metricName='f1')

    # Чтение тренировочных и тестовых данных
    train_df = spark.read.parquet(train_path)
    test_df = spark.read.parquet(test_path)

    # Создание модели
    gbt = GBTClassifier(labelCol=LABEL_COL)  # Градиентный бустинг классификатор
    pipeline = build_pipeline(gbt)  # Создание пайплайна

    # Обучение модели и трансформация тестового датасета
    model = optimization(pipeline, gbt, train_df, evaluator)

    predict = model.transform(test_df)  # Прогнозирование на тестовом датасете

    # Оценка модели
    metrics = evaluate_model(evaluator, predict, ['f1', 'weightedPrecision', 'weightedRecall', 'accuracy'])
    print('Best model saved')  # Выводим сообщение о сохранении лучшей модели

    # Логирование метрик в MLflow
    for metric_name, metric_value in metrics.items():
        mlflow.log_metric(metric_name, metric_value)

    # Логирование модели в MLflow
    mlflow.spark.log_model(model, artifact_path="p-kovalchuk", registered_model_name="p-kovalchuk")

    # Логирование гиперпараметров в MLflow
    mlflow.log_param('MaxDepth', model.stages[-1].getMaxDepth())
    mlflow.log_param('MaxNumTrees', model.stages[-1].getMaxBins())
    mlflow.log_param('Impurity', model.stages[-1].getMinInfoGain())

    # Логирование этапов пайплайна (стейджей)
    for i in range(0, len(model.stages)):  # Проходим по всем этапам (стейджам) модели
        stage = model.stages[i]
        mlflow.log_param(f'stage_{i}_type', stage)
        if isinstance(stage, VectorAssembler):
            mlflow.log_param(f'stage_{i}_input', stage.getInputCols())
            mlflow.log_param(f'stage_{i}_output', stage.getOutputCol())
        elif isinstance(stage, StringIndexerModel):
            mlflow.log_param(f'stage_{i}_input', stage.getInputCol())
            mlflow.log_param(f'stage_{i}_output', stage.getOutputCol())
        else:
            mlflow.log_param(f'stage_{i}_features', stage.getFeaturesCol())
            mlflow.log_param(f'stage_{i}_label', stage.getLabelCol())


def main(train_path, test_path):
    """
    Главная функция, которая запускает процесс обучения и оценки модели.

    :param train_path: Путь к тренировочному датасету.
    :param test_path: Путь к тестовому датасету.
    """
    mlflow_start()  # Запуск сессии в MLflow
    # Создание SparkSession
    spark = _spark_session()
    # Запуск процесса обучения и оценки модели
    process(spark, train_path, test_path)
    mlflow.end_run()  # Завершаем сессию в MLflow


def _spark_session():
    """
    Функция для создания SparkSession.

    :return: SparkSession для работы с данными.
    """
    return SparkSession.builder.appName('PySparkMLJob').getOrCreate()


# Парсинг аргументов командной строки для получения путей к датасетам
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--train', type=str, default='train.parquet', help='Please set train datasets path.')
    parser.add_argument('--test', type=str, default='test.parquet', help='Please set test datasets path.')
    args = parser.parse_args()
    train = args.train
    test = args.test
    main(train, test)
# # Запуск процесса с аргументами, переданными через Jupyter Notebook
# train_path = 'train.parquet'  # Путь к тренировочному набору данных
# test_path = 'test.parquet'  # Путь к тестовому набору данных
#
#
# main(train_path, test_path)
