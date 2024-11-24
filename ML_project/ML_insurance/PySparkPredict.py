import argparse
import os
import mlflow
from mlflow.tracking import MlflowClient
from pyspark.sql import SparkSession
from mlflow.tracking import MlflowClient

os.environ['MLFLOW_S3_ENDPOINT_URL'] = 'https://storage.yandexcloud.net'
os.environ['AWS_ACCESS_KEY_ID'] = 'VsSLmhBg5or3QeP-bYwW'  # креды доступа к S3
os.environ['AWS_SECRET_ACCESS_KEY'] = 'e61seRCXf_STt5CFDQ8yoRXHWWHam_D9_pqnHGDe'


def process(spark, data_path, result):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param data_path: путь до датасета
    :param result: путь сохранения результата
    """
    df = spark.read.parquet(data_path)
    # Устанавливаем URI для отслеживания экспериментов в MLflow
    mlflow.set_tracking_uri("https://mlflow.lab.karpov.courses")
    client = MlflowClient()

    # v_1  Получение последней версии модели в стадии Production
    # def get_last_prod_model(name):
    #     last_models = client.get_registered_model(name).latest_versions
    #     models = list(filter(lambda x: x.current_stage == 'Production', last_models))
    #     if len(models) == 0:
    #         return None
    #     else:
    #         return models[0]

    # model_version = get_last_prod_model('p-kovalchuk')
    # model = mlflow.spark.load_model(f'models:/p-kovalchuk/{model_version.version}')
    # ----------------------------------------------------------------
    # v_2  Получение последней версии модели в стадии Production
    model_version = 'Production'
    model_name = 'p-kovalchuk'
    model = mlflow.spark.load_model(f'models:/{model_name}/{model_version}')

    # ----------------------------------------------------------------
    res = model.transform(df)
    res.show(5)
    res.write.mode("overwrite").format('parquet').save(result)


def main(data, result):
    spark = _spark_session()
    process(spark, data, result)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkPredict').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', type=str, default='data.parquet', help='Please set datasets path.')
    parser.add_argument('--result', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    data = args.data
    result = args.result
    main(data, result)
