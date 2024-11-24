import argparse
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer

# Константа для хранения пути к модели
MODEL_PATH = 'spark_ml_model'


# Основная функция для выполнения задачи
def main(data_path, model_path, result_path):
    # Создание SparkSession
    spark = _spark_session()

    # Чтение данных из Parquet файла
    test_df = spark.read.parquet(data_path)
    test_df.show(5)

    # Преобразование категориальных признаков в числовые
    index_user_type = StringIndexer(inputCol="user_type", outputCol="index_user_type").fit(test_df)
    index_platform = StringIndexer(inputCol="platform", outputCol="index_platform").fit(test_df)

    test_df = index_user_type.transform(test_df)
    test_df = index_platform.transform(test_df)
    test_df = test_df.drop("user_type", "platform")  # Удаление исходных категориальных признаков
    test_df.show(5)

    # Загрузка обученной модели Метод PipelineModel.load(path) используется для загрузки обученной модели,
    # сохраненной в виде PipelineModel, из указанного пути. PipelineModel представляет собой сериализованную версию
    # обученного пайплайна машинного обучения
    best_model = PipelineModel.load(model_path)

    # Применение модели к тестовым данным
    test_prediction = best_model.transform(test_df)
    test_prediction.show(5)

    # Сохранение результатов предсказаний в Parquet файл
    test_prediction.select("session_id", 'prediction').write.mode('overwrite').parquet(result_path)

    # Фильтруем предсказания, чтобы оставить только те, где бот был обнаружен (prediction == 1)
    bot_sessions = test_prediction.filter(test_prediction.prediction == 1)

    # Выводим session_id сессий, где был обнаружен бот
    bot_sessions.select("session_id", "prediction").show()
    print(f"Количество сессий, где был обнаружен бот: {bot_sessions.count()}")


# Функция для создания SparkSession
def _spark_session():
    return SparkSession.builder.appName('PySparkMLPredict').getOrCreate()


# Обработка аргументов командной строки
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--model_path', type=str, default=MODEL_PATH, help='Укажите путь к модели.')
    parser.add_argument('--data_path', type=str, default='test.parquet', help='Укажите путь к датасету.')
    parser.add_argument('--result_path', type=str, default='result', help='Укажите путь для сохранения результатов.')
    args = parser.parse_args()
    data_path = args.data_path
    model_path = args.model_path
    result_path = args.result_path
    main(data_path, model_path, result_path)


# # Параметры для запуска в Jupyter Notebook
# data_path = 'test.parquet'
# model_path = 'spark_ml_model'
# result_path = 'result'
#
# # Запуск основного процесса
# main(data_path, model_path, result_path)