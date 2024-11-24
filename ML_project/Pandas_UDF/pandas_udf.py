import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType


# Определяем пользовательскую функцию для маскировки номеров карт
@pandas_udf(returnType=StringType())  # Используем декоратор pandas_udf для создания UDF,
# которая принимает pandas Series и возвращает pandas Series. Указываем тип возвращаемого значения и тип UDF.
def card_number_mask(s: pd.Series) -> pd.Series:
    # Применяем метод slice_replace к каждому элементу серии, начиная с 4-го символа и заканчивая 12-м,
    # заменяя этот участок на "XXXXXXXX". Это позволяет маскировать номер карты, сохраняя первые 4 и последние 4 символа.
    s = s.str.slice_replace(start=4, stop=12, repl="XXXXXXXX")
    return s


# Основная часть скрипта
if __name__ == "__main__":
    # Создаем экземпляр SparkSession, который является основной точкой взаимодействия с Spark
    spark = SparkSession.builder.appName('PySparkUDF').getOrCreate()

    # Создаем DataFrame с двумя столбцами: "id" и "card_number"
    df = spark.createDataFrame([(1, "4042654376478743"), (2, "4042652276478747")], ["id", "card_number"])

    # Выводим содержимое DataFrame
    df.show()

    # Применяем нашу пользовательскую функцию card_number_mask к столбцу "card_number",
    # добавляем результат в новый столбец "hidden"
    dfr = df.withColumn("hidden", card_number_mask("card_number"))

    # Выводим итоговый DataFrame с примененной маскировкой номеров карт
    dfr.show(truncate=False)
