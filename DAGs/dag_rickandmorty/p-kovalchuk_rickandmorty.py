"""> Задание
Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location" с полями id, name, type, dimension, resident_cnt.
! Обратите внимание: ваш логин в LMS нужно использовать, заменив дефис на нижнее подчёркивание
С помощью API (https://rickandmortyapi.com/documentation/#location) найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
Запишите значения соответствующих полей этих трёх локаций в таблицу. resident_cnt — длина списка в поле residents.
> hint
* Для работы с GreenPlum используется соединение 'conn_greenplum_write' в случае, если вы работаете с LMS либо настроить соединение самостоятельно в вашем личном Airflow. Параметры соединения:

Host: greenplum.lab.karpov.courses
Port: 6432
DataBase: students (не karpovcourses!!!)
Login: student
Password: Wrhy96_09iPcreqAS

* Не забудьте, используя свой логин в  LMS, заменить дефис на нижнее подчёркивание p_kovalchuk

* Можно использовать хук PostgresHook, можно оператор PostgresOperator

* Предпочтительно использовать  написанный вами оператор для вычисления top-3 локаций из API

* Можно использовать XCom для передачи значений  между тасками, можно сразу записывать нужное значение в таблицу

* Не забудьте обработать повторный запуск каждого таска: предотвратите повторное создание таблицы, позаботьтесь об отсутствии в ней дублей

"""
import logging
from airflow import DAG  # Импортируем класс DAG из Airflow для определения DAG'а
from airflow.utils.dates import days_ago  # Импортируем функцию days_ago для установки начальной даты DAG'а
from airflow.operators.dummy import \
    DummyOperator  # Импортируем DummyOperator, который используется как заглушка в начале и конце DAG'а
from airflow.providers.postgres.hooks.postgres import \
    PostgresHook  # Импортируем PostgresHook для взаимодействия с базой данных PostgreSQL
from airflow.operators.python import \
    PythonOperator  # Импортируем PythonOperator для выполнения Python-функций в задачах DAG'а
from p_kovalchukpv_plugins.p_kovalchukpv_ram_operator import \
    PavelRamRickAndMortytOperator  # Импортируем пользовательский оператор для получения данных из API

# Настройка логгера
logger = logging.getLogger(__name__)  # Создаем логгер с именем текущего модуля
logger.setLevel(logging.INFO)  # Устанавливаем уровень логирования на INFO

handler = logging.StreamHandler()  # Создаем обработчик логирования для вывода логов в консоль
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')  # Определяем формат логирования
handler.setFormatter(formatter)  # Применяем формат к обработчику
logger.addHandler(handler)  # Добавляем обработчик к логгеру

# Параметры по умолчанию для DAG
DEFAULT_ARGS = {
    'start_date': days_ago(2),  # Начальная дата запуска DAG'а
    'owner': 'p_kovalchuk',  # Владелец DAG'а
    'poke_interval': 600  # Интервал повторного запуска задач в секундах
}


# Функция для создания таблицы в базе данных
def create_table(**kwargs):
    pg_hook = PostgresHook(
        postgres_conn_id='conn_greenplum_write')  # Создаем объект PostgresHook для соединения с базой данных
    sql_statement = """
    CREATE TABLE IF NOT EXISTS p_kovalchuk_ram_location (
        id TEXT,
        name varchar(100) NULL,
        type varchar(100) NULL,
        dimension varchar(100) NULL,
        resident_cnt int4 NULL
    )
    DISTRIBUTED BY (id);
    """  # SQL-запрос для создания таблицы
    try:
        pg_hook.run(sql_statement, False)  # Выполняем SQL-запрос для создания таблицы
        logger.info('Таблица p_kovalchuk_ram_location успешно создана.')  # Логируем успешное создание таблицы
    except Exception as e:
        logger.error(f'Ошибка при создании таблицы: {e}')  # Логируем ошибку при создании таблицы


# Функция для вставки данных в таблицу
def insert_table(**kwargs):
    list_top_3 = kwargs['ti'].xcom_pull(key='top_3', task_ids='resident_cnt')  # Получаем данные из XCom
    if not list_top_3:
        raise ValueError('No data found in XCom for key "top_3"')  # Проверяем, есть ли данные в XCom

    pg_hook = PostgresHook(
        postgres_conn_id='conn_greenplum_write')  # Создаем объект PostgresHook для соединения с базой данных

    for loc in list_top_3:  # Проходимся по списку локаций
        id = loc['id']
        name = loc['name'].replace("'", "''")  # Экранируем одиночные кавычки в строке
        type = loc['type'].replace("'", "''")
        dimension = loc['dimension'].replace("'", "''")
        resident_cnt = loc['resident_cnt']

        delete_sql = f"DELETE FROM p_kovalchuk_ram_location WHERE id = '{id}'"  # SQL-запрос для удаления строки с данным id
        insert_sql = f"""
        INSERT INTO p_kovalchuk_ram_location (id, name, type, dimension, resident_cnt)
        VALUES ('{id}', '{name}', '{type}', '{dimension}', {resident_cnt})
        """  # SQL-запрос для вставки данных

        try:
            pg_hook.run(delete_sql)  # Выполняем SQL-запрос для удаления
            pg_hook.run(insert_sql)  # Выполняем SQL-запрос для вставки
            logger.info(
                f"Данные для id {id} успешно вставлены в таблицу p_kovalchuk_ram_location.")  # Логируем успешную вставку данных
        except Exception as e:
            logger.error(f"Ошибка при вставке данных для id {id}: {e}")  # Логируем ошибку при вставке данных


# Определяем DAG
with DAG(
        "pavel_rickandmorty",  # Имя DAG'а
        schedule_interval='@daily',  # Расписание DAG'а
        default_args=DEFAULT_ARGS,  # Параметры по умолчанию для DAG'а
        tags=['p-kovalchuk']  # Теги для DAG'а
) as dag:
    start = DummyOperator(task_id='start')  # Задача-заглушка для начала DAG'а

    create_table_task = PythonOperator(
        task_id='create_table',  # Имя задачи
        python_callable=create_table,  # Функция для выполнения
        provide_context=True,  # Передаем контекст выполнения
        dag=dag  # Указываем DAG, к которому относится задача
    )

    resident_cnt = PavelRamRickAndMortytOperator(
        task_id='resident_cnt',  # Имя задачи
        dag=dag  # Указываем DAG, к которому относится задача
    )

    insert_table_task = PythonOperator(
        task_id='insert_table',  # Имя задачи
        python_callable=insert_table,  # Функция для выполнения
        provide_context=True,  # Передаем контекст выполнения
        dag=dag  # Указываем DAG, к которому относится задача
    )

    end = DummyOperator(task_id='end')  # Задача-заглушка для окончания DAG'а

    start >> create_table_task >> resident_cnt >> insert_table_task >> end  # Определяем последовательность выполнения задач
