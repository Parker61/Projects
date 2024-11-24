from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
import datetime

# Определение глобальных  аргументов по умолчанию для всех задач в DAG
DEFAULT_ARGS = {
    'owner': 'p-kovalchuk',  # Владелец задач
    'poke_interval': 600,  # Интервал проверки состояния задачи
    'email': ['kovalchukpv61@gmail.com'],  # Email для уведомлений
    'retries': 3,  # Количество попыток выполнения задачи при сбое
    'retry_delay': datetime.timedelta(seconds=5),  # Задержка перед повторной попыткой
    'sla': datetime.timedelta(hours=2),  # Срок службы (Service Level Agreement)
    'execution_delay': datetime.timedelta(seconds=300),  # Задержка выполнения задачи
    'trigger_rules': 'all_success'  # Правила триггера выполнения задач
}


# Функция для вывода текущей даты и времени
def func_display_date(**kwargs):
    """
    Функция выводит текущую дату и время, полученную из контекста выполнения задачи.

    :param kwargs: Контекст выполнения задачи, включая переменную 'ds' с датой выполнения.
    """
    date_now = kwargs['ds']  # Получаем дату выполнения задачи из контекста
    print(f'{date_now}')  # Выводим дату и время


# Определение DAG (Directed Acyclic Graph)
with DAG(
        'display_date_dag',  # Имя DAG
        tags=['p-kovalchuk'],  # Теги для идентификации задач
        default_args=DEFAULT_ARGS,  # Глобальные аргументы по умолчанию
        schedule_interval='@daily',  # Интервал запуска DAG
        start_date=datetime.datetime(2024, 5, 9),  # Дата начала выполнения DAG
        catchup=False  # Не запускать задачи за пропущенные периоды до start_date
) as dag:
    # Задача DummyOperator как начало и конец DAG
    start = DummyOperator(
        task_id='start',  # ID задачи
        dag=dag  # Связь задачи с DAG
    )

    # Задача BashOperator  для вывода даты выполнения   задачи
    bash_task = BashOperator(
        task_id='bash_task',  # ID задачи
        bash_command='echo "{{ ds }}"',  # Команда для вывода даты
        dag=dag  # Связь задачи с DAG
    )

    # Задача PythonOperator для вывода  даты выполнения задачи
    python_task = PythonOperator(
        task_id='python_task',  # ID задачи
        python_callable=func_display_date,  # Функция для вызова
        provide_context=True,  # Предоставление контекста выполнения задачи
        dag=dag  # Связь задачи с DAG
    )

    # Задача DummyOperator как конец DAG
    end = DummyOperator(
        task_id='end',  # ID задачи
        dag=dag  # Связь задачи с DAG
    )

    # Установка порядка выполнения задач
    start >> bash_task >> python_task >> end
