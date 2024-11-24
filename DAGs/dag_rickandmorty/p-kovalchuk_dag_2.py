from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
import datetime
from airflow.hooks.postgres_hook import PostgresHook
import logging

# Импорт класса PostgresHook из библиотеки Airflow для работы с PostgreSQL.

# Определение глобальных аргументов по умолчанию для всех задач в DAG
DEFAULT_ARGS = {
    'owner': 'p-kovalchuk',  # Владелец задач
    'poke_interval': 600,  # Интервал проверки состояния задачи
    'email': ['kovalchukpv61@gmail.com'],  # Email для уведомлений
    'retries': 3,  # Количество попыток выполнения задачи при сбое
    'retry_delay': datetime.timedelta(seconds=5),  # Задержка перед повторной попыткой
    'sla': datetime.timedelta(hours=2),  # Срок службы (Service Level Agreement)
    'execution_delay': datetime.timedelta(seconds=300),  # Задержка выполнения задачи
    'trigger_rules': 'all_success'  # Правила триггера  выполнения задач
}


# Функция для извлечения заголовка статьи из базы данных
def extract_article_heading(**kwargs):
    # Получаем дату выполнения задачи из контекста
    execution_date = kwargs['execution_date']
    # Получаем день недели (понедельник=1, вторник=2, ..., суббота=6)
    day_of_week = execution_date.isoweekday()
    # Инициализируем хук для работы с PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    # Получаем соединение с базой данных
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    # Формируем и выполняем запрос к базе данных для получения заголовка статьи по дню недели
    cursor.execute('SELECT heading FROM articles WHERE id = %s', (day_of_week,))
    # Получаем результат запроса
    query_result = cursor.fetchone()
    if query_result:
        article_heading = query_result[0]
        # Выводим заголовок статьи в логи
        logging.info(f"Заголовок статьи для {execution_date}: {article_heading}")
        # Возвращаем заголовок статьи для передачи в XCom
        if article_heading is not None:
            kwargs['ti'].xcom_push(key='article_heading', value=article_heading)
        return article_heading
    else:
        logging.warning(f"Для {execution_date} нет статьи в базе данных.")
        return None


# Функция для определения ветвления
def choose_branch(execution_date, **kwargs):
    day_of_week = execution_date.isoweekday()
    if day_of_week == 7:  # Воскресенье
        return 'skip_task'
    else:
        return 'python_task'


# Определение  DAG (Directed Acyclic Graph)
with DAG(
        'display_articles_dag',  # Имя DAG
        tags=['p-kovalchuk'],  # Теги для идентификации задач
        default_args=DEFAULT_ARGS,  # Глобальные аргументы по умолчанию
        schedule_interval='@daily',  # Интервал запуска DAG
        start_date=datetime.datetime(2022, 3, 1),  # Дата начала выполнения DAG
        end_date=datetime.datetime(2022, 3, 14),  # Дата окончания выполнения DAG
        catchup=True  # Не  запускать задачи за пропущенные периоды до start_date
) as dag:
    # Задача DummyOperator как начало и конец DAG
    start = DummyOperator(
        task_id='start',  # ID задачи
        dag=dag  # Связь задачи с DAG
    )

    # Определение ветвления на основе дня недели
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=choose_branch,
        provide_context=True,
        dag=dag
    )

    # Задача DummyOperator для пропуска задач по воскресеньям
    skip_task = DummyOperator(
        task_id='skip_task',
        dag=dag
    )

    # Задача PythonOperator для вывода заголовка статьи
    python_task = PythonOperator(
        task_id='python_task',  # ID задачи
        python_callable=extract_article_heading,  # Функция для вызова
        provide_context=True,  # Предоставление контекста выполнения задачи
        dag=dag  # Связь задачи с DAG
    )

    # Задача DummyOperator как конец DAG
    end = DummyOperator(
        task_id='end',  # ID задачи
        dag=dag  # Связь задачи с DAG
    )

    # Установка порядка выполнения задач
    start >> branch_task
    branch_task >> skip_task
    branch_task >> python_task >> end
