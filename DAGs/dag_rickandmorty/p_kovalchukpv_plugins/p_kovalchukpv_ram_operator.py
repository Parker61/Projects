from airflow.models import BaseOperator  # Импортируем BaseOperator для создания пользовательского оператора
from p_kovalchukpv_plugins.p_kovalchukpv_api_hook import PavelRickAndMortyApiHook  # Импортируем ранее созданный Hook для взаимодействия с API
import logging  # Импортируем модуль логирования

class PavelRamRickAndMortytOperator(BaseOperator):
    """
    Подсчитывает количество живых или мертвых персонажей с использованием PavelRickAndMortyApiHook.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)  # Инициализируем базовый класс BaseOperator

    def execute(self, context):
        """
        Выполняет оператор.
        """
        hook = PavelRickAndMortyApiHook()  # Создаем экземпляр Hook для взаимодействия с API
        locations = hook.get_location_results()  # Получаем результаты запроса к API
        list_raw = []  # Создаем пустой список для хранения данных локаций
        for loc in locations:  # Проходим по всем локациям
            dict_loc = {  # Создаем словарь с нужными полями
                'id': loc['id'],
                'name': loc['name'],
                'type': loc['type'],
                'dimension': loc['dimension'],
                'resident_cnt': len(loc['residents'])  # Подсчитываем количество резидентов
            }
            list_raw.append(dict_loc)  # Добавляем словарь в список
        sorted_list = sorted(list_raw, key=lambda x: x['resident_cnt'], reverse=True)
        # Сортируем список по количеству резидентов в порядке убывания
        list_top_3 = sorted_list[:3]  # Выбираем топ-3 локации
        context['ti'].xcom_push(key='top_3', value=list_top_3)  # Передаем список топ-3 локаций через XCom
        for loc in list_top_3:  # Логируем информацию о топ-3 локациях
            logging.info(f'id: {loc["id"]}, Локация: {loc["name"]}, Количество резидентов: {loc["resident_cnt"]}')
