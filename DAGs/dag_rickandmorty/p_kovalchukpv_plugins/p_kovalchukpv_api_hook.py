from airflow.providers.http.hooks.http import HttpHook  # Импортируем HttpHook для взаимодействия с HTTP API

class PavelRickAndMortyApiHook(HttpHook):
    """
    Hook для взаимодействия с Rick and Morty API.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(http_conn_id=None, **kwargs)  # Инициализация базового класса HttpHook
        self.method = 'GET'  # Устанавливаем метод HTTP запроса на 'GET'
        self.url = 'https://rickandmortyapi.com/api/location'  # Устанавливаем URL для доступа к API Rick and Morty

    def get_location_results(self):
        """
        Получает результаты запроса к API.
        """
        response = self.run(self.url)  # Выполняем HTTP запрос к API по указанному URL
        if response.status_code == 200:  # Проверяем, успешен ли запрос (статус 200)
            data = response.json()  # Преобразуем ответ в формат JSON
            results = data.get('results')  # Извлекаем данные из поля 'results'
            return results  # Возвращаем результаты запроса
        else:
            raise Exception(f"Failed to fetch location results. Status code: {response.status_code}")  # Бросаем исключение в случае ошибки запроса
