import requests
import os
from dotenv import load_dotenv
import logging

# Настройка логирования
logging.basicConfig(
    filename="test_service.log",
    filemode="a",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)

# Загрузка переменных окружения
load_dotenv()
url = os.getenv("API_URL", "http://127.0.0.1:8000")  # URL сервиса по умолчанию

# Проверка рабочей директории и её установка
current_directory = os.getcwd()
expected_directory = "/home/mle-user/mle-project-sprint-4-v001/mle-project-sprint-4-v001"
if current_directory != expected_directory:
    logging.info(f"Changing working directory from {current_directory} to {expected_directory}")
    os.chdir(expected_directory)

# Универсальная функция для запросов
def make_request(method, endpoint, params=None, json=None):
    full_url = url + endpoint
    headers = {"Content-type": "application/json", "Accept": "text/plain"}
    try:
        logging.info(f"Making request to {full_url} with params: {params} and json: {json}")
        resp = requests.request(method, full_url, headers=headers, params=params, json=json)
        resp.raise_for_status()  # Проверка статуса ответа
        return resp.json()  # Возвращаем JSON-данные, если запрос успешен
    except requests.exceptions.RequestException as e:
        logging.error(f"Request to {endpoint} failed: {e}")
        return None

# Тестирование персональных рекомендаций
logging.info("Testing personal recommendations")
params = {"user_id": 0}
recs = make_request("POST", "/recommendations", params=params)
logging.info(f"Recommendations ID: {recs}")

# Тестирование рекомендаций по популярности
logging.info("Testing popular recommendations")
params = {"user_id": 3000000000000}
recs = make_request("POST", "/recommendations", params=params)
logging.info(f"Recommendations ID: {recs}")

# Запрос на загрузку обновленных файлов рекомендаций
logging.info("Loading updated recommendations files")
params = {"rec_type": "default", "file_path": "/home/mle-user/mle-project-sprint-4-v001/mle-project-sprint-4-v001/recsys/recommendations/top_popular.parquet"}
response = make_request("GET", "/load_recommendations", params=params)
logging.info(f"Response: {response}")

# Запрос на вывод статистики
logging.info("Requesting statistics")
stats = make_request("GET", "/get_statistics")
logging.info(f"Statistics: {stats}")

# Тестирование запросов для тестового пользователя
test_user_id = 11  # Тестовый пользователь
logging.info(f"Testing user: {test_user_id}")

# Получение событий для тестового пользователя
params = {"user_id": test_user_id, "k": 10}
events = make_request("POST", "/get_user_events", params=params)
logging.info(f"User events: {events}")

# Добавление событий для тестового пользователя
logging.info("Adding events for testing user")
for i in range(1, 6):
    params = {"user_id": test_user_id, "item_id": i}
    response = make_request("POST", "/put_user_event", params=params)
    logging.info(f"Response for event {i}: {response}")

# Проверка генерации онлайн-рекомендаций
params = {"user_id": test_user_id, "k": 10, "N": 10}
online_recs = make_request("POST", "/get_online_u2i", params=params)
logging.info(f"Online recommendations: {online_recs}")

# Итоговые смешанные рекомендации для тестового пользователя
params = {"user_id": test_user_id}
final_recs = make_request("POST", "/recommendations", params=params)
logging.info(f"Final recommendations: {final_recs}")

print("Тестовый скрипт завершил работу")
