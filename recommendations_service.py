from fastapi import FastAPI, Query
from contextlib import asynccontextmanager
import logging
import pandas as pd
import yaml
import pickle
from implicit.als import AlternatingLeastSquares
import os

# Настройка логирования
global logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("uvicorn.error")

# Загрузка параметров
global params
with open('params.yaml', 'r') as fd:
    params = yaml.safe_load(fd)
logger.info(f"Top popular path: {params['top_popular_path']}")

class Recommendations:
    def __init__(self):
        self._recs = {"personal": None, "default": None}
        self._stats = {
            "request_personal_count": 0,
            "request_default_count": 0,
        }

    def load(self, type, path):
        logger.info(f"Loading recommendations, type: {type}")
        try:
            if not os.path.exists(path):
                logger.error(f"File {path} not found! Current working directory: {os.getcwd()}")
                return
            # Если файл найден, продолжаем загрузку
            self._recs[type] = pd.read_parquet(path)
            if type == "personal":
                self._recs[type] = self._recs[type].set_index("user_id")
                logger.info("Loaded")
                logger.info(self._recs[type].head(1))
        except Exception as e:
            logger.error(f"Failed to load recommendations from {path}: {e}")

    def get(self, user_id, k=params['k']):
        try:
            recs = self._recs["personal"].loc[user_id, "item_id"].astype(int).to_list()[:int(k)]
            self._stats["request_personal_count"] += 1
            logger.info(f"Found {len(recs)} personal recommendations!")
        except (KeyError, IndexError):
            recs = self._recs["default"]["item_id"].astype(int).to_list()[:int(k)]
            self._stats["request_default_count"] += 1
            logger.info(f"Found {len(recs)} default recommendations!")

        if recs:
            for item_id in recs:
                track_name = items.query("item_id == @item_id")["track_name"].iat[0]
                artist_name = items.query("item_id == @item_id")["artists_names"].iat[0]
                logger.info(f"Track: {track_name}, Artist: {artist_name}")

        return recs

    def stats(self):
        logger.info("Stats for recommendations")
        for name, value in self._stats.items():
            logger.info(f"{name:<30} {value} ")
        return self._stats

class EventStore:
    def __init__(self, max_events_per_user=10):
        self.events = {}
        self.max_events_per_user = max_events_per_user

    def put(self, user_id, item_id):
        user_events = self.events.get(user_id, [])
        self.events[user_id] = [item_id] + user_events[:self.max_events_per_user]
        logger.info(f"Added event: User {user_id}, Item {item_id}")

    def get(self, user_id, k):
        user_events = self.events.get(user_id, [])
        logger.info(f"Retrieved {len(user_events)} events for user {user_id}")
        return user_events[:k]

# Асинхронная функция для получения рекомендаций похожих товаров
async def get_als_i2i(item_id, items, N=1):
    similar_items = als_model.similar_items(item_id, N=N)
    similar_tracks_enc = similar_items[0].tolist()[1:N+1]
    similar_tracks_scores = similar_items[1].tolist()[1:N+1]

    similar_tracks = []
    for i in similar_tracks_enc:
        similar_tracks.append(items.query("item_id == @i")["item_id"].iat[0])

    return similar_tracks, similar_tracks_scores

# Функция для удаления дубликатов из списка
def dedup_ids(ids):
    seen = set()
    return [id for id in ids if not (id in seen or seen.add(id))]

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting")
    try:
        rec_store.load(type="personal", path=params['personal_als_path'])

        # Загрузка файла top_popular, если он существует
        if os.path.exists(params['top_popular_path']):
            rec_store.load(type="default", path=params['top_popular_path'])
        else:
            logger.error(f"File {params['top_popular_path']} not found!")

        yield
    except Exception as e:
        logger.error(f"Failed to load recommendations: {e}")
    finally:
        logger.info("Stopping")

# Инициализация FastAPI приложения
app = FastAPI(title="FastAPI Recommendation Service", lifespan=lifespan)
rec_store = Recommendations()
events_store = EventStore()

# Загрузка заранее сохранённых моделей и данных
with open(params['als_model_path'], 'rb') as f:
    als_model = pickle.load(f)
logger.info('ALS model loaded')

items = pd.read_parquet(params['items_path'])
logger.info('Items loaded')
logger.info(items.head(1))

# Эндпоинт для получения рекомендаций
@app.post("/recommendations")
async def recommendations(user_id: int, k: int = params['k']):
    recs_offline = rec_store.get(user_id, k)
    recs_online_response = await get_online_u2i(user_id, k, params['N'])
    recs_online = recs_online_response["recs"]

    # Преобразуем numpy.int64 в int для всех данных
    recs_offline = [int(item) for item in recs_offline]
    recs_online = [int(item) for item in recs_online]

    min_length = min(len(recs_offline), len(recs_online))
    recs_blended = []

    for i in range(min_length):
        recs_blended.append(recs_online[i])
        recs_blended.append(recs_offline[i])

    recs_blended.extend(recs_offline[min_length:])
    recs_blended.extend(recs_online[min_length:])
    recs_blended = dedup_ids(recs_blended)[:int(k)]

    return {"recs": recs_blended}

# Эндпоинт для добавления события для пользователя
@app.post("/put_user_event")
async def put_user_event(user_id: int, item_id: int):
    events_store.put(user_id, item_id)
    return {"result": "ok"}

# Эндпоинт для получения событий пользователя
@app.post("/get_user_events")
async def get_user_events(user_id: int, k: int = params['k']):
    events = events_store.get(user_id, k)
    return {"events": events}

# Эндпоинт для загрузки рекомендаций
@app.get("/load_recommendations")
async def load_recommendations(rec_type: str, file_path: str = Query(...)):
    absolute_path = os.path.abspath(file_path)  # Преобразуем путь в абсолютный
    logger.info(f"Resolved absolute path: {absolute_path}")

    if not os.path.exists(absolute_path):
        logger.error(f"File {absolute_path} not found!")
        return {"status": "error", "message": f"File {absolute_path} not found!"}

    rec_store.load(type=rec_type, path=absolute_path)
    return {"status": "loaded"}

# Эндпоинт для получения статистики
@app.get("/get_statistics")
async def get_statistics():
    return rec_store.stats()

# Эндпоинт для получения онлайн рекомендаций
@app.post("/get_online_u2i")
async def get_online_u2i(user_id: int, k: int, N: int):
    logger.info(f"Received data - user_id: {user_id}, k: {k}, N: {N}")

    events = events_store.get(user_id, k)
    sim_item_ids = []
    sim_track_scores = []
    if len(events) > 0:
        for item_id in events:
            sim_item_id, sim_track_score = await get_als_i2i(item_id, items, N=N)
            sim_item_ids.append(sim_item_id)
            sim_track_scores.append(sim_track_score)
        sim_item_ids = sum(sim_item_ids, [])
        sim_track_scores = sum(sim_track_scores, [])
    else:
        recs = []

    # Сортируем похожие объекты
    combined = list(zip(sim_item_ids, sim_track_scores))
    combined = sorted(combined, key=lambda x: x[1], reverse=True)
    combined = [int(item) for item, _ in combined]  # Преобразуем numpy.int64 в int

    recs = dedup_ids(combined)

    return {"recs": recs}
