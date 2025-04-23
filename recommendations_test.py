from fastapi import FastAPI
from contextlib import asynccontextmanager
import logging
import pandas as pd
import yaml

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("uvicorn.error")

# Загрузка параметров
with open('params.yaml', 'r') as fd:
    params = yaml.safe_load(fd)

class Recommendations:
    def __init__(self):
        self._recs = {"personal": None, "default": None}

    def load(self, type, path):
        logger.info(f"Loading recommendations, type: {type}")
        self._recs[type] = pd.read_parquet(path)
        if type == "personal":
            self._recs[type] = self._recs[type].set_index("user_id")
        logger.info("Loaded")
        logger.info(self._recs[type].head(1))

    def get(self, user_id, k=params['k']):
        try:
            recs = self._recs["personal"].loc[user_id, "item_id"].to_list()[:int(k)]
        except (KeyError, IndexError):
            recs = self._recs["default"]["item_id"].to_list()[:int(k)]

        # Логирование названий треков и артистов
        if recs:
            for item_id in recs:
                track_name = items.query("item_id == @item_id")["track_name"].iat[0]
                artist_name = items.query("item_id == @item_id")["artists_names"].iat[0]
                logger.info(f"Track: {track_name}, Artist: {artist_name}")

        return recs

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting")

    rec_store.load(type="personal", path=params['personal_als_path'])
    rec_store.load(type="default", path=params['top_popular_path'])

    yield
    logger.info("Stopping")

app = FastAPI(title="FastAPI Recommendation Service", lifespan=lifespan)
rec_store = Recommendations()

# Загрузка данных о треках
items = pd.read_parquet(params['items_path'])
logger.info('Items loaded')
logger.info(items.head(1))

@app.post("/recommendations")
async def recommendations(user_id: int, k: int = params['k']):
    recs = rec_store.get(user_id, k)
    return {"recs": recs}

@app.get("/load_recommendations")
async def load_recommendations(rec_type: str, file_path: str):
    rec_store.load(type=rec_type, path=file_path)
    return {"status": "loaded"}

@app.get("/get_statistics")
async def get_statistics():
    return {"message": "Statistics function not implemented in this version."}