
from fastapi   import FastAPI, BackgroundTasks
from datetime  import datetime
from app.database  import get_stats
from app.collector import run_all_collections

app = FastAPI(title="Stats API")

@app.get("/")
def root():
    return {"status": "ok", "timestamp": str(datetime.now())}

@app.get("/stats/sample")
def get_sample():
    """動作確認用サンプル"""
    return {
        "source": "sample",
        "updated_at": str(datetime.now()),
        "data": [
            {"year": 2022, "value": 125.1},
            {"year": 2023, "value": 127.8},
            {"year": 2024, "value": 130.2},
        ]
    }

@app.get("/stats/{collection_name}")
def get_collection(collection_name: str):
    data = get_stats(collection_name)
    return {
        "collection":  collection_name,
        "updated_at":  str(datetime.now()),
        "count":       len(data),
        "data":        data
    }

@app.post("/collect")
async def trigger_collection(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_all_collections)
    return {"message": "収集を開始しました", "timestamp": str(datetime.now())}
