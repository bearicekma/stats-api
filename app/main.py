from fastapi import FastAPI
from datetime import datetime

app = FastAPI(title="Stats API")

@app.get("/")
def root():
    return {"status": "ok", "timestamp": str(datetime.now())}

@app.get("/stats/sample")
def get_sample():
    """動作確認用のサンプルデータ"""
    return {
        "source": "sample",
        "updated_at": str(datetime.now()),
        "data": [
            {"year": 2022, "value": 125.1},
            {"year": 2023, "value": 127.8},
            {"year": 2024, "value": 130.2},
        ]
    }
