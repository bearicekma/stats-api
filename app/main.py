
from fastapi   import FastAPI, BackgroundTasks
from datetime  import datetime
from app.database  import get_stats          # Firestoreからデータ取得
from app.collector import run_all_collections # 収集ジョブ

app = FastAPI(title="Stats API")

@app.get("/")
def root():
    return {"status": "ok", "timestamp": str(datetime.now())}

@app.get("/stats/sample")
def get_sample():
    """動作確認用サンプル（ステップ2から継続）"""
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
    """
    Firestoreから指定コレクションのデータを返す

    collection_name: URLの一部がそのままパラメータになる
    例: /stats/population → collection_name="population"
    """
    data = get_stats(collection_name)
    return {
        "collection":  collection_name,
        "updated_at":  str(datetime.now()),
        "count":       len(data),   # 件数
        "data":        data
    }

@app.post("/collect")
async def trigger_collection(background_tasks: BackgroundTasks):
    """
    データ収集を手動トリガーするエンドポイント
    Cloud Schedulerからも呼び出される

    BackgroundTasks: レスポンスを返した後にバックグラウンドで処理を続ける
    すぐに{"message":"開始しました"}を返しつつ収集処理が裏で走る
    """
    background_tasks.add_task(run_all_collections)
    return {"message": "収集を開始しました", "timestamp": str(datetime.now())}
