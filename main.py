
# Stats API メインファイル
# ルーターの登録と共通エンドポイントのみを記載する

from fastapi       import FastAPI, BackgroundTasks
from datetime      import datetime
from app.database  import get_stats
from app.collector import run_all_collections
from app.routers   import estat, boj

app = FastAPI(title="Stats API")

# ルーターを登録する
app.include_router(estat.router)
app.include_router(boj.router)


@app.get("/")
def root():
    # 死活確認エンドポイント
    return {"status": "ok", "timestamp": str(datetime.now())}


@app.get("/master/{collection_name}")
def get_master(collection_name: str):
    # GCSのmaster/プレフィックス配下のParquetをDuckDBで読み込んで返す
    data = get_stats(collection_name, category="master")
    return {
        "collection": collection_name,
        "updated_at": str(datetime.now()),
        "count":      len(data),
        "data":       data
    }


@app.get("/stats/{collection_name}")
def get_collection(collection_name: str):
    # GCSに保存されたParquetファイルをDuckDBで読み込んで返す
    data = get_stats(collection_name)
    return {
        "collection": collection_name,
        "updated_at": str(datetime.now()),
        "count":      len(data),
        "data":       data
    }


@app.post("/collect")
async def trigger_collection(background_tasks: BackgroundTasks):
    # データ収集をバックグラウンドでトリガーする（Cloud Schedulerから呼ばれる）
    background_tasks.add_task(run_all_collections)
    return {"message": "収集を開始しました", "timestamp": str(datetime.now())}
