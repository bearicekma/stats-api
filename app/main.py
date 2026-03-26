from fastapi       import FastAPI, BackgroundTasks
from datetime      import datetime
from app.database  import get_stats
from app.collector import run_all_collections
import httpx
import os

app = FastAPI(title="Stats API")

# e-Stat APIの1回あたりの最大取得件数
ESTAT_LIMIT = 100000

@app.get("/")
def root():
    return {"status": "ok", "timestamp": str(datetime.now())}

@app.get("/stats/sample")
def get_sample():
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
    """Firestoreから保存済みデータを返す（保存方式）"""
    data = get_stats(collection_name)
    return {
        "collection": collection_name,
        "updated_at": str(datetime.now()),
        "count":      len(data),
        "data":       data
    }

@app.get("/passthrough/{stats_data_id}")
async def passthrough(stats_data_id: str):
    """
    e-Stat APIをページネーションで全件取得してそのまま返す
    Claude・Firestoreは使わないので費用ゼロ

    stats_data_id : e-Statの統計表ID
    例: /passthrough/0003448237
    """
    app_id         = os.environ["ESTAT_APP_ID"]
    all_values     = []   # 全ページのデータを格納するリスト
    start_position = 1    # 取得開始位置（1始まり）

    async with httpx.AsyncClient() as client:
        while True:
            # e-Stat APIを呼び出す
            response = await client.get(
                "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData",
                params={
                    "appId":         app_id,
                    "statsDataId":   stats_data_id,
                    "lang":          "J",
                    "limit":         ESTAT_LIMIT,    # 1回あたり最大10万件
                    "startPosition": start_position, # 取得開始位置
                },
                timeout=60  # データが多い場合は時間がかかるので60秒に設定
            )
            response.raise_for_status()
            raw_json = response.json()

            # 総件数と今回の終了位置を取得する
            result_inf   = raw_json["GET_STATS_DATA"]["STATISTICAL_DATA"]["RESULT_INF"]
            total_number = int(result_inf["TOTAL_NUMBER"])  # 総件数
            to_number    = int(result_inf["TO_NUMBER"])     # 今回の終了位置

            # 今回取得したデータをリストに追加する
            values = raw_json["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["VALUE"]
            all_values.extend(values)

            # 全件取得完了したらループを抜ける
            if to_number >= total_number:
                break

            # 次のページの開始位置を設定する
            start_position = to_number + 1

    return {
        "stats_data_id": stats_data_id,
        "fetched_at":    str(datetime.now()),
        "count":         len(all_values),  # 取得した総件数
        "data":          all_values        # 全データ
    }

@app.post("/collect")
async def trigger_collection(background_tasks: BackgroundTasks):
    """データ収集を手動トリガーする（保存方式用）"""
    background_tasks.add_task(run_all_collections)
    return {"message": "収集を開始しました", "timestamp": str(datetime.now())}
