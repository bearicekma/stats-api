from fastapi       import FastAPI, BackgroundTasks, Request
from datetime      import datetime
from app.database  import get_stats
from app.collector import run_all_collections
import httpx
import os

app = FastAPI(title="Stats API")

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

# Firestoreから保存済みデータを返す（保存方式）
@app.get("/stats/{collection_name}")
def get_collection(collection_name: str):
    data = get_stats(collection_name)
    return {
        "collection": collection_name,
        "updated_at": str(datetime.now()),
        "count":      len(data),
        "data":       data
    }

# class_infoからコード→名称の変換辞書を作成する
def build_code_to_name_map(class_info: list) -> dict:
    code_map = {}

    for class_obj in class_info:
        class_id   = class_obj["@id"]
        class_name = class_obj["@name"]

        classes = class_obj.get("CLASS", [])

        if isinstance(classes, dict):
            classes = [classes]

        code_map[class_id] = {
            "label": class_name,
            "codes": {c["@code"]: c["@name"] for c in classes}
        }

    return code_map

# 1行分のデータのコードを名称に変換する
# 変換前: {"@cat01": "002", "@area": "00000", "$": "1250000"}
# 変換後: {"分類": "総合", "地域": "全国", "値": 1250000}
def convert_row(row: dict, code_map: dict) -> dict:
    converted = {}

    for key, value in row.items():
        if key == "$":
            try:
                converted["値"] = float(value) if "." in str(value) else int(value)
            except (ValueError, TypeError):
                converted["値"] = value

        elif key.startswith("@"):
            field_id = key[1:]

            if field_id in code_map:
                col_name = code_map[field_id]["label"]
                codes    = code_map[field_id]["codes"]
                converted[col_name] = codes.get(value, value)
            else:
                converted[field_id] = value

    return converted

# e-Stat APIをページネーションで全件取得しコードを名称に変換して返す
# URLのクエリパラメータをそのままe-Stat APIに渡す汎用設計
# 例: /estat/pass/0003427113?cdArea=00000,13A01,20A01&cdTimeFrom=2024000000
# 例: /estat/pass/0003427113?cdArea=00000&cdCat01=001&cdTimeTo=2024999999
@app.get("/estat/pass/{stats_data_id}")
async def estat_pass(stats_data_id: str, request: Request):
    app_id         = os.environ["ESTAT_APP_ID"]
    all_values     = []
    start_position = 1
    class_info     = None
    total_number   = 0

    # URLのクエリパラメータを全て取得する
    # 例: ?cdArea=00000&cdTimeFrom=2020000000 → {"cdArea": "00000", "cdTimeFrom": "2020000000"}
    query_params = dict(request.query_params)

    # e-Stat APIの固定パラメータを設定する
    params = {
        "appId":         app_id,
        "statsDataId":   stats_data_id,
        "lang":          "J",
        "limit":         ESTAT_LIMIT,
        "startPosition": start_position,
    }

    # URLで指定された全クエリパラメータをそのままe-Stat APIに追加する
    # e-Stat APIのパラメータ名（cdArea, cdTimeFrom など）をそのまま使う
    params.update(query_params)

    async with httpx.AsyncClient() as client:
        while True:
            params["startPosition"] = start_position

            response = await client.get(
                "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData",
                params=params,
                timeout=60
            )
            response.raise_for_status()
            raw_json = response.json()

            statistical_data = raw_json["GET_STATS_DATA"]["STATISTICAL_DATA"]
            result_inf       = statistical_data["RESULT_INF"]
            total_number     = int(result_inf["TOTAL_NUMBER"])
            to_number        = int(result_inf["TO_NUMBER"])

            if class_info is None:
                class_info = statistical_data["CLASS_INF"]["CLASS_OBJ"]

            values = statistical_data["DATA_INF"]["VALUE"]
            all_values.extend(values)

            if to_number >= total_number:
                break

            start_position = to_number + 1

    code_map       = build_code_to_name_map(class_info)
    converted_data = [convert_row(row, code_map) for row in all_values]

    return {
        "stats_data_id": stats_data_id,
        "fetched_at":    str(datetime.now()),
        "total_number":  total_number,
        "count":         len(converted_data),
        "data":          converted_data
    }

# データ収集を手動トリガーする（保存方式用）
@app.post("/collect")
async def trigger_collection(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_all_collections)
    return {"message": "収集を開始しました", "timestamp": str(datetime.now())}
