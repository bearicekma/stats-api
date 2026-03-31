from fastapi           import FastAPI, BackgroundTasks, Request
from fastapi.responses import StreamingResponse
from datetime          import datetime
from app.database      import get_stats
from app.collector     import run_all_collections
import httpx
import json
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

@app.get("/stats/{collection_name}")
def get_collection(collection_name: str):
    data = get_stats(collection_name)
    return {
        "collection": collection_name,
        "updated_at": str(datetime.now()),
        "count":      len(data),
        "data":       data
    }

@app.get("/estat/meta/{stats_data_id}")
async def estat_meta(stats_data_id: str):
    app_id = os.environ["ESTAT_APP_ID"]

    async with httpx.AsyncClient() as client:
        meta_response = await client.get(
            "https://api.e-stat.go.jp/rest/3.0/app/json/getMetaInfo",
            params={"appId": app_id, "statsDataId": stats_data_id, "lang": "J"},
            timeout=30
        )
        meta_response.raise_for_status()

        count_response = await client.get(
            "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData",
            params={"appId": app_id, "statsDataId": stats_data_id, "lang": "J", "limit": 1, "startPosition": 1},
            timeout=30
        )
        count_response.raise_for_status()

    class_info = meta_response.json()["GET_META_INFO"]["METADATA_INF"]["CLASS_INF"]["CLASS_OBJ"]
    if isinstance(class_info, dict):
        class_info = [class_info]

    parameters = []
    for obj in class_info:
        classes = obj.get("CLASS", [])
        if isinstance(classes, dict):
            classes = [classes]
        parameters.append({
            "parameter": f"cd{obj['@id'].capitalize()}",
            "name":      obj["@name"],
            "count":     len(classes),
            "values":    [{"code": c["@code"], "name": c["@name"]} for c in classes]
        })

    total = int(count_response.json()["GET_STATS_DATA"]["STATISTICAL_DATA"]["RESULT_INF"]["TOTAL_NUMBER"])

    return {
        "stats_data_id": stats_data_id,
        "total_number":  f"{total:,} 件",
        "parameters":    parameters
    }

def build_code_to_name_map(class_info: list) -> dict:
    code_map = {}
    for class_obj in class_info:
        class_id   = class_obj["@id"]
        class_name = class_obj["@name"]
        classes    = class_obj.get("CLASS", [])
        if isinstance(classes, dict):
            classes = [classes]
        code_map[class_id] = {
            "label": class_name,
            "codes": {c["@code"]: c["@name"] for c in classes}
        }
    return code_map

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

@app.get("/estat/pass/{stats_data_id}")
async def estat_pass(stats_data_id: str, request: Request):
    app_id         = os.environ["ESTAT_APP_ID"]
    all_values     = []
    start_position = 1
    class_info     = None
    total_number   = 0

    params = {
        "appId":       app_id,
        "statsDataId": stats_data_id,
        "lang":        "J",
        "limit":       ESTAT_LIMIT,
    }
    params.update(dict(request.query_params))

    # e-Statから全件取得
    async with httpx.AsyncClient(timeout=300) as client:
        while True:
            params["startPosition"] = start_position

            response = await client.get(
                "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData",
                params=params,
            )
            response.raise_for_status()
            raw_json         = response.json()
            statistical_data = raw_json["GET_STATS_DATA"]["STATISTICAL_DATA"]
            result_inf       = statistical_data["RESULT_INF"]
            total_number     = int(result_inf["TOTAL_NUMBER"])
            to_number        = int(result_inf["TO_NUMBER"])

            if class_info is None:
                class_info = statistical_data["CLASS_INF"]["CLASS_OBJ"]

            all_values.extend(statistical_data["DATA_INF"]["VALUE"])

            if to_number >= total_number:
                break
            start_position = to_number + 1

    code_map       = build_code_to_name_map(class_info)
    converted_data = [convert_row(row, code_map) for row in all_values]
    fetched_at     = str(datetime.now())

    # StreamingResponseで逐次送信（32MB制限を回避）
    async def stream_json():
        yield (
            '{"stats_data_id":"' + stats_data_id + '",'
            '"fetched_at":"'     + fetched_at     + '",'
            '"total_number":'    + str(total_number) + ','
            '"count":'           + str(len(converted_data)) + ','
            '"data":['
        ).encode("utf-8")

        for i, row in enumerate(converted_data):
            prefix = b"" if i == 0 else b","
            yield prefix + json.dumps(row, ensure_ascii=False).encode("utf-8")

        yield b"]}"

    return StreamingResponse(stream_json(), media_type="application/json")

@app.post("/collect")
async def trigger_collection(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_all_collections)
    return {"message": "収集を開始しました", "timestamp": str(datetime.now())}
