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
# 戻り値: {"cat01": {"label": "男女", "codes": {"001": "総数", "002": "男"}}, ...}
def build_code_to_name_map(class_info: list) -> dict:
    code_map = {}

    for class_obj in class_info:
        class_id   = class_obj["@id"]
        class_name = class_obj["@name"]

        classes = class_obj.get("CLASS", [])

        # CLASSが辞書（1件のみ）の場合はリストに変換する
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
            # $は実際の数値なので「値」に変換する
            try:
                converted["値"] = float(value) if "." in str(value) else int(value)
            except (ValueError, TypeError):
                converted["値"] = value

        elif key.startswith("@"):
            # cat01 のようにアットマークを除去する
            field_id = key[1:]

            if field_id in code_map:
                # 列名を日本語名に変換する
                col_name = code_map[field_id]["label"]
                # コードを名称に変換する
                codes    = code_map[field_id]["codes"]
                converted[col_name] = codes.get(value, value)
            else:
                # code_mapにないキーはそのまま残す
                converted[field_id] = value

    return converted

# e-Stat APIをページネーションで全件取得しコードを名称に変換して返す
# Claude・Firestoreは使わないので費用ゼロ
# 例: /estat/pass/0003427113?areas=00000,13A01,20A01
# 例: /estat/pass/0003427113?areas=00000,13A01,20A01&time_from=2020000000&time_to=2024000000
@app.get("/estat/pass/{stats_data_id}")
async def estat_pass(
    stats_data_id: str,
    areas:     str = None,  # カンマ区切りで複数地域を指定（例: 00000,13A01,20A01）
    time_from: str = None,  # 時間軸の開始（例: 2020000000）
    time_to:   str = None,  # 時間軸の終了（例: 2024000000）
):
    app_id         = os.environ["ESTAT_APP_ID"]
    all_values     = []
    start_position = 1
    class_info     = None
    total_number   = 0

    # e-Stat APIのパラメータを組み立てる
    params = {
        "appId":         app_id,
        "statsDataId":   stats_data_id,
        "lang":          "J",
        "limit":         ESTAT_LIMIT,   # 1回あたり最大10万件
        "startPosition": start_position,
    }

    # 絞り込みパラメータが指定された場合のみ追加する
    if areas:
        params["cdArea"]     = areas      # 例: 00000,13A01,20A01
    if time_from:
        params["cdTimeFrom"] = time_from
    if time_to:
        params["cdTimeTo"]   = time_to

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

            # 分類情報は最初のページだけ取得する
            if class_info is None:
                class_info = statistical_data["CLASS_INF"]["CLASS_OBJ"]

            values = statistical_data["DATA_INF"]["VALUE"]
            all_values.extend(values)

            # 全件取得完了したらループを抜ける
            if to_number >= total_number:
                break

            # 次のページの開始位置を設定する
            start_position = to_number + 1

    # コード→名称の変換辞書を作成する
    code_map = build_code_to_name_map(class_info)

    # 全行のコードを名称に変換する
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
