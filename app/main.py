from fastapi       import FastAPI, BackgroundTasks
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

def build_code_to_name_map(class_info: list) -> dict:
    """
    class_infoからコード→名称の変換辞書を作成する

    戻り値の構造:
    {
        "cat01": {"label": "男女", "codes": {"001": "総数", "002": "男"}},
        "area":  {"label": "地域", "codes": {"00000": "全国", "01000": "北海道"}},
    }
    """
    code_map = {}

    for class_obj in class_info:
        class_id   = class_obj["@id"]    # 例: "cat01"
        class_name = class_obj["@name"]  # 例: "男女"

        classes = class_obj.get("CLASS", [])

        # CLASSが辞書（1件のみ）の場合はリストに変換する
        if isinstance(classes, dict):
            classes = [classes]

        code_map[class_id] = {
            "label": class_name,                                 # 列名に使う日本語名
            "codes": {c["@code"]: c["@name"] for c in classes}  # コード→名称の辞書
        }

    return code_map

def convert_row(row: dict, code_map: dict) -> dict:
    """
    1行分のデータのコードを名称に変換する

    変換前: {"@cat01": "002", "@area": "01000", "@time": "2020100000", "$": "1250000"}
    変換後: {"男女": "男", "地域": "北海道", "時間軸": "2020年10月", "値": 1250000}
    """
    converted = {}

    for key, value in row.items():
        if key == "$":
            # $は実際の数値なので「値」に変換する
            try:
                converted["値"] = float(value) if "." in str(value) else int(value)
            except (ValueError, TypeError):
                converted["値"] = value

        elif key.startswith("@"):
            # @cat01 → cat01 のようにアットマークを除去する
            field_id = key[1:]

            if field_id in code_map:
                # 列名を日本語名に変換する（例: cat01 → 男女）
                col_name = code_map[field_id]["label"]

                # コードを名称に変換する（例: 002 → 男）
                codes = code_map[field_id]["codes"]
                converted[col_name] = codes.get(value, value)
            else:
                # code_mapにないキーはそのまま残す
                converted[field_id] = value

    return converted

@app.get("/estat/pass/{stats_data_id}")
async def estat_pass(stats_data_id: str):
    """
    e-Stat APIをページネーションで全件取得し
    コードを日本語名称に変換してから返す
    Claude・Firestoreは使わないので費用ゼロ

    stats_data_id : e-Statの統計表ID
    例: /estat/pass/0003448237
    """
    app_id         = os.environ["ESTAT_APP_ID"]
    all_values     = []
    start_position = 1
    class_info     = None  # 分類情報（最初のページから取得）

    async with httpx.AsyncClient() as client:
        while True:
            response = await client.get(
                "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData",
                params={
                    "appId":         app_id,
                    "statsDataId":   stats_data_id,
                    "lang":          "J",
                    "limit":         ESTAT_LIMIT,
                    "startPosition": start_position,
                },
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

            if to_number >= total_number:
                break

            start_position = to_number + 1

    # コード→名称の変換辞書を作成する
    code_map = build_code_to_name_map(class_info)

    # 全行のコードを名称に変換する
    converted_data = [convert_row(row, code_map) for row in all_values]

    return {
        "stats_data_id": stats_data_id,
        "fetched_at":    str(datetime.now()),
        "count":         len(converted_data),
        "data":          converted_data  # コードが日本語名称に変換済みのデータ
    }

@app.post("/collect")
async def trigger_collection(background_tasks: BackgroundTasks):
    """データ収集を手動トリガーする（保存方式用）"""
    background_tasks.add_task(run_all_collections)
    return {"message": "収集を開始しました", "timestamp": str(datetime.now())}
