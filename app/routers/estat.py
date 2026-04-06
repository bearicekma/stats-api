# e-Stat APIエンドポイント
# /estat/meta/{stats_data_id} : メタ情報・総件数
# /estat/pass/{stats_data_id} : パススルー（コード→名称変換付き）

from fastapi           import APIRouter, Request
from fastapi.responses import StreamingResponse
from datetime          import datetime
import httpx
import json
import os

router = APIRouter(prefix="/estat", tags=["estat"])

# 1リクエストあたりのe-Stat取得上限件数
ESTAT_LIMIT = 100000


# ── ユーティリティ関数 ──────────────────────────────────────

def build_code_to_name_map(class_info: list) -> dict:
    # class_infoからコード→名称の変換辞書を作成する
    # 例: {"area": {"label": "地域", "codes": {"00000": "全国", ...}}}
    code_map = {}
    for class_obj in class_info:
        class_id   = class_obj["@id"]
        class_name = class_obj["@name"]
        classes    = class_obj.get("CLASS", [])

        # CLASSが1件のみの場合はdictで返るためリストに統一する
        if isinstance(classes, dict):
            classes = [classes]

        code_map[class_id] = {
            "label": class_name,
            "codes": {c["@code"]: c["@name"] for c in classes}
        }
    return code_map


def convert_row(row: dict, code_map: dict) -> dict:
    # 1行分のデータのコードを名称に変換する
    # 例: {"@area": "00000", "$": "105.3"} → {"地域": "全国", "値": 105.3}
    converted = {}
    for key, value in row.items():

        if key == "$":
            # 数値データを適切な型に変換する
            try:
                converted["値"] = float(value) if "." in str(value) else int(value)
            except (ValueError, TypeError):
                converted["値"] = value

        elif key.startswith("@"):
            field_id = key[1:]  # 先頭の "@" を除去する

            if field_id in code_map:
                # コードを日本語名称に変換する
                col_name = code_map[field_id]["label"]
                codes    = code_map[field_id]["codes"]
                converted[col_name] = codes.get(value, value)
            else:
                converted[field_id] = value

    return converted


# ── エンドポイント ──────────────────────────────────────────

@router.get("/meta/{stats_data_id}")
async def estat_meta(stats_data_id: str):
    # e-Statの統計表のメタ情報（パラメータ一覧）と総件数を返す
    # 例: /estat/meta/0003427113
    app_id = os.environ["ESTAT_APP_ID"]

    async with httpx.AsyncClient() as client:

        # メタ情報（パラメータ一覧）を取得する
        meta_response = await client.get(
            "https://api.e-stat.go.jp/rest/3.0/app/json/getMetaInfo",
            params={"appId": app_id, "statsDataId": stats_data_id, "lang": "J"},
            timeout=30
        )
        meta_response.raise_for_status()

        # 総件数を確認するため1件だけ取得する
        count_response = await client.get(
            "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData",
            params={"appId": app_id, "statsDataId": stats_data_id, "lang": "J", "limit": 1, "startPosition": 1},
            timeout=30
        )
        count_response.raise_for_status()

    # CLASS_OBJがdictの場合（分類が1つのみ）はリストに統一する
    class_info = meta_response.json()["GET_META_INFO"]["METADATA_INF"]["CLASS_INF"]["CLASS_OBJ"]
    if isinstance(class_info, dict):
        class_info = [class_info]

    # パラメータ一覧を整形する
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

    # 総件数を取得する（TOTAL_NUMBERはDATALIST_INFに含まれる）
    total = int(count_response.json()["GET_STATS_DATA"]["STATISTICAL_DATA"]["RESULT_INF"]["TOTAL_NUMBER"])

    return {
        "stats_data_id": stats_data_id,
        "total_number":  f"{total:,} 件",
        "parameters":    parameters
    }


@router.get("/pass/{stats_data_id}")
async def estat_pass(stats_data_id: str, request: Request):
    # e-Stat APIをページネーションで全件取得しコードを名称に変換して返す
    # URLのクエリパラメータをそのままe-Stat APIに転送する汎用設計
    # 例: /estat/pass/0003427113?cdArea=00000,13A01&cdTimeFrom=2024000000
    app_id = os.environ["ESTAT_APP_ID"]

    # ベースパラメータにリクエストのクエリパラメータを上書きマージする
    params = {
        "appId":       app_id,
        "statsDataId": stats_data_id,
        "lang":        "J",
        "limit":       ESTAT_LIMIT,
    }
    params.update(dict(request.query_params))

    async def stream_json():
        # ページ単位で取得・変換・送信することでメモリ使用量を最小化する
        start_position = 1
        class_info     = None
        code_map       = None
        total_number   = 0
        total_sent     = 0
        first_row      = True
        fetched_at     = str(datetime.now())

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
                values           = statistical_data["DATA_INF"]["VALUE"]

                # 最初のページでclass_infoを取得しヘッダーJSONを送信する
                if class_info is None:
                    class_info = statistical_data["CLASS_INF"]["CLASS_OBJ"]
                    if isinstance(class_info, dict):
                        class_info = [class_info]
                    code_map = build_code_to_name_map(class_info)

                    yield (
                        '{"stats_data_id":"' + stats_data_id + '",'
                        '"fetched_at":"'     + fetched_at    + '",'
                        '"total_number":'    + str(total_number) + ','
                        '"data":['
                    ).encode("utf-8")

                # このページ分を変換して即座に送信する
                for row in values:
                    prefix = b"" if first_row else b","
                    first_row = False
                    yield prefix + json.dumps(
                        convert_row(row, code_map), ensure_ascii=False
                    ).encode("utf-8")
                    total_sent += 1

                # 全件取得完了の確認
                if to_number >= total_number:
                    break
                start_position = to_number + 1

        # フッターにcount（実際に送信した件数）を付加して閉じる
        yield ('],"count":' + str(total_sent) + '}').encode("utf-8")

    return StreamingResponse(stream_json(), media_type="application/json")
