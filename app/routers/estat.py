# e-Stat APIエンドポイント
# /estat/meta/{stats_data_id} : メタ情報・総件数
# /estat/pass/{stats_data_id} : パススルー（コード→名称変換付き・並列取得で高速化）

from fastapi           import APIRouter, Request
from fastapi.responses import StreamingResponse
from datetime          import datetime
import asyncio
import httpx
import orjson
import os

router = APIRouter(prefix="/estat", tags=["estat"])

# 1リクエストあたりのe-Stat取得上限件数
ESTAT_LIMIT = 100000

# e-Stat APIへの同時リクエスト数（過負荷を避けるため3に制限）
ESTAT_CONCURRENCY = 3

# e-Stat データ取得APIのエンドポイント
ESTAT_GET_STATS_DATA = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"


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

    # 総件数を取得する（TOTAL_NUMBERはRESULT_INFに含まれる）
    total = int(count_response.json()["GET_STATS_DATA"]["STATISTICAL_DATA"]["RESULT_INF"]["TOTAL_NUMBER"])

    return {
        "stats_data_id": stats_data_id,
        "total_number":  f"{total:,} 件",
        "parameters":    parameters
    }


@router.get("/pass/{stats_data_id}")
async def estat_pass(stats_data_id: str, request: Request):
    # e-Stat APIを並列ページ取得で高速に全件取得し、コードを名称に変換して返す
    # URLのクエリパラメータをそのままe-Stat APIに転送する汎用設計
    # 例: /estat/pass/0003427113?cdArea=00000,13A01&cdTimeFrom=2024000000
    app_id = os.environ["ESTAT_APP_ID"]

    # ベースパラメータにリクエストのクエリパラメータを上書きマージする
    base_params = {
        "appId":       app_id,
        "statsDataId": stats_data_id,
        "lang":        "J",
        "limit":       ESTAT_LIMIT,
    }
    base_params.update(dict(request.query_params))

    # ── 先行コール ───────────────────────────────────────
    # limit=1 で叩き、total_number と class_info（変換辞書の元）を軽量取得する
    # 応答が数百バイトに収まるため即座に返る
    async with httpx.AsyncClient(timeout=60) as client:
        lead_params = dict(base_params)
        lead_params["limit"]         = 1
        lead_params["startPosition"] = 1

        lead_resp = await client.get(ESTAT_GET_STATS_DATA, params=lead_params)
        lead_resp.raise_for_status()
        statistical_data = lead_resp.json()["GET_STATS_DATA"]["STATISTICAL_DATA"]
        total_number     = int(statistical_data["RESULT_INF"]["TOTAL_NUMBER"])

        # コード→名称の変換辞書を構築する（class_infoは先行コールから取得）
        class_info = statistical_data["CLASS_INF"]["CLASS_OBJ"]
        if isinstance(class_info, dict):
            class_info = [class_info]
        code_map = build_code_to_name_map(class_info)

    # ── ページ境界を計算する ─────────────────────────────
    # 例: total=250000, LIMIT=100000 → startPosition = [1, 100001, 200001]
    start_positions = list(range(1, total_number + 1, ESTAT_LIMIT))

    # ── 全ページを並列取得する ───────────────────────────
    # Semaphoreで同時実行数をESTAT_CONCURRENCYに制限しe-Statへの過負荷を防ぐ
    semaphore = asyncio.Semaphore(ESTAT_CONCURRENCY)

    async def fetch_page(client: httpx.AsyncClient, start_pos: int) -> list:
        # 1ページ分を取得しVALUE配列を返す
        # metaGetFlg=N で不要なメタデータ送信を抑制し応答を軽量化する
        async with semaphore:
            page_params = dict(base_params)
            page_params["startPosition"] = start_pos
            page_params["metaGetFlg"]    = "N"

            resp = await client.get(ESTAT_GET_STATS_DATA, params=page_params)
            resp.raise_for_status()
            return resp.json()["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["VALUE"]

    async with httpx.AsyncClient(timeout=300) as client:
        # asyncio.gatherは渡した順序どおりに結果を返すためページ順序が保たれる
        tasks        = [fetch_page(client, sp) for sp in start_positions]
        pages_values = await asyncio.gather(*tasks)

    # ── ストリーム送信 ───────────────────────────────────
    # 全ページ取得済みだが、巨大な単一文字列を作らずorjsonで1行ずつ送出する
    # （sync generatorはStarletteがthreadpoolで実行するためイベントループを塞がない）
    fetched_at = str(datetime.now())

    def stream_json():
        # ヘッダー部分
        yield (
            '{"stats_data_id":"' + stats_data_id + '",'
            '"fetched_at":"'     + fetched_at    + '",'
            '"total_number":'    + str(total_number) + ','
            '"data":['
        ).encode("utf-8")

        # 各ページの各行を変換しながら逐次送出する
        total_sent = 0
        first_row  = True
        for values in pages_values:
            for row in values:
                prefix    = b"" if first_row else b","
                first_row = False
                # orjson.dumpsはbytesを返し、非ASCIIをUTF-8でそのまま出力する
                yield prefix + orjson.dumps(convert_row(row, code_map))
                total_sent += 1

        # フッターに実際の送信件数を付加して閉じる
        yield ('],"count":' + str(total_sent) + '}').encode("utf-8")

    return StreamingResponse(stream_json(), media_type="application/json")
