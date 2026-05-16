# e-Stat APIエンドポイント
# /estat/meta/{stats_data_id} : メタ情報・総件数
# /estat/pass/{stats_data_id} : パススルー（並列先読み＋メモリ一定のストリーミング）

from fastapi           import APIRouter, Request
from fastapi.responses import StreamingResponse
from datetime          import datetime
from collections       import deque
import asyncio
import httpx
import orjson
import os

router = APIRouter(prefix="/estat", tags=["estat"])

# 1リクエストあたりのe-Stat取得上限件数
ESTAT_LIMIT = 100000

# e-Stat APIへの同時リクエスト数 兼 先読みウィンドウ幅（過負荷・メモリ抑制のため3）
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
    # e-Stat APIを並列先読みしつつメモリ一定でストリーム返却する
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
    # limit=1 で total_number と class_info（変換辞書の元）を軽量取得する
    async with httpx.AsyncClient(timeout=60) as client:
        lead_params = dict(base_params)
        lead_params["limit"]         = 1
        lead_params["startPosition"] = 1

        lead_resp = await client.get(ESTAT_GET_STATS_DATA, params=lead_params)
        lead_resp.raise_for_status()
        statistical_data = lead_resp.json()["GET_STATS_DATA"]["STATISTICAL_DATA"]
        total_number     = int(statistical_data["RESULT_INF"]["TOTAL_NUMBER"])

        # コード→名称の変換辞書を構築する
        class_info = statistical_data["CLASS_INF"]["CLASS_OBJ"]
        if isinstance(class_info, dict):
            class_info = [class_info]
        code_map = build_code_to_name_map(class_info)

    # ── ページ境界を計算する ─────────────────────────────
    # 例: total=250000, LIMIT=100000 → startPosition = [1, 100001, 200001]
    start_positions = list(range(1, total_number + 1, ESTAT_LIMIT))
    fetched_at      = str(datetime.now())

    # ── ストリーム送信（並列先読み＋メモリ一定） ─────────
    async def stream_json():
        # ヘッダー部分を送出する
        yield (
            '{"stats_data_id":"' + stats_data_id + '",'
            '"fetched_at":"'     + fetched_at    + '",'
            '"total_number":'    + str(total_number) + ','
            '"data":['
        ).encode("utf-8")

        total_sent = 0
        first_row  = True

        async with httpx.AsyncClient(timeout=300) as client:

            async def fetch_page(start_pos: int) -> list:
                # 1ページ分を取得しVALUE配列を返す
                # metaGetFlg=N で不要なメタデータ送信を抑制し応答を軽量化する
                page_params = dict(base_params)
                page_params["startPosition"] = start_pos
                page_params["metaGetFlg"]    = "N"

                resp = await client.get(ESTAT_GET_STATS_DATA, params=page_params)
                resp.raise_for_status()
                return resp.json()["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["VALUE"]

            in_flight = deque()   # 先読み中タスクのキュー（最大ESTAT_CONCURRENCY）
            next_idx  = 0

            # 先読みウィンドウを初期充填する（最大ESTAT_CONCURRENCYページ並列）
            while next_idx < len(start_positions) and len(in_flight) < ESTAT_CONCURRENCY:
                in_flight.append(asyncio.create_task(fetch_page(start_positions[next_idx])))
                next_idx += 1

            try:
                while in_flight:
                    # 最古のページを順番に待つ（出力順序を保証）
                    values = await in_flight.popleft()

                    # 次ページの取得を先行開始しウィンドウを補充する
                    if next_idx < len(start_positions):
                        in_flight.append(asyncio.create_task(fetch_page(start_positions[next_idx])))
                        next_idx += 1

                    # このページを変換しながら逐次送出する
                    for i, row in enumerate(values):
                        prefix    = b"" if first_row else b","
                        first_row = False
                        # orjson.dumpsはbytesを返し非ASCIIをUTF-8でそのまま出力する
                        yield prefix + orjson.dumps(convert_row(row, code_map))
                        total_sent += 1

                        # 8192行ごとにループへ制御を返す
                        # （送信バイトの実送出・次ページ先読みを進めるため）
                        if (i & 0x1FFF) == 0:
                            await asyncio.sleep(0)

                    # 送信済みページのメモリを即解放する
                    del values
            finally:
                # クライアント切断時など、先読み中タスクをキャンセルしリークを防ぐ
                for task in in_flight:
                    task.cancel()

        # フッターに実際の送信件数を付加して閉じる
        yield ('],"count":' + str(total_sent) + '}').encode("utf-8")

    return StreamingResponse(stream_json(), media_type="application/json")
