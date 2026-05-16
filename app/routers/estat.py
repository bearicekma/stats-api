# e-Stat APIエンドポイント
# /estat/meta/{stats_data_id} : メタ情報・フィルタ後件数・推定ページ数
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

# e-Stat APIへの同時リクエスト数 兼 先読みウィンドウ幅（過負荷・メモリ抑制のため5）
ESTAT_CONCURRENCY = 5

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

@router.get("/meta/{stats_data_id}", summary="統計表のメタ情報・フィルタ後件数")
async def estat_meta(stats_data_id: str, request: Request):
    """
    e-Stat統計表のパラメータ一覧と、フィルタ条件を反映した件数・推定ページ数を返します。

    `/pass` で実際に取得する前に、クエリの重さ（件数・ページ数）を確認できます。

    **パスパラメータ:**
    - `stats_data_id` (str) e-Statの統計表ID。例: 0003427113（消費者物価指数）

    **クエリパラメータ（任意・/pass と同じ条件を指定可能）:**
    - `cdArea` (str) 地域コード。カンマ区切りで複数指定可。例: 13A01
    - `cdCat01` (str) 分類事項01のコード
    - `cdTime` (str) 時間軸コード
    - `cdTimeFrom` (str) 時間軸の開始。例: 2020000000
    - `cdTimeTo` (str) 時間軸の終了
    - その他 e-Stat getStatsData が受け付ける絞り込みパラメータ

    **レスポンス:**
    - `stats_data_id` (str) 統計表ID
    - `applied_filters` (object) 適用されたフィルタ条件のエコーバック
    - `total_number` (str) フィルタ後の総件数。例: 12,480 件
    - `estimated_pages` (int) /pass が取得するページ数の見積もり（1ページ=10万件）
    - `parameters` (array) 指定可能なパラメータと選択肢の一覧

    **使用例:**
    - /estat/meta/0003427113 … 全件の件数とパラメータ一覧
    - /estat/meta/0003427113?cdArea=13A01&cdTimeFrom=2020000000 … 絞り込み後の件数
    """
    app_id = os.environ["ESTAT_APP_ID"]

    # フィルタ条件（クエリパラメータ）を取り出す
    applied_filters = dict(request.query_params)

    async with httpx.AsyncClient() as client:

        # パラメータ一覧を取得する（絞り込みと無関係なため全件のまま）
        meta_response = await client.get(
            "https://api.e-stat.go.jp/rest/3.0/app/json/getMetaInfo",
            params={"appId": app_id, "statsDataId": stats_data_id, "lang": "J"},
            timeout=30
        )
        meta_response.raise_for_status()

        # フィルタ後件数を確認するため1件だけ取得する
        # クエリパラメータを転送し、limit/startPositionは最小値で強制上書きする
        count_params = {"appId": app_id, "statsDataId": stats_data_id, "lang": "J"}
        count_params.update(applied_filters)
        count_params["limit"]         = 1
        count_params["startPosition"] = 1

        count_response = await client.get(
            "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData",
            params=count_params,
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

    # フィルタ後件数を取得する（TOTAL_NUMBERは絞り込み条件を反映する）
    total = int(count_response.json()["GET_STATS_DATA"]["STATISTICAL_DATA"]["RESULT_INF"]["TOTAL_NUMBER"])

    # /pass が取得するページ数を推定する（重さの目安）
    estimated_pages = (total + ESTAT_LIMIT - 1) // ESTAT_LIMIT

    return {
        "stats_data_id":   stats_data_id,
        "applied_filters": applied_filters,
        "total_number":    f"{total:,} 件",
        "estimated_pages": estimated_pages,
        "parameters":      parameters
    }


@router.get("/pass/{stats_data_id}", summary="統計データ取得（パススルー・名称変換付き）")
async def estat_pass(stats_data_id: str, request: Request):
    """
    e-Stat統計表のデータを取得し、コードを日本語名称に変換してJSONで返します。

    並列先読みでメモリを一定に保ちながらストリーミング送信するため、大量データにも対応します。
    Power Query / Excel での取り込みを想定した汎用エンドポイントです。

    **パスパラメータ:**
    - `stats_data_id` (str) e-Statの統計表ID。例: 0003427113（消費者物価指数）

    **クエリパラメータ（任意・e-Stat getStatsData にそのまま転送）:**
    - `cdArea` (str) 地域コード。カンマ区切りで複数指定可。例: 00000,13A01
    - `cdCat01` (str) 分類事項01のコード
    - `cdTimeFrom` (str) 時間軸の開始。例: 2024000000
    - `cdTimeTo` (str) 時間軸の終了
    - その他 e-Stat getStatsData が受け付ける絞り込みパラメータ

    **レスポンス（JSONストリーミング）:**
    - `stats_data_id` (str) 統計表ID
    - `fetched_at` (str) 取得日時
    - `total_number` (int) 総件数
    - `data` (array) コードを名称変換したデータ行（`値` は数値型に変換）
    - `count` (int) 実際に送信した件数

    **注意:**
    - 大量データ（数十万件以上）は取得に時間がかかります。事前に /estat/meta/{stats_data_id} で件数確認を推奨します
    - 数百万件規模はパススルーに不向きです（事前収集方式の検討を推奨）

    **使用例:**
    - /estat/pass/0003427113?cdArea=13A01&cdTimeFrom=2020000000
    """
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
