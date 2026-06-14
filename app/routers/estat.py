# e-Stat APIエンドポイント
# /estat/meta/{stats_data_id} : メタ情報・フィルタ後件数・推定ページ数
# /estat/pass/{stats_data_id} : パススルー（並列先読み＋メモリ一定のストリーミング、JSON/CSV対応）

from fastapi           import APIRouter, Request
from fastapi.responses import StreamingResponse
from datetime          import datetime
from collections       import deque
import asyncio
import csv
import io
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

# /pass でユーザーから受け取ってもe-Statに転送しない予約パラメータ
# （ページング制御は内部で行うため。limit等をユーザーに上書きされるとページ割りが壊れる）
RESERVED_PARAMS = {
    "limit", "startposition", "metagetflg",
    "explanationgetflg", "annotationgetflg", "format",
}


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
    # 1行分のデータのコードを名称に変換する（JSON出力用）
    # 例: {"@area": "00000", "$": "105.3"} → {"地域": "全国", "値": 105.3}
    converted = {}
    for key, value in row.items():

        if key == "$":
            # 数値データを適切な型に変換する
            try:
                converted["値"] = float(value) if "." in str(value) else int(value)
            except (ValueError, TypeError):
                converted["値"] = value

        elif key == "@unit":
            # 単位は専用の列名に統一する
            converted["単位"] = value

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


def build_csv_columns(code_map: dict) -> list:
    # CSVの固定列を決める：分類項目のlabel（定義順）＋ 単位 ＋ 値
    columns = [info["label"] for info in code_map.values()]
    columns.append("単位")
    columns.append("値")
    return columns


async def fetch_data_page(client: httpx.AsyncClient, base_params: dict, start_pos: int) -> list:
    # 1ページ分のデータ行（VALUE配列）を取得する
    # 各種GetFlg=N で付帯情報の転送を抑制し応答を軽量化する
    page_params = dict(base_params)
    page_params["startPosition"]     = start_pos
    page_params["metaGetFlg"]        = "N"   # メタ情報を省略
    page_params["explanationGetFlg"] = "N"   # 解説情報を省略
    page_params["annotationGetFlg"]  = "N"   # 注釈情報を省略

    resp = await client.get(ESTAT_GET_STATS_DATA, params=page_params)
    resp.raise_for_status()
    values = resp.json()["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["VALUE"]

    # VALUEが1件のみの場合はdictで返るためリストに統一する
    if isinstance(values, dict):
        values = [values]
    return values


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
    e-Stat統計表のデータを取得し、コードを日本語名称に変換して返します。

    並列先読みでメモリを一定に保ちながらストリーミング送信するため、大量データにも対応します。
    Power Query / Excel での取り込みを想定した汎用エンドポイントです。

    **パスパラメータ:**
    - `stats_data_id` (str) e-Statの統計表ID。例: 0003427113（消費者物価指数）

    **出力形式（任意）:**
    - `format=csv` (既定) CSV形式（UTF-8 BOM付き）で返します。大量データに最適で軽量です
    - `format=json` JSON形式で返します（ネスト構造が必要な場合）

    **クエリパラメータ（任意・e-Stat getStatsData にそのまま転送）:**
    - `cdArea` (str) 地域コード。カンマ区切りで複数指定可。例: 00000,13A01
    - `cdCat01` (str) 分類事項01のコード
    - `cdTimeFrom` (str) 時間軸の開始。例: 2024000000
    - `cdTimeTo` (str) 時間軸の終了
    - その他 e-Stat getStatsData が受け付ける絞り込みパラメータ

    **レスポンス（JSON時・ストリーミング）:**
    - `stats_data_id` (str) 統計表ID
    - `fetched_at` (str) 取得日時
    - `total_number` (int) 総件数
    - `data` (array) コードを名称変換したデータ行（`値` は数値型に変換）
    - `count` (int) 実際に送信した件数

    **レスポンス（CSV時・ストリーミング）:**
    - 1行目がヘッダー（分類項目＋単位＋値）、2行目以降がデータ

    **注意:**
    - 大量データ（数十万件以上）は取得に時間がかかります。事前に /estat/meta/{stats_data_id} で件数確認を推奨します
    - 数百万件規模はパススルーに不向きです（事前収集方式の検討を推奨）

    **使用例:**
    - /estat/pass/0003427113?cdArea=13A01&cdTimeFrom=2020000000
    - /estat/pass/0003427113?format=csv&cdArea=13A01
    """
    app_id = os.environ["ESTAT_APP_ID"]

    # 出力形式を判定する（既定はjson）
    output_format = request.query_params.get("format", "csv").lower()

    # ベースパラメータを構築する
    # 予約パラメータ（limit等）はユーザー指定を除外し、内部のページング制御を守る
    base_params = {
        "appId":       app_id,
        "statsDataId": stats_data_id,
        "lang":        "J",
        "limit":       ESTAT_LIMIT,
    }
    user_params = {
        k: v for k, v in request.query_params.items()
        if k.lower() not in RESERVED_PARAMS
    }
    base_params.update(user_params)

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

    # ── ページを並列先読みしながら順に取り出すジェネレータ ─
    # JSON/CSV両モードで共有する。送信済みページは都度メモリ解放される
    async def iter_pages(client: httpx.AsyncClient):
        in_flight = deque()   # 先読み中タスクのキュー（最大ESTAT_CONCURRENCY）
        next_idx  = 0

        # 先読みウィンドウを初期充填する
        while next_idx < len(start_positions) and len(in_flight) < ESTAT_CONCURRENCY:
            in_flight.append(asyncio.create_task(fetch_data_page(client, base_params, start_positions[next_idx])))
            next_idx += 1

        try:
            while in_flight:
                # 最古のページを順番に待つ（出力順序を保証）
                values = await in_flight.popleft()

                # 次ページの取得を先行開始しウィンドウを補充する
                if next_idx < len(start_positions):
                    in_flight.append(asyncio.create_task(fetch_data_page(client, base_params, start_positions[next_idx])))
                    next_idx += 1

                yield values
        finally:
            # クライアント切断時など、先読み中タスクをキャンセルしリークを防ぐ
            for task in in_flight:
                task.cancel()

    # ── JSONストリーム ───────────────────────────────────
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
            async for values in iter_pages(client):
                # このページを変換しながら逐次送出する
                for i, row in enumerate(values):
                    prefix    = b"" if first_row else b","
                    first_row = False
                    # orjson.dumpsはbytesを返し非ASCIIをUTF-8でそのまま出力する
                    yield prefix + orjson.dumps(convert_row(row, code_map))
                    total_sent += 1

                    # 8192行ごとにループへ制御を返す（実送出・先読みを進めるため）
                    if (i & 0x1FFF) == 0:
                        await asyncio.sleep(0)
                del values

        # フッターに実際の送信件数を付加して閉じる
        yield ('],"count":' + str(total_sent) + '}').encode("utf-8")

    # ── CSVストリーム ────────────────────────────────────
    async def stream_csv():
        # 固定列を決め、BOM付きヘッダー行を送出する
        columns = build_csv_columns(code_map)
        buf     = io.StringIO()
        writer  = csv.writer(buf)

        writer.writerow(columns)
        # 先頭にBOMを付与しExcelでの直接オープン時の文字化けを防ぐ
        yield ("\ufeff" + buf.getvalue()).encode("utf-8")
        buf.seek(0); buf.truncate(0)

        async with httpx.AsyncClient(timeout=300) as client:
            async for values in iter_pages(client):
                for i, row in enumerate(values):
                    # 名称変換した辞書を固定列順に並べる（無い列は空欄）
                    d = convert_row(row, code_map)
                    writer.writerow([d.get(c, "") for c in columns])

                    # 1024行ごとにバッファを送出してメモリを解放する
                    if (i & 0x3FF) == 0:
                        yield buf.getvalue().encode("utf-8")
                        buf.seek(0); buf.truncate(0)
                        await asyncio.sleep(0)

                # ページ末でバッファを送出する
                yield buf.getvalue().encode("utf-8")
                buf.seek(0); buf.truncate(0)
                del values

    # ── 出力形式に応じて返す（既定はCSV） ────────────────
    if output_format == "json":
        return StreamingResponse(stream_json(), media_type="application/json")
    return StreamingResponse(stream_csv(), media_type="text/csv; charset=utf-8")
