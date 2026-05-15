# マスタデータエンドポイント
# /master/_M_pref : 都道府県マスタ
# /master/_M_city       : 市区町村マスタ
# /master/_M_calendar   : カレンダーマスタ（祝日・平日判定）

from datetime import datetime

import duckdb
import tempfile
import os

from fastapi           import APIRouter, Request
from fastapi.responses import JSONResponse
from google.cloud      import storage

router = APIRouter(prefix="/master", tags=["マスタデータ"])

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")


def _download_parquet(collection_name: str) -> str:
    # GCSからParquetを一時ファイルにダウンロードしてパスを返す
    gcs      = storage.Client()
    bucket   = gcs.bucket(BUCKET_NAME)
    gcs_path = f"master/{collection_name}/data.parquet"
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    bucket.blob(gcs_path).download_to_filename(tmp_path)
    return tmp_path


@router.get("/{collection_name}", summary="マスタデータ取得")
async def get_master(collection_name: str, request: Request):
    """
    各種マスタデータを取得します。

    **利用可能な collection_name:**
    - `_M_pref` 都道府県マスタ（47都道府県）
    - `_M_city` 市区町村マスタ（全国市区町村）
    - `_M_calendar` カレンダーマスタ（祝日・平日判定、1950年〜）

    **_M_pref のレスポンスフィールド:**
    - `code` (string) 都道府県コード（2桁）
    - `name` (string) 都道府県名

    **_M_city のレスポンスフィールド:**
    - `code_5_digit` (string) 市区町村コード（5桁）
    - `code_6_digit` (string) 市区町村コード（6桁、検査数字付き）
    - `name` (string) 市区町村名
    - `name_unique` (string) 重複市区町村名に `.` を付加した一意名称
    - `pref_code` (string) 都道府県コード（2桁）
    - `pref_name` (string) 都道府県名

    **_M_calendar のレスポンスフィールド:**
    - `DATE` (string) 日付（YYYY-MM-DD）
    - `年` / `月` / `日` (int) 年月日
    - `年度` (int) 年度
    - `元号` (string) 元号付き年（例: 令和7年）
    - `曜日` (string) 曜日（月〜日）
    - `曜日コード` (int) 曜日コード（0=月 / 1=火 / 2=水 / 3=木 / 4=金 / 5=土 / 6=日）
    - `平日/休日` (string) 平日 or 休日
    - `祝日` (bool) 祝日フラグ
    - `祝日名` (string) 祝日名（祝日以外はnull）

    **_M_calendar のクエリパラメータ（_M_calendarのみ有効）:**
    - `year` (任意) 年で絞込。例: `2026`
    - `month` (任意) 月で絞込（1-12）。例: `5`
    - `from` (任意) 開始日（YYYY-MM-DD）。例: `2026-01-01`
    - `to` (任意) 終了日（YYYY-MM-DD）。例: `2026-12-31`
    - `holiday_only` (任意) `true` で祝日のみ取得
    - `weekday` (任意) 曜日コードで絞込（0=月〜6=日）。例: `0`

    **URL例:**
    - `/master/_M_pref` 都道府県一覧
    - `/master/_M_city` 市区町村一覧
    - `/master/_M_calendar?year=2026` 2026年のカレンダー
    - `/master/_M_calendar?year=2026&holiday_only=true` 2026年の祝日一覧
    - `/master/_M_calendar?from=2026-04-01&to=2026-06-30` 期間指定
    - `/master/_M_calendar?weekday=0&year=2026` 2026年の月曜日一覧
    """
    tmp_path = None
    try:
        tmp_path = _download_parquet(collection_name)

        # _M_calendar のみ絞り込みパラメータを処理する
        conditions = []
        if collection_name == "_M_calendar":
            params       = dict(request.query_params)
            year         = params.get("year")
            month        = params.get("month")
            from_        = params.get("from")
            to_          = params.get("to")
            holiday_only = params.get("holiday_only", "").lower() == "true"
            weekday      = params.get("weekday")

            if year:
                conditions.append(f"年 = {int(year)}")
            if month:
                conditions.append(f"月 = {int(month)}")
            if from_:
                conditions.append(f"DATE >= '{from_}'")
            if to_:
                conditions.append(f"DATE <= '{to_}'")
            if holiday_only:
                conditions.append("祝日 = TRUE")
            if weekday is not None:
                conditions.append(f"曜日コード = {int(weekday)}")

        where  = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        sql    = f"SELECT * FROM read_parquet('{tmp_path}') {where} ORDER BY DATE"
        result = duckdb.query(sql).df()
        data   = result.to_dict(orient="records")
        return {"collection": collection_name, "updated_at": str(datetime.now()), "count": len(data), "data": data}

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
