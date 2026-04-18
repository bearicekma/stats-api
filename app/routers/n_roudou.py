# 長野労働局APIエンドポイント
# /n_roudou/juri_sangyo : 受理地別・産業別新規求人数（大分類）

from fastapi           import APIRouter, Request
from fastapi.responses import JSONResponse
from google.cloud      import storage
import duckdb
import math
import os
import pandas as pd
import tempfile


router = APIRouter(prefix="/n_roudou", tags=["n_roudou"])

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")
GCS_PATH    = "n_roudou/juri_sangyo/data.parquet"


def _download_parquet() -> str:
    # GCSからParquetを一時ファイルにダウンロードしてパスを返す
    gcs    = storage.Client()
    bucket = gcs.bucket(BUCKET_NAME)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    bucket.blob(GCS_PATH).download_to_filename(tmp_path)
    return tmp_path


def _normalize_date(value: str) -> str | None:
    # YYYY-MM または YYYY-MM-DD を YYYY-MM-01 形式に正規化する
    if not value:
        return None
    parts = value.split("-")
    if len(parts) >= 2:
        return f"{parts[0]}-{parts[1].zfill(2)}-01"
    return None


def _clean_value(v):
    # float型のNaN・inf・-infをNoneに変換する（JSONシリアライズ対策）
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    # DataFrameを辞書リストに変換する
    # datetime型の列はYYYY-MM-DD形式の文字列に変換する
    # NaN/infをNoneに変換する（JSONシリアライズ対策）

    # datetime型の列を文字列化する（DATEと公表日が対象）
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d").where(df[col].notnull(), None)

    records = df.to_dict(orient="records")
    return [
        {k: _clean_value(v) for k, v in record.items()}
        for record in records
    ]


@router.get("/juri_sangyo")
async def get_juri_sangyo(request: Request):
    # 受理地別・産業別新規求人数を取得する
    # パラメータ:
    #   date : 対象年月（YYYY-MM または YYYY-MM-DD、単月指定）
    #   from : 開始年月（YYYY-MM）
    #   to   : 終了年月（YYYY-MM）
    #   code : 産業コード（all / D / E / G / H / I / J / K / M / N / O / P / R / other）
    #
    # 例: /n_roudou/juri_sangyo?date=2026-02
    # 例: /n_roudou/juri_sangyo?from=2025-04&to=2026-02&code=D
    # 例: /n_roudou/juri_sangyo?code=all

    params = dict(request.query_params)
    date_  = _normalize_date(params.get("date", ""))
    from_  = _normalize_date(params.get("from", ""))
    to_    = _normalize_date(params.get("to",   ""))
    code   = params.get("code", None)

    # WHERE句を組み立てる
    conditions = []
    if date_:
        conditions.append(f"DATE = DATE '{date_}'")
    if from_:
        conditions.append(f"DATE >= DATE '{from_}'")
    if to_:
        conditions.append(f"DATE <= DATE '{to_}'")
    if code:
        # シングルクォート対策として置換する
        safe_code = code.replace("'", "''")
        conditions.append(f"産業コード = '{safe_code}'")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    tmp_path = None
    try:
        tmp_path = _download_parquet()
        sql = f"""
            SELECT * FROM read_parquet('{tmp_path}')
            {where}
            ORDER BY DATE, 産業コード
        """
        df = duckdb.query(sql).to_df()
        records = _df_to_records(df)

        return {
            "count": len(records),
            "data":  records,
        }

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": f"クエリ失敗: {type(e).__name__}: {str(e)}"}
        )

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
