# 資源エネルギー庁 給油所小売価格調査エンドポイント
# /enecho/gasoline : ガソリン・軽油・灯油の週次小売価格（全国・都道府県別）

import os
import tempfile
from datetime import datetime

import duckdb
from fastapi           import APIRouter, Request
from fastapi.responses import JSONResponse
from google.cloud      import storage

router = APIRouter(prefix="/enecho", tags=["資源エネルギー庁"])

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")
GCS_PATH    = "enecho/gasoline/data.parquet"


def _download_parquet() -> str:
    # GCSからParquetを一時ファイルにダウンロードしてパスを返す
    gcs    = storage.Client()
    bucket = gcs.bucket(BUCKET_NAME)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    bucket.blob(GCS_PATH).download_to_filename(tmp_path)
    return tmp_path


@router.get(
    "/gasoline",
    summary="給油所小売価格調査（ガソリン・軽油・灯油）",
)
async def get_gasoline(request: Request):
    """
    資源エネルギー庁の給油所小売価格調査データを取得します。
    毎週月曜調査・水曜14時公表のデータを週次で収集しています。
    収録期間: 1990年8月〜

    **クエリパラメータ:**
    - `item` (任意) 品目: `ハイオク` / `レギュラー` / `軽油` / `灯油店頭` / `灯油配達`
    - `region` (任意) 地域: `全国` / `長野` など（都道府県名）
    - `from` (任意) 開始日（YYYY-MM-DD）
    - `to` (任意) 終了日（YYYY-MM-DD）

    **レスポンスフィールド:**
    - `date` (string) 調査日（YYYY-MM-DD形式）
    - `品目` (string) ハイオク / レギュラー / 軽油 / 灯油店頭 / 灯油配達
    - `地域` (string) 全国または都道府県名
    - `価格` (float) 円/リットル（灯油店頭・配達は円/18リットル）
    - `消費税率` (float) 例: 0.10

    **URL例:**
    - `/enecho/gasoline?item=レギュラー&region=全国`
    - `/enecho/gasoline?item=レギュラー&region=長野&from=2024-01-01`
    - `/enecho/gasoline?item=灯油店頭&region=全国&from=2020-01-01`
    """
    params = dict(request.query_params)
    item   = params.get("item")
    region = params.get("region")
    from_  = params.get("from")
    to_    = params.get("to")

    conditions = []
    if item:
        conditions.append(f"品目 = '{item}'")
    if region:
        conditions.append(f"地域 = '{region}'")
    if from_:
        conditions.append(f"date >= DATE '{from_}'")
    if to_:
        conditions.append(f"date <= DATE '{to_}'")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    tmp_path = None
    try:
        tmp_path = _download_parquet()
        sql = f"""
            SELECT
                CAST(date AS VARCHAR) AS date,
                品目, 地域, 価格, 消費税率
            FROM read_parquet('{tmp_path}')
            {where}
            ORDER BY date, 品目, 地域
        """
        result = duckdb.query(sql).df()
        data   = result.to_dict(orient="records")
        return {"count": len(data), "updated_at": str(datetime.now()), "data": data}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
