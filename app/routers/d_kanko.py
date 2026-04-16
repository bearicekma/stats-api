# デジタル観光統計オープンデータエンドポイント
# /stats/d_kanko : 観光来訪者数データの取得（期間・都道府県・市区町村で絞り込み可）

from fastapi           import APIRouter, Request
from fastapi.responses import JSONResponse
from google.cloud      import storage
import duckdb
import tempfile
import os

router = APIRouter(prefix="/stats/d_kanko", tags=["d_kanko"])

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")
GCS_PATH    = "d_kanko/d_kanko.parquet"


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


@router.get("")
async def get_d_kanko(request: Request):
    # デジタル観光統計オープンデータを取得する
    # パラメータ:
    #   type : pref（都道府県のみ）/ city（市区町村のみ）/ 省略=両方
    #   from : 開始年月（YYYY-MM または YYYY-MM-DD）
    #   to   : 終了年月（YYYY-MM または YYYY-MM-DD）
    #   pref : 都道府県名（部分一致）
    #   city : 市区町村名（部分一致）※type=cityのみ有効
    #
    # 例: /stats/d_kanko?pref=長野&from=2024-01&to=2024-12&type=pref
    # 例: /stats/d_kanko?city=松本&type=city
    # 例: /stats/d_kanko?pref=長野&type=city&from=2024-01&to=2024-12

    params = dict(request.query_params)
    type_  = params.get("type", None)
    from_  = _normalize_date(params.get("from", ""))
    to_    = _normalize_date(params.get("to",   ""))
    pref   = params.get("pref", None)
    city   = params.get("city", None)

    # WHERE句を組み立てる
    conditions = []

    if type_ in ("pref", "city"):
        conditions.append(f"区分 = '{type_}'")
    if from_:
        conditions.append(f"date >= DATE '{from_}'")
    if to_:
        conditions.append(f"date <= DATE '{to_}'")
    if pref:
        conditions.append(f"都道府県名 LIKE '%{pref}%'")
    if city and type_ == "city":
        conditions.append(f"地域名称 LIKE '%{city}%'")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    tmp_path = None
    try:
        tmp_path = _download_parquet()
        sql = f"""
            SELECT
                strftime(date, '%Y-%m-%d') AS date,
                区分, 都道府県コード, 都道府県名,
                地域コード, 地域名称, 人数
            FROM read_parquet('{tmp_path}')
            {where}
            ORDER BY date, 区分, 都道府県コード, 地域コード
        """
        result = duckdb.query(sql).df()
        data   = result.to_dict(orient="records")
        return {"count": len(data), "data": data}

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
