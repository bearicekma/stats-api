# 社人研「日本の地域別将来推計人口」市区町村別データを返すルータ
# 推計版ごとのフォルダでGCSに保持し、estimate_ver未指定なら最新版を返す
import os
import re
import tempfile
import duckdb
from datetime import datetime
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse, Response
from google.cloud import storage

router = APIRouter(prefix="/ipss", tags=["社人研 将来推計人口"])

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")
PREFIX = "ipss/shorai_jinko"
_ver_cache = None


def _list_versions():
    # GCSのフォルダ名から推計版を拾う。新版はParquetを置くだけで認識される
    global _ver_cache
    if _ver_cache is not None:
        return _ver_cache
    gcs = storage.Client()
    vers = set()
    for b in gcs.list_blobs(BUCKET_NAME, prefix=f"{PREFIX}/"):
        m = re.match(rf"{PREFIX}/(\d+)/data\.parquet$", b.name)
        if m:
            vers.add(m.group(1))
    _ver_cache = sorted(vers)
    return _ver_cache


def _download_parquet(ver: str) -> str:
    # 指定版のParquetを一時ファイルに落としてパスを返す
    gcs = storage.Client()
    bucket = gcs.bucket(BUCKET_NAME)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    bucket.blob(f"{PREFIX}/{ver}/data.parquet").download_to_filename(tmp_path)
    return tmp_path


@router.get("/versions", summary="推計版一覧")
async def get_versions():
    vers = _list_versions()
    return {"versions": vers, "latest": vers[-1] if vers else None}


@router.get("/shorai_jinko", summary="将来推計人口（市区町村別）")
async def get_shorai_jinko(
    estimate_ver: str = Query(None, description="推計版。未指定なら最新版"),
    pref_code: str = Query(None, description="都道府県コード2桁"),
    area_code: str = Query(None, description="地域コード5桁"),
    area_level: str = Query(None, description="pref / city / special_ward / other"),
    year: int = Query(None, description="2020〜2050の5年刻み"),
    sex: str = Query(None, description="総数 / 男 / 女"),
    age5_code: str = Query(None, description="00〜18、95歳以上は 19+"),
    limit: int = Query(None, ge=1),
    format: str = Query("json", description="json または csv"),
):
    tmp_path = None
    try:
        vers = _list_versions()
        if not vers:
            return JSONResponse(status_code=503, content={"error": "no data available"})
        ver = estimate_ver or vers[-1]
        if ver not in vers:
            return JSONResponse(status_code=404, content={"error": f"estimate_ver not found. available={vers}"})

        tmp_path = _download_parquet(ver)

        conditions, params = [], []
        for col, val in [("pref_code", pref_code), ("area_code", area_code), ("area_level", area_level), ("year", year), ("sex", sex), ("age5_code", age5_code)]:
            if val is not None:
                conditions.append(f"{col} = ?")
                params.append(val)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = f"SELECT * FROM read_parquet('{tmp_path}') {where} ORDER BY area_code, year, sex, age5_code"
        if limit:
            sql += f" LIMIT {limit}"

        con = duckdb.connect()
        result = con.execute(sql, params).df()
        con.close()

        if format == "csv":
            return Response(content=result.to_csv(index=False).encode("utf-8-sig"), media_type="text/csv; charset=utf-8")

        data = result.to_dict(orient="records")
        return {"collection": "ipss/shorai_jinko", "estimate_ver": ver, "updated_at": str(datetime.now()), "count": len(data), "data": data}

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
