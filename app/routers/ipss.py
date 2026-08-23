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
    estimate_ver: str = Query(None, description="推計版。未指定なら最新版を返す。例: 2023"),
    pref_code: str = Query(None, description="都道府県コード2桁。例: 20（長野県）"),
    area_code: str = Query(None, description="地域コード5桁。例: 20201（長野市）、20000（長野県計）、00000（全国計）"),
    area_level: str = Query(None, description="country=全国計 / pref=都道府県計 / city=市町村 / special_ward=東京23区 / other=福島県浜通り地域"),
    year: int = Query(None, description="2020, 2025, 2030, 2035, 2040, 2045, 2050 の5年刻み"),
    sex: str = Query(None, description="総数 / 男 / 女（日本語で指定）"),
    age5_code: str = Query(None, description="5歳階級コード。00=0〜4歳、01=5〜9歳 … 18=90〜94歳、19+=95歳以上"),
    limit: int = Query(None, ge=1, description="取得件数の上限。省略時は全件"),
    format: str = Query("json", description="json（既定）または csv"),
):
    """
    国立社会保障・人口問題研究所「日本の地域別将来推計人口」の市区町村別データ。

    **全件数が多いため、無指定で叩くと応答が巨大になります。必ず絞り込んでください。**

    ### 使用例
    - 長野県内の全市町村・2050年の推計
      `?pref_code=20&year=2050`
    - 都道府県別の総人口推移（47件×7年次）
      `?area_level=pref&sex=総数`
    - 全国の年次別・年齢階級別人口
      `?area_code=00000&sex=総数`
    - CSV形式で取得（Power Query向け・BOM付きUTF-8）
      `?pref_code=20&format=csv`

    ### 収録内容
    - 推計版: 令和5(2023)年推計、2020年国勢調査ベース
    - 年次: 2020〜2050年の5年刻み（2020年は実績値）
    - 地域: 全国計1 / 都道府県計47 / 市町村1,700 / 東京23区 / 福島県浜通り地域1
    - 年齢: 5歳階級20区分。政令市の行政区と再掲行は除外済み

    ### 注意
    - `sex` の「総数」は男＋女と一致します
    - `age5_code` の `19+` は95歳以上の開放階級です。マスタ `_M_age` の `19`(95〜99歳) と `20`(100歳以上) の合算に相当し、直接JOINできません
    - 福島県の浜通り地域13市町村は個別の推計がなく、`07999` に一括計上されています
    - 全国計は47都道府県の合算値です
    - 利用可能な推計版は `/ipss/versions` で確認できます
    """
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
