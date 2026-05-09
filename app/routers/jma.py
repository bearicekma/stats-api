# 気象庁 長野県天気予報エンドポイント
# /jma/nagano : 長野県内5地点の週間天気予報（蓄積型）

import os
import tempfile
from datetime import datetime

import duckdb
from fastapi           import APIRouter, Request
from fastapi.responses import JSONResponse
from google.cloud      import storage

router = APIRouter(prefix="/jma", tags=["気象庁 天気予報"])

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "stats-api-491107-data")
GCS_PATH    = "jma/nagano/data.parquet"


def _download_parquet() -> str:
    gcs    = storage.Client()
    bucket = gcs.bucket(BUCKET_NAME)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    bucket.blob(GCS_PATH).download_to_filename(tmp_path)
    return tmp_path


@router.get("/nagano", summary="長野県5地点 週間天気予報")
async def get_jma_nagano(request: Request):
    """
    長野県内5地点の週間天気予報を取得します。
    毎朝6:00 JST に気象庁APIから取得・更新し、過去データも蓄積しています。

    ---

    **取得地点（5地点）:**

    | 地点 | 地域 |
    |------|------|
    | 長野 | 北部（長野市・飯山市周辺） |
    | 松本 | 中部（松本市・安曇野市周辺） |
    | 諏訪 | 中部高地（諏訪市・岡谷市周辺） |
    | 飯田 | 南部（飯田市・駒ヶ根市周辺） |
    | 軽井沢 | 東部（軽井沢町・小諸市周辺） |

    ---

    **フィールドごとのデータソースと地点の定義:**

    - `weather` / `weather_code`（天気テキスト・天気コード）
      - 今日: 北部（長野・軽井沢）/ 中部（松本・諏訪）/ 南部（飯田）のゾーン別
      - 明日以降: 長野県全体として1種類（全5地点に同じ値が入ります）

    - `precip_prob`（降水確率 %）
      - 今日・明日: 北部/中部/南部のゾーン別
      - 明後日以降: 長野県全体として1種類（全5地点に同じ値が入ります）

    - `temp_max` / `temp_min`（最高・最低気温 ℃）
      - 今日・明日: 長野/松本/諏訪/飯田/軽井沢の地点別（3日予報より）
      - 明後日以降: 長野のみ値あり、他4地点はnull（週間予報の対象が長野のみのため）

    - `wind`（風）
      - 今日〜明後日: 北部/中部/南部のゾーン別（3日予報より）
      - 3日目以降: null（週間予報に風の情報なし）

    - `reliability`（気温予報の信頼度 A/B/C）
      - 今日・明日: null（3日予報には信頼度なし）
      - 明後日以降: 長野県全体として1種類

    ---

    **クエリパラメータ:**
    - `location` (任意) 地点名（カンマ区切りで複数可）。省略時は全5地点を返します
    - `from` (任意) 開始日（YYYY-MM-DD）
    - `to` (任意) 終了日（YYYY-MM-DD）

    **URL例:**
    - `/jma/nagano` 全地点・全期間
    - `/jma/nagano?location=長野` 長野地点のみ
    - `/jma/nagano?location=長野,松本&from=2026-05-01`
    - `/jma/nagano?to=2026-05-06` 全地点の5月6日まで

    ※気象庁APIは非公式のため、仕様変更により取得できなくなる場合があります。
    """
    params   = dict(request.query_params)
    location = params.get("location")
    from_    = params.get("from")
    to_      = params.get("to")

    conditions = []
    if location:
        locs = [l.strip() for l in location.split(",")]
        loc_list = ", ".join(f"'{l}'" for l in locs)
        conditions.append(f"location IN ({loc_list})")
    if from_:
        conditions.append(f"target_date >= DATE '{from_}'")
    if to_:
        conditions.append(f"target_date <= DATE '{to_}'")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    tmp_path = None
    try:
        tmp_path = _download_parquet()
        sql = f"""
            SELECT
                CAST(target_date AS VARCHAR) AS target_date,
                location, weather_code, weather,
                temp_max, temp_min, precip_prob, reliability, wind,
                published_at, retrieved_at
            FROM read_parquet('{tmp_path}')
            {where}
            ORDER BY target_date, location
        """
        result = duckdb.query(sql).df()
        data   = result.to_dict(orient="records")
        return {"count": len(data), "updated_at": str(datetime.now()), "data": data}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
