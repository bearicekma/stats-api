# FRED（米連邦準備銀行 経済データ）APIエンドポイント
# /fred/pass   : 系列データ取得（時系列の数値）
# /fred/meta   : 系列のメタ情報（名称・単位・期間など）
# /fred/search : キーワードで系列を検索
# aaa

from fastapi           import APIRouter, Request
from fastapi.responses import JSONResponse
import httpx
import os

router = APIRouter(prefix="/fred", tags=["fred"])

# FRED API のベースURL
FRED_BASE_URL = "https://api.stlouisfed.org/fred"


@router.get("/pass")
async def fred_pass(request: Request):
    # 指定した系列IDの時系列データを取得する
    # 例: /fred/pass?series_id=DEXJPUS&observation_start=2024-01-01
    # 例: /fred/pass?series_id=FEDFUNDS
    params = dict(request.query_params)

    # series_idは必須
    if "series_id" not in params:
        return JSONResponse(
            status_code=400,
            content={
                "error":   "series_idパラメータは必須です",
                "hint":    "/fred/search で系列IDを検索できます",
                "example": "/fred/pass?series_id=DEXJPUS&observation_start=2024-01-01",
            }
        )

    # APIキーとJSON形式を付加する
    params["api_key"]       = os.environ["FRED_API_KEY"]
    params["file_type"]     = "json"

    async with httpx.AsyncClient(timeout=30) as client:
        res = await client.get(
            f"{FRED_BASE_URL}/series/observations",
            params=params,
        )
    res.raise_for_status()
    data = res.json()

    # observationsを整形して返す
    observations = data.get("observations", [])
    cleaned = [
        {
            "date":  obs["date"],
            # 欠損値（"."）はNoneに変換する
            "value": None if obs["value"] == "." else float(obs["value"])
        }
        for obs in observations
    ]

    return {
        "series_id": params["series_id"],
        "count":     len(cleaned),
        "data":      cleaned,
    }


@router.get("/meta")
async def fred_meta(request: Request):
    # 指定した系列IDのメタ情報を取得する
    # （名称・単位・更新頻度・収録期間・最終更新日など）
    # 例: /fred/meta?series_id=DEXJPUS
    params = dict(request.query_params)

    if "series_id" not in params:
        return JSONResponse(
            status_code=400,
            content={
                "error":   "series_idパラメータは必須です",
                "example": "/fred/meta?series_id=DEXJPUS",
            }
        )

    params["api_key"]   = os.environ["FRED_API_KEY"]
    params["file_type"] = "json"

    async with httpx.AsyncClient(timeout=30) as client:
        res = await client.get(
            f"{FRED_BASE_URL}/series",
            params=params,
        )
    res.raise_for_status()
    data = res.json()

    # seriesリストの先頭1件を整形して返す
    series_list = data.get("seriess", [])
    if not series_list:
        return JSONResponse(status_code=404, content={"error": "系列が見つかりません"})

    s = series_list[0]
    return {
        "series_id":         s.get("id"),
        "title":             s.get("title"),
        "units":             s.get("units"),
        "frequency":         s.get("frequency"),
        "seasonal_adj":      s.get("seasonal_adjustment"),
        "observation_start": s.get("observation_start"),
        "observation_end":   s.get("observation_end"),
        "last_updated":      s.get("last_updated"),
        "notes":             s.get("notes"),
    }


@router.get("/search")
async def fred_search(request: Request):
    # キーワードで系列を検索する
    # 例: /fred/search?search_text=japan+yen&limit=10
    params = dict(request.query_params)

    if "search_text" not in params:
        return JSONRespo
